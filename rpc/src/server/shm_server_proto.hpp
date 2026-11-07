#pragma once
// =============================================================================
// shm_server_proto.hpp — SHM + Protobuf 零拷贝 Server（muduo EventLoopThreadPool）
// =============================================================================
// 写端: req_write_ptr → SerializeToArray 直接进 ring buffer（零拷贝）
// 读端: read_request_view → ParseFromArray 直接从 ring 解析（不拷贝 body）
// 线程池: muduo::net::EventLoopThreadPool + Channel 包装 req_fd
// =============================================================================

#include "general/abstract.hpp"
#include "general/shm_channel.hpp"
#include "general/shm_connection.hpp"
#include "general/message.hpp"
#include "general/log_system/lcz_log.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/EventLoopThread.h"
#include "muduo/net/Channel.h"
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/time.h>
#include <unistd.h>
#include <atomic>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <span>

namespace lcz_rpc
{

    class ShmServerProto : public BaseServer
    {
    public:
        ShmServerProto(const std::string &notify_path = "lcz_shm_proto_notify",
                       const std::string &shm_prefix = "lcz_shm_proto",
                       size_t req_size = SHM_DEFAULT_BUF_SIZE,
                       size_t resp_size = SHM_DEFAULT_BUF_SIZE,
                       int max_clients = 64,
                       int worker_threads = 4)
            : _notify_path(notify_path), _shm_prefix(shm_prefix),
              _req_size(req_size), _resp_size(resp_size),
              _max_clients(max_clients), _worker_count(worker_threads) {}

        void start() override
        {
            _listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
            if (_listen_fd < 0)
            {
                LCZ_ERROR("[ShmServerProto] socket failed");
                return;
            }
            // accept 超时 500ms：bind 之后再设，避免干扰 socket 创建
            struct timeval tv = {0, 500000};
            setsockopt(_listen_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            struct sockaddr_un addr = {};
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, _notify_path.c_str(), sizeof(addr.sun_path) - 1);
            unlink(_notify_path.c_str());
            if (bind(_listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 || listen(_listen_fd, _max_clients) < 0)
            {
                LCZ_ERROR("[ShmServerProto] bind/listen failed");
                close(_listen_fd);
                return;
            }

            // muduo EventLoop 线程池（每个 worker 一个 EventLoopThread）
            for (int i = 0; i < _worker_count; ++i)
            {
                auto t = std::make_unique<muduo::net::EventLoopThread>();
                muduo::net::EventLoop *loop = t->startLoop();
                _workers.push_back({std::move(t), loop});
            }

            _running = true;
            LCZ_INFO("[ShmServerProto] listening on %s, workers=%d", _notify_path.c_str(), _worker_count);

            // accept + 分发给 worker EventLoop
            int next_id = 0, round_robin = 0;
            while (_running)
            {
                int conn_fd = accept(_listen_fd, nullptr, nullptr);
                if (conn_fd < 0)
                {
                    if (!_running)
                        break;
                    continue;
                }
                if (next_id >= _max_clients)
                {
                    close(conn_fd);
                    continue;
                }

                std::string shm_name = _shm_prefix + "_" + std::to_string(next_id++);
                auto entry = std::make_shared<ClientEntry>();
                if (!entry->channel.create(shm_name, _req_size, _resp_size))
                {
                    close(conn_fd);
                    continue;
                }
                int req_fd = -1, resp_fd = -1;
                if (!ShmChannel::handshake_server(conn_fd, req_fd, resp_fd, shm_name))
                {
                    entry->channel.destroy();
                    close(conn_fd);
                    continue;
                }
                close(conn_fd);
                entry->channel.set_req_notify_fd(req_fd);
                entry->channel.set_resp_notify_fd(resp_fd);

                auto conn = std::make_shared<ShmConnection>();
                conn->setName("shm_server_proto_" + std::to_string(next_id - 1));
                conn->setSender([weak_entry = std::weak_ptr<ClientEntry>(entry)](const BaseMessage::ptr &msg)
                                {
                auto entry = weak_entry.lock();
                if (!entry) return false; // 连接已销毁，通道句柄失效，丢弃响应
                auto resp = std::dynamic_pointer_cast<ProtoRpcResponse>(msg);
                if (!resp) return false;
                std::span<char> buf = entry->channel.resp_write_ptr();
                if (!buf.empty() && buf.size() >= resp->byteSize()) {
                    resp->serializeToArray(buf);
                    entry->channel.resp_commit(resp->byteSize(), MsgType::RSP_RPC_PROTO);
                } else {
                    std::string body = resp->serialize();
                    if (!entry->channel.write_response(body, MsgType::RSP_RPC_PROTO))
                    {
                        LCZ_ERROR("[ShmServerProto] write_response failed, response dropped, id=%s", msg->rid().c_str());
                        return false;
                    }
                }
                entry->channel.notify_resp();
                return true; });
                entry->conn = conn;
                if (_cb_connection)
                    _cb_connection(conn);

                // muduo Channel 包装 req_fd，注册到 worker EventLoop
                auto *workerLoop = _workers[round_robin++ % _worker_count].loop;
                auto ch = std::make_unique<muduo::net::Channel>(workerLoop, req_fd);
                ch->setReadCallback([this, entry, req_fd](muduo::Timestamp)
                                    {
                uint64_t val; (void)::read(req_fd, &val, sizeof(val));
                ShmChannel::FrameView view;
                while (entry->channel.read_request_view(view)) {
                    auto msg = MessageFactory::create(view.type);
                    // protobuf 的 ParseFromArray 已把字段拷进消息对象，解析完即可解 pin
                    bool ok = msg && msg->unserialize(view.body_view());
                    if (ok) msg->setMsgType(view.type);
                    entry->channel.release_request_view();
                    if (ok && _cb_message) _cb_message(entry->conn, msg);
                } });
                auto *ch_raw = ch.get();
                workerLoop->runInLoop([ch_raw]()
                                      { ch_raw->enableReading(); });

                {
                    std::lock_guard<std::mutex> lk(_mtx);
                    _clients.push_back(entry);
                    _channels.push_back(std::move(ch));
                }
                LCZ_INFO("[ShmServerProto] client %d -> worker %d, shm=%s",
                         next_id - 1, (round_robin - 1) % _worker_count, shm_name.c_str());
            }

            close(_listen_fd);
            _listen_fd = -1;
            unlink(_notify_path.c_str());
        }

        void stop() override
        {
            _running = false;
            if (_listen_fd >= 0)
            {
                shutdown(_listen_fd, SHUT_RDWR); // 强制唤醒阻塞在 accept() 的线程
                close(_listen_fd);
                _listen_fd = -1;
            }
        }
        ~ShmServerProto()
        {
            _running = false;
            std::lock_guard<std::mutex> lk(_mtx);
            _channels.clear();
            for (auto &e : _clients)
                e->channel.destroy();
            for (auto &w : _workers)
                w.loop->quit();
        }

    private:
        struct ClientEntry
        {
            ShmChannel channel;
            ShmConnection::ptr conn;
        };

        std::string _notify_path, _shm_prefix;
        size_t _req_size, _resp_size;
        int _max_clients, _worker_count;
        int _listen_fd = -1;
        std::atomic<bool> _running{false};

        struct Worker
        {
            std::unique_ptr<muduo::net::EventLoopThread> thread;
            muduo::net::EventLoop *loop;
        };
        std::vector<Worker> _workers;
        std::mutex _mtx;
        std::vector<std::shared_ptr<ClientEntry>> _clients;
        std::vector<std::unique_ptr<muduo::net::Channel>> _channels;
    };

} // namespace lcz_rpc
