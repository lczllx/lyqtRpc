#pragma once
// =============================================================================
// shm_server_proto.hpp — SHM + Protobuf 零拷贝 Server（muduo EventLoopThreadPool）
// =============================================================================
// 写端: req_write_ptr → SerializeToArray 直接进 ring buffer（零拷贝）
// 读端: read_request → ParseFromString
// 线程池: muduo::net::EventLoopThreadPool + Channel 包装 req_fd
// =============================================================================

#include "../general/abstract.hpp"
#include "../general/shm_channel.hpp"
#include "../general/shm_connection.hpp"
#include "../general/message.hpp"
#include "../general/log_system/lcz_log.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/EventLoopThread.h"
#include "muduo/net/Channel.h"
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <atomic>
#include <memory>
#include <unordered_map>
#include <mutex>

namespace lcz_rpc
{

    class ShmServerProto : public BaseServer
    {
    public:
        ShmServerProto(const std::string &notify_path = "lcz_shm_proto_notify",
                       const std::string &shm_prefix = "lcz_shm_proto",
                       size_t req_size = 64 * 1024 * 1024,
                       size_t resp_size = 64 * 1024 * 1024,
                       int max_clients = 64,
                       int worker_threads = 4)
            : _notify_path(notify_path), _shm_prefix(shm_prefix),
              _req_size(req_size), _resp_size(resp_size),
              _max_clients(max_clients), _worker_count(worker_threads) {}

        void start() override
        {
            int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
            if (listen_fd < 0)
            {
                LCZ_ERROR("[ShmServerProto] socket failed");
                return;
            }
            struct sockaddr_un addr = {};
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, _notify_path.c_str(), sizeof(addr.sun_path) - 1);
            unlink(_notify_path.c_str());
            if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 || listen(listen_fd, _max_clients) < 0)
            {
                LCZ_ERROR("[ShmServerProto] bind/listen failed");
                close(listen_fd);
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
                int conn_fd = accept(listen_fd, nullptr, nullptr);
                if (conn_fd < 0)
                    break;
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
                conn->setSender([entry](const BaseMessage::ptr &msg)
                                {
                auto resp = std::dynamic_pointer_cast<ProtoRpcResponse>(msg);
                if (!resp) return;
                size_t contig = 0;
                char* buf = entry->channel.resp_write_ptr(contig);
                if (buf && contig >= resp->byteSize()) {
                    resp->serializeToArray(buf, contig);
                    entry->channel.resp_commit(resp->byteSize(), MsgType::RSP_RPC_PROTO);
                } else {
                    std::string body = resp->serialize();
                    entry->channel.write_response(body, MsgType::RSP_RPC_PROTO);
                }
                entry->channel.notify_resp(); });
                entry->conn = conn;
                if (_cb_connection)
                    _cb_connection(conn);

                // muduo Channel 包装 req_fd，注册到 worker EventLoop
                auto *workerLoop = _workers[round_robin++ % _worker_count].loop;
                auto ch = std::make_unique<muduo::net::Channel>(workerLoop, req_fd);
                ch->setReadCallback([this, entry, req_fd](muduo::Timestamp)
                                    {
                uint64_t val; (void)::read(req_fd, &val, sizeof(val));
                std::string body; MsgType type;
                while (entry->channel.read_request(body, type)) {
                    auto msg = MessageFactory::create(type);
                    if (msg && msg->unserialize(body)) {
                        msg->setMsgType(type);
                        if (_cb_message) _cb_message(entry->conn, msg);
                    }
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

            close(listen_fd);
            unlink(_notify_path.c_str());
        }

        void stop() override { _running = false; }
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
