#pragma once
// =============================================================================
// shm_client_proto.hpp — SHM + Protobuf 零拷贝 Client
// =============================================================================
// 写端: req_write_ptr → SerializeToArray 直接进 ring buffer（零拷贝）
// 读端: read_response_view → ParseFromArray（protobuf 紧凑二进制，直接从 ring 解析）
// =============================================================================

#include "general/abstract.hpp"
#include "general/shm_channel.hpp"
#include "general/shm_connection.hpp"
#include "general/message.hpp"
#include "general/log_system/lcz_log.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstdlib>
#include <thread>
#include <stop_token>
#include <span>

namespace lcz_rpc
{

    class ShmClientProto : public BaseClient
    {
    public:
        ShmClientProto(const std::string &notify_path = "lcz_shm_proto_notify")
            : _notify_path(notify_path)
        {
            if (const char *env = std::getenv("LCZ_SHM_WRITE_WAIT_MS"))
                _write_wait_ms = std::atoi(env);
        }

        void connect() override
        {
            int conn_fd = socket(AF_UNIX, SOCK_STREAM, 0);
            if (conn_fd < 0)
            {
                LCZ_ERROR("[ShmClientProto] socket failed");
                return;
            }
            struct sockaddr_un addr = {};
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, _notify_path.c_str(), sizeof(addr.sun_path) - 1);
            if (::connect(conn_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
            {
                LCZ_ERROR("[ShmClientProto] connect %s failed", _notify_path.c_str());
                close(conn_fd);
                return;
            }
            int req_fd = -1, resp_fd = -1;
            if (!ShmChannel::handshake_client(conn_fd, req_fd, resp_fd, _shm_name))
            {
                LCZ_ERROR("[ShmClientProto] handshake failed");
                close(conn_fd);
                return;
            }
            close(conn_fd);
            if (!_channel.open(_shm_name))
            {
                LCZ_ERROR("[ShmClientProto] open failed");
                return;
            }
            _channel.set_req_notify_fd(req_fd);
            _channel.set_resp_notify_fd(resp_fd);

            auto conn = std::make_shared<ShmConnection>();
            conn->setName("shm_client_proto_" + _shm_name);
            conn->setSender([this](const BaseMessage::ptr &msg)
                            { return send(msg); });
            _conn = conn;
            if (_cb_connection)
                _cb_connection(conn);
            _worker = std::jthread([this](std::stop_token st)
                                   { responseLoop(st); });
            LCZ_INFO("[ShmClientProto] connected, shm=%s", _shm_name.c_str());
        }

        // 零拷贝发送：req_write_ptr → SerializeToArray 直接写入 ring buffer
        bool send(const BaseMessage::ptr &msg) override
        {
            auto req = std::dynamic_pointer_cast<ProtoRpcRequest>(msg);
            if (!req)
            {
                if (_conn)
                    _conn->setLastSendError(RpcError::SERVICE_ERROR);
                return false;
            }
            if (!_channel.is_open())
            {
                if (_conn)
                {
                    _conn->setLastSendError(RpcError::CONN_CLOSED);
                    _conn->markDead();
                }
                return false;
            }
            // 单帧超过 ring 容量：重试不会变好，直接按配置/载荷错误上报
            if (req->byteSize() + 8 > _channel.req_capacity())
            {
                LCZ_ERROR("[ShmClientProto] message too large: %zu bytes > ring capacity %zu",
                          req->byteSize() + 8, _channel.req_capacity());
                if (_conn)
                    _conn->setLastSendError(RpcError::SERVICE_ERROR);
                return false;
            }

            std::span<char> buf = _channel.req_write_ptr();
            if (!buf.empty() && buf.size() >= req->byteSize())
            {
                req->serializeToArray(buf);
                _channel.req_commit(req->byteSize(), MsgType::REQ_RPC_PROTO);
            }
            else
            {
                // fallback: string copy
                std::string body = req->serialize();
                if (!_channel.write_request(body, MsgType::REQ_RPC_PROTO, _write_wait_ms))
                {
                    if (_conn)
                    {
                        if (!_channel.is_open())
                        {
                            _conn->setLastSendError(RpcError::CONN_CLOSED);
                            _conn->markDead();
                        }
                        else
                        {
                            // ring 满/等待超时：本地过载，可退避，不计熔断
                            _conn->setLastSendError(RpcError::BACKOFF);
                        }
                    }
                    return false;
                }
                _channel.notify_req();
                return true;
            }
            // req_commit 内部已调 notify_req
            return true;
        }

        void shutdown() override
        {
            _worker.request_stop();
            if (_worker.joinable())
                _worker.join();
            if (_conn) _conn->markDead();
            _channel.destroy();
        }
        BaseConnection::ptr connection() override { return _conn; }
        bool connected() override { return _channel.is_open() && _conn && _conn->connected(); }
        ~ShmClientProto() { shutdown(); }

    private:
        void responseLoop(std::stop_token st)
        {
            int epfd = epoll_create1(0);
            struct epoll_event ev;
            ev.events = EPOLLIN;
            ev.data.fd = _channel.resp_notify_fd();
            epoll_ctl(epfd, EPOLL_CTL_ADD, _channel.resp_notify_fd(), &ev);
            const int resp_fd = _channel.resp_notify_fd();
            LCZ_INFO("[ShmClientProto] responseLoop started, resp_fd=%d", resp_fd);
            while (!st.stop_requested())
            {
                struct epoll_event events[1];
                int n = epoll_wait(epfd, events, 1, 500);
                if (n < 0)
                    break;

                // 每 500ms 超时醒一次：Server 崩溃或已 shutdown 时把连接标死，
                // 否则 connected() 永远为 true，发现/心跳路径会一直往死通道里写。
                if (n == 0 && !_channel.peerAlive())
                {
                    LCZ_ERROR("[ShmClientProto] server is gone (shm=%s), marking connection dead", _shm_name.c_str());
                    if (_conn) _conn->markDead();
                    break;
                }
                if (n > 0)
                {
                    uint64_t val;
                    (void)::read(resp_fd, &val, sizeof(val));
                }
                ShmChannel::FrameView view;
                while (_channel.read_response_view(view))
                {
                    auto msg = MessageFactory::create(view.type);
                    // ParseFromArray 已把字段拷进消息对象，解析完即可解 pin
                    bool ok = msg && msg->unserialize(view.body_view());
                    if (ok) msg->setMsgType(view.type);
                    _channel.release_response_view();
                    if (ok && _cb_message)
                        _cb_message(_conn, msg);
                }
            }
            if (!st.stop_requested() && _conn)
                _conn->markDead(); // epoll 出错等非主动退出
            close(epfd);
        }

        ShmChannel _channel;
        ShmConnection::ptr _conn;
        std::string _notify_path, _shm_name;
        std::jthread _worker;
        int _write_wait_ms = SHM_REQUEST_WAIT_MS; // ring 满时的有界等待预算（LCZ_SHM_WRITE_WAIT_MS 可覆盖）
    };

} // namespace lcz_rpc
