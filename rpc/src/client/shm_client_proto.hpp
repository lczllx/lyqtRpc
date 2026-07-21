#pragma once
// =============================================================================
// shm_client_proto.hpp — SHM + Protobuf 零拷贝 Client
// =============================================================================
// 写端: req_write_ptr → SerializeToArray 直接进 ring buffer（零拷贝）
// 读端: read_response → ParseFromString（protobuf 紧凑二进制，快于 JSON）
// =============================================================================

#include "../general/abstract.hpp"
#include "../general/shm_channel.hpp"
#include "../general/shm_connection.hpp"
#include "../general/message.hpp"
#include "../general/log_system/lcz_log.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <thread>
#include <atomic>

namespace lcz_rpc
{

    class ShmClientProto : public BaseClient
    {
    public:
        ShmClientProto(const std::string &notify_path = "lcz_shm_proto_notify")
            : _notify_path(notify_path) {}

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
                            { send(msg); });
            _conn = conn;
            if (_cb_connection)
                _cb_connection(conn);
            _worker = std::thread([this]()
                                  { responseLoop(); });
            LCZ_INFO("[ShmClientProto] connected, shm=%s", _shm_name.c_str());
        }

        // 零拷贝发送：req_write_ptr → SerializeToArray 直接写入 ring buffer
        bool send(const BaseMessage::ptr &msg) override
        {
            auto req = std::dynamic_pointer_cast<ProtoRpcRequest>(msg);
            if (!req)
                return false;

            size_t contig = 0;
            char *buf = _channel.req_write_ptr(contig);
            if (buf && contig >= req->byteSize())
            {
                req->serializeToArray(buf, contig);
                _channel.req_commit(req->byteSize(), MsgType::REQ_RPC_PROTO);
            }
            else
            {
                // fallback: string copy
                std::string body = req->serialize();
                if (!_channel.write_request(body, MsgType::REQ_RPC_PROTO))
                    return false;
                _channel.notify_req();
                return true;
            }
            // req_commit 内部已调 notify_req
            return true;
        }

        void shutdown() override
        {
            _running = false;
            if (_worker.joinable())
                _worker.join();
            _channel.destroy();
        }
        BaseConnection::ptr connection() override { return _conn; }
        bool connected() override { return _channel.is_open(); }
        ~ShmClientProto() { shutdown(); }

    private:
        void responseLoop()
        {
            int epfd = epoll_create1(0);
            struct epoll_event ev;
            ev.events = EPOLLIN;
            ev.data.fd = _channel.resp_notify_fd();
            epoll_ctl(epfd, EPOLL_CTL_ADD, _channel.resp_notify_fd(), &ev);
            _running = true;
            std::string body;
            MsgType type;
            const int resp_fd = _channel.resp_notify_fd();
            LCZ_INFO("[ShmClientProto] responseLoop started, resp_fd=%d", resp_fd);
            while (_running)
            {
                struct epoll_event events[1];
                int n = epoll_wait(epfd, events, 1, 500);
                if (n < 0)
                    break;
                if (n > 0)
                {
                    uint64_t val;
                    (void)::read(resp_fd, &val, sizeof(val));
                }
                while (_channel.read_response(body, type))
                {
                    auto msg = MessageFactory::create(type);
                    if (msg && msg->unserialize(body))
                    {
                        msg->setMsgType(type);
                        if (_cb_message)
                            _cb_message(_conn, msg);
                    }
                }
            }
            close(epfd);
        }

        ShmChannel _channel;
        ShmConnection::ptr _conn;
        std::string _notify_path, _shm_name;
        std::thread _worker;
        std::atomic<bool> _running{false};
    };

} // namespace lcz_rpc
