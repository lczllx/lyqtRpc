#pragma once
// =============================================================================
// shm_client.hpp — 共享内存 Transport Client 端（支持多客户端）
// =============================================================================
// connect() 流程：
//   1. socket + connect Unix socket
//   2. handshake_client 交换 eventfd + 接收 SHM 名字
//   3. channel.open(shm_name)
//   4. 后台线程 epoll 监听 resp_notify_fd → drain_responses → _cb_message 派发
//
// send(): 序列化 → write_request → notify_req() 通知 Server
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

    class ShmClient : public BaseClient
    {
    public:
        // notify_path: Server 的 Unix socket 路径（只需知道去哪 connect）
        ShmClient(const std::string &notify_path = "lcz_shm_notify")
            : _notify_path(notify_path) {}

        void connect() override
        {
            // 1. connect Unix socket
            int conn_fd = socket(AF_UNIX, SOCK_STREAM, 0);
            if (conn_fd < 0)
            {
                LCZ_ERROR("[ShmClient] socket failed errno=%d", errno);
                return;
            }

            struct sockaddr_un addr = {};
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, _notify_path.c_str(), sizeof(addr.sun_path) - 1);
            if (::connect(conn_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
            {
                LCZ_ERROR("[ShmClient] connect %s failed errno=%d (server 是否已启动?)",
                          _notify_path.c_str(), errno);
                close(conn_fd);
                return;
            }

            // 2. 握手：交换 eventfd + 接收 SHM 名字
            int req_fd = -1, resp_fd = -1;
            if (!ShmChannel::handshake_client(conn_fd, req_fd, resp_fd, _shm_name))
            {
                LCZ_ERROR("[ShmClient] handshake failed");
                close(conn_fd);
                return;
            }
            close(conn_fd);

            // 3. 打开 Server 分配的 SHM
            if (!_channel.open(_shm_name))
            {
                LCZ_ERROR("[ShmClient] open %s failed", _shm_name.c_str());
                return;
            }
            _channel.set_req_notify_fd(req_fd);
            _channel.set_resp_notify_fd(resp_fd);

            auto conn = std::make_shared<ShmConnection>();
            conn->setName("shm_client_" + _shm_name);
            conn->setSender([this](const BaseMessage::ptr &msg)
                            { send(msg); });
            _conn = conn;
            if (_cb_connection)
                _cb_connection(conn);

            _worker = std::thread([this]()
                                  { responseLoop(); });
            LCZ_INFO("[ShmClient] connected, shm=%s req_fd=%d resp_fd=%d",
                     _shm_name.c_str(), req_fd, resp_fd);
        }

        bool send(const BaseMessage::ptr &msg) override
        {
            std::string body = msg->serialize();
            bool ok = _channel.write_request(body, msg->msgType());
            if (ok)
                _channel.notify_req();
            return ok;
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
        ~ShmClient() { shutdown(); }

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
            lcz_rpc::MsgType type;
            const int resp_fd = _channel.resp_notify_fd();
            LCZ_INFO("[ShmClient] responseLoop started, resp_fd=%d", resp_fd);
            while (_running)
            {
                struct epoll_event events[1];
                int n = epoll_wait(epfd, events, 1, 500);
                if (n < 0)
                    break;

                if (n > 0)
                {
                    uint64_t val;
                    ssize_t __attribute__((unused)) _rd = ::read(resp_fd, &val, sizeof(val));
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
        std::string _notify_path;
        std::string _shm_name; // 握手后由 Server 分配
        std::thread _worker;
        std::atomic<bool> _running{false};
    };

} // namespace lcz_rpc
