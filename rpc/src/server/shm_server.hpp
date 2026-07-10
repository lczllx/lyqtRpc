#pragma once
// =============================================================================
// shm_server.hpp — 共享内存 Transport Server 端，实现 BaseServer
// -----------------------------------------------------------------------------
// start() 流程：
//   1. ShmChannel::create() 创建共享内存 + 双 ring buffer
//   2. setup_notify_server() 握手交换 eventfd（SCM_RIGHTS 跨进程传 fd）
//   3. epoll 监听 req_notify_fd，收到信号 → drain_requests → _cb_message 派发
//   4. _cb_message 回调中 Router 处理业务 → conn->send(resp)
//      → ShmConnection::send() → write_response + write(resp_notify_fd) 通知 Client
// =============================================================================

#include "../general/abstract.hpp"
#include "../general/shm_channel.hpp"
#include "../general/shm_connection.hpp"
#include "../general/message.hpp"
#include "../general/log_system/lcz_log.h"
#include <sys/epoll.h>
#include <unistd.h>
#include <atomic>

namespace lcz_rpc {

class ShmServer : public BaseServer {
public:
    ShmServer(const std::string& shm_name = "lcz_shm",
              const std::string& notify_path = "lcz_shm_notify",
              size_t req_size  = 64 * 1024 * 1024,
              size_t resp_size = 64 * 1024 * 1024)
        : _shm_name(shm_name), _notify_path(notify_path),
          _req_size(req_size), _resp_size(resp_size) {}

    void start() override {
        // 1. 创建共享内存 + 控制区 + 双 ring buffer
        if (!_channel.create(_shm_name, _req_size, _resp_size)) {
            LCZ_ERROR("[ShmServer] create failed"); return;
        }

        // 2. Unix 域 socket 握手，交换 eventfd
        if (!_channel.setup_notify_server(_notify_path)) {
            LCZ_ERROR("[ShmServer] notify setup failed");
            _channel.destroy(); return;
        }

        // 3. 构造虚拟连接：Router 调 conn->send(resp) → 写响应 + 通知 Client
        auto conn = std::make_shared<ShmConnection>();
        conn->setName("shm_server");
        conn->setSender([this](const BaseMessage::ptr& msg) {
            std::string body = msg->serialize();
            LCZ_DEBUG("[ShmServer] conn->send: type=%d body_len=%zu", static_cast<int>(msg->msgType()), body.size());
            bool ok = _channel.write_response(body, msg->msgType());
            LCZ_DEBUG("[ShmServer] write_response ok=%d, notifying client via resp_fd=%d", ok, _channel.resp_notify_fd());
            _channel.notify_resp();
        });
        if (_cb_connection) _cb_connection(conn);

        // 4. epoll 监听 req_notify_fd（Client 写完请求后 write 这个 fd）
        int epfd = epoll_create1(0);
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = _channel.req_notify_fd();
        epoll_ctl(epfd, EPOLL_CTL_ADD, _channel.req_notify_fd(), &ev);

        LCZ_INFO("[ShmServer] started (notify mode), fd=%d", _channel.req_notify_fd());
        _running = true;

        // 5. 事件循环：休眠等 Client 通知，收到后批量读请求
        std::string body; lcz_rpc::MsgType type;
        const int req_fd = _channel.req_notify_fd();
        LCZ_INFO("[ShmServer] event loop started, req_fd=%d", req_fd);
        while (_running) {
            struct epoll_event events[1];
            int n = epoll_wait(epfd, events, 1, 500);
            if (n < 0) break;

            if (n > 0) {
                uint64_t val;
                ssize_t rd = ::read(req_fd, &val, sizeof(val));
                LCZ_DEBUG("[ShmServer] epoll wake: n=%d val=%lu bytes_read=%zd", n, val, rd);
            }

            while (_channel.read_request(body, type)) {
                LCZ_INFO("[ShmServer] recv request type=%d body_len=%zu", static_cast<int>(type), body.size());
                auto msg = MessageFactory::create(type);
                if (!msg) {
                    LCZ_ERROR("[ShmServer] failed to create msg for type=%d", static_cast<int>(type));
                    continue;
                }
                if (!msg->unserialize(body)) {
                    LCZ_ERROR("[ShmServer] failed to unserialize body_len=%zu", body.size());
                    continue;
                }
                msg->setMsgType(type);
                if (_cb_message) _cb_message(conn, msg);
                LCZ_DEBUG("[ShmServer] request processed");
            }
        }
        close(epfd);
    }

    void stop() override { _running = false; }
    ~ShmServer() { _running = false; _channel.destroy(); }

private:
    ShmChannel         _channel;
    ShmConnection::ptr _conn;
    std::string        _shm_name, _notify_path;
    size_t             _req_size, _resp_size;
    std::atomic<bool>  _running{false};
};

} // namespace lcz_rpc
