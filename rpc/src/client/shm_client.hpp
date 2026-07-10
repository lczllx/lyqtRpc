#pragma once
// =============================================================================
// shm_client.hpp — 共享内存 Transport Client 端，实现 BaseClient
// -----------------------------------------------------------------------------
// connect() 流程：
//   1. open() 打开已有共享内存
//   2. setup_notify_client() 握手交换 eventfd
//   3. 后台线程 epoll 监听 resp_notify_fd → drain_responses → _cb_message 派发
//
// send(): 序列化 → write_request → notify_req() 通知 Server
// =============================================================================

#include "../general/abstract.hpp"
#include "../general/shm_channel.hpp"
#include "../general/shm_connection.hpp"
#include "../general/message.hpp"
#include "../general/log_system/lcz_log.h"
#include <sys/epoll.h>
#include <unistd.h>
#include <thread>
#include <atomic>

namespace lcz_rpc {

class ShmClient : public BaseClient {
public:
    ShmClient(const std::string& shm_name = "lcz_shm",
              const std::string& notify_path = "lcz_shm_notify")
        : _shm_name(shm_name), _notify_path(notify_path) {}

    void connect() override {
        if (!_channel.open(_shm_name)) {
            LCZ_ERROR("[ShmClient] connect failed"); return;
        }
        if (!_channel.setup_notify_client(_notify_path)) {
            LCZ_ERROR("[ShmClient] notify setup failed");
            _channel.destroy(); return;
        }

        auto conn = std::make_shared<ShmConnection>();
        conn->setName("shm_client");
        conn->setSender([this](const BaseMessage::ptr& msg) { send(msg); });
        _conn = conn;
        if (_cb_connection) _cb_connection(conn);

        _worker = std::thread([this]() { responseLoop(); });
        LCZ_INFO("[ShmClient] connected (notify mode)");
    }

    // 序列化 → 写请求 → 通知 Server
    bool send(const BaseMessage::ptr& msg) override {
        std::string body = msg->serialize();
        bool ok = _channel.write_request(body, msg->msgType());
        if (ok) _channel.notify_req();                    // ★ 唤醒 Server
        return ok;
    }

    void shutdown() override {
        _running = false;
        if (_worker.joinable()) _worker.join();
        _channel.destroy();
    }

    BaseConnection::ptr connection() override { return _conn; }
    bool connected() override { return _channel.is_open(); }
    ~ShmClient() { shutdown(); }

private:
    // 后台 epoll 线程：休眠等 Server 通知，收到后批量读响应
    void responseLoop() {
        int epfd = epoll_create1(0);
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = _channel.resp_notify_fd();
        epoll_ctl(epfd, EPOLL_CTL_ADD, _channel.resp_notify_fd(), &ev);

        _running = true;
        std::string body; lcz_rpc::MsgType type;
        const int resp_fd = _channel.resp_notify_fd();
        LCZ_INFO("[ShmClient] responseLoop started, resp_fd=%d", resp_fd);
        while (_running) {
            struct epoll_event events[1];
            int n = epoll_wait(epfd, events, 1, 500);
            if (n < 0) break;

            if (n > 0) {
                uint64_t val;
                ssize_t rd = ::read(resp_fd, &val, sizeof(val));
                LCZ_DEBUG("[ShmClient] epoll wake: n=%d val=%lu bytes_read=%zd", n, val, rd);
            }

            while (_channel.read_response(body, type)) {
                LCZ_INFO("[ShmClient] recv response type=%d body_len=%zu", static_cast<int>(type), body.size());
                auto msg = MessageFactory::create(type);
                if (!msg) {
                    LCZ_ERROR("[ShmClient] failed to create msg for type=%d", static_cast<int>(type));
                    continue;
                }
                if (!msg->unserialize(body)) {
                    LCZ_ERROR("[ShmClient] failed to unserialize body_len=%zu", body.size());
                    continue;
                }
                msg->setMsgType(type);
                if (_cb_message) _cb_message(_conn, msg);
            }
        }
        close(epfd);
    }

    ShmChannel         _channel;
    ShmConnection::ptr _conn;
    std::string        _shm_name, _notify_path;
    std::thread        _worker;
    std::atomic<bool>  _running{false};
};

} // namespace lcz_rpc
