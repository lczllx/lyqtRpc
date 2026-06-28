#pragma once
// =============================================================================
// shm_server.hpp — 共享内存 Transport Server 端，实现 BaseServer
// -----------------------------------------------------------------------------
// start() 流程：
//   1. ShmChannel::create() 创建共享内存 + 控制区 + 双 ring buffer
//   2. 构造 ShmConnection，注入 _sender = write_response（Router 发响应用）
//   3. 调 _cb_connection 通知框架"连接已建立"
//   4. 轮询 read_request → MessageFactory::create → unserialize → _cb_message 派发
//
// 当前为轮询模式（Phase 4），CPU 100%。Phase 3 加 eventfd 后休眠。
// =============================================================================

#include "abstract.hpp"
#include "shm_channel.hpp"
#include "shm_connection.hpp"
#include "message.hpp"
#include "log_system/lcz_log.h"
#include <atomic>

namespace lcz_rpc {

class ShmServer : public BaseServer {
public:
    ShmServer(const std::string& shm_name = "lcz_shm",
              size_t req_size  = 64 * 1024 * 1024,
              size_t resp_size = 64 * 1024 * 1024)
        : _shm_name(shm_name), _req_size(req_size), _resp_size(resp_size) {}

    // ====== BaseServer 接口 ======
    void start() override {
        // 1. 创建共享内存 + 控制区 + 双 ring buffer
        if (!_channel.create(_shm_name, _req_size, _resp_size)) {
            LCZ_ERROR("[ShmServer] create failed");
            return;
        }

        // 2. 构造虚拟连接，注入 _sender：Router 调 conn->send(resp) → 写响应 ring buffer
        auto conn = std::make_shared<ShmConnection>();
        conn->setName("shm_server");
        conn->setSender([this](const BaseMessage::ptr& msg) {
            std::string body = msg->serialize();
            _channel.write_response(body, msg->msgType());
        });

        // 3. 通知框架连接就绪（Dispacher 用此 conn 做回调上下文）
        if (_cb_connection) _cb_connection(conn);

        LCZ_INFO("[ShmServer] started, polling requests...");
        _running = true;

        // 4. 轮询读请求（Phase 3 换成 eventfd + epoll）
        std::string body; MsgType type;
        while (_running) {
            if (_channel.read_request(body, type)) {
                // 读到一帧 → 反序列化 → 回调框架
                auto msg = MessageFactory::create(type);
                if (msg && msg->unserialize(body)) {
                    msg->setMsgType(type);
                    if (_cb_message) _cb_message(conn, msg); // → Dispacher → Router
                }
            }
        }
    }

    void stop() override { _running = false; }
    ~ShmServer() { _running = false; _channel.destroy(); }

private:
    ShmChannel         _channel;     // mmap + ring buffer 通道
    ShmConnection::ptr _conn;        // 虚拟连接句柄
    std::string        _shm_name;    // /dev/shm/ 下的名字
    size_t             _req_size, _resp_size;
    std::atomic<bool>  _running{false};
};

} // namespace lcz_rpc
