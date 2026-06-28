#pragma once
// =============================================================================
// shm_client.hpp — 共享内存 Transport Client 端，实现 BaseClient
// -----------------------------------------------------------------------------
// connect() 流程：
//   1. ShmChannel::open() 打开 Server 创建的共享内存
//   2. 构造 ShmConnection，注入 _sender = send（写请求 ring buffer）
//   3. 调 _cb_connection 通知框架
//
// pollResponse()：轮询读响应 → 反序列化 → _cb_message 派发（Phase 3 换 eventfd）
// =============================================================================

#include "abstract.hpp"
#include "shm_channel.hpp"
#include "shm_connection.hpp"
#include "message.hpp"
#include "log_system/lcz_log.h"

namespace lcz_rpc {

class ShmClient : public BaseClient {
public:
    ShmClient(const std::string& shm_name = "lcz_shm")
        : _shm_name(shm_name) {}

    // ====== BaseClient 接口 ======

    void connect() override {
        // 1. 打开已有的共享内存通道
        if (!_channel.open(_shm_name)) {
            LCZ_ERROR("[ShmClient] connect failed");
            return;
        }

        // 2. 构造虚拟连接，注入 _sender = 自身 send()
        //    Router 调 conn->send(req) ≡ this->send(req) ≡ write_request
        auto conn = std::make_shared<ShmConnection>();
        conn->setName("shm_client");
        conn->setSender([this](const BaseMessage::ptr& msg) {
            send(msg);                                          // 委托到自己
        });

        if (_cb_connection) _cb_connection(conn);
        LCZ_INFO("[ShmClient] connected");
    }

    // 将消息序列化后写入请求 ring buffer
    bool send(const BaseMessage::ptr& msg) override {
        std::string body = msg->serialize();
        return _channel.write_request(body, msg->msgType());
    }

    void shutdown() override { _channel.destroy(); }

    // 返回虚拟连接句柄（框架需要 conn->send 发请求）
    BaseConnection::ptr connection() override { return _conn; }
    bool connected() override { return _channel.is_open(); }

    // 轮询读响应 → 反序列化 → 派发到 framework callback（Phase 3 换 eventfd）
    bool pollResponse() {
        std::string body; MsgType type;
        if (!_channel.read_response(body, type)) return false;

        auto msg = MessageFactory::create(type);
        if (!msg || !msg->unserialize(body)) return false;
        msg->setMsgType(type);

        if (_cb_message) _cb_message(_conn, msg);  // → Dispacher → Requestor
        return true;
    }

    ~ShmClient() { _channel.destroy(); }

private:
    ShmChannel         _channel;     // mmap + ring buffer 通道
    ShmConnection::ptr _conn;        // 虚拟连接句柄
    std::string        _shm_name;    // /dev/shm/ 下的名字
};

} // namespace lcz_rpc
