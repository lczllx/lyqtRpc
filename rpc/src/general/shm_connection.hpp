#pragma once
// =============================================================================
// shm_connection.hpp — 共享内存 Transport 的虚拟连接
// -----------------------------------------------------------------------------
// ShmConnection 实现 BaseConnection 接口，不管理 TCP socket。
// 它是一个注入回调的句柄——上层 Router 调 conn->send(resp) 时，
// 实际上调的是 Server/Client 注入的 _sender 函数，直接写 ring buffer。
//
// 设计原因：现有框架的 _cb_message 签名要求传 BaseConnection::ptr，
//           Dispacher 和 Router 不感知底层是 TCP 还是 SHM。
//           所以需要这个"假连接"来适配框架接口。
// =============================================================================

#include "abstract.hpp"
#include <functional>
#include <string>

namespace lcz_rpc
{

    class ShmConnection : public BaseConnection
    {
    public:
        using ptr = std::shared_ptr<ShmConnection>;
        using Sender = std::function<void(const BaseMessage::ptr &)>;

        // 注入写回调：Server 注入 write_response，Client 注入 send
        void setSender(Sender s) { _sender = std::move(s); }
        void setName(const std::string &n) { _name = n; }

        // ====== BaseConnection 接口 ======
        // Router 处理完业务调 conn->send(resp)，最终走到 _sender → 写 ring buffer
        void send(const BaseMessage::ptr &msg) override
        {
            if (_sender)
                _sender(msg);
        }
        void shutdown() override {}                // SHM 没有连接可关
        bool connected() override { return true; } // SHM 通道打开即视为已连接
        std::string peerAddress() const override { return "shm://" + _name; }

    private:
        Sender _sender;    // Server 端 = write_response, Client 端 = send
        std::string _name; // 日志标识，如 "shm_server" / "shm_client"
    };

} // namespace lcz_rpc
