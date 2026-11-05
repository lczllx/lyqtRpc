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
#include <atomic>
#include <string>
#include <string_view>

namespace lcz_rpc
{

    class ShmConnection : public BaseConnection
    {
    public:
        using ptr = std::shared_ptr<ShmConnection>;
        using Sender = std::function<bool(const BaseMessage::ptr &)>;

        // 注入写回调：Server 注入 write_response，Client 注入 send
        void setSender(Sender s) { _sender = std::move(s); }
        void setName(std::string_view n) { _name = std::string(n); }

        // ====== BaseConnection 接口 ======
        // Router 处理完业务调 conn->send(resp)，最终走到 _sender → 写 ring buffer
        bool send(const BaseMessage::ptr &msg) override
        {
            return _sender ? _sender(msg) : false;
        }
        void shutdown() override {}                // SHM 没有连接可关
        // 传输层检测到对端消失/通道不可用时调用；此后 Router/发现路径不再使用该连接
        void markDead() { _alive.store(false, std::memory_order_release); }
        bool connected() override { return _alive.load(std::memory_order_acquire); }
        std::string peerAddress() const override { return "shm://" + _name; }

    private:
        Sender _sender;    // Server 端 = write_response, Client 端 = send
        std::string _name; // 日志标识，如 "shm_server" / "shm_client"
        std::atomic<bool> _alive{true}; // 对端存活状态，由传输层后台线程/发送失败置 false
    };

} // namespace lcz_rpc
