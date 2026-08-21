// Dispacher 单元测试：按 MsgType 派发到类型化 handler，未知类型关闭连接
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>
#include "src/general/dispacher.hpp"
#include "src/general/abstract.hpp"

using lcz_rpc::Dispacher;
using lcz_rpc::BaseConnection;
using lcz_rpc::BaseMessage;
using lcz_rpc::RpcRequest;
using lcz_rpc::RpcResponse;
using lcz_rpc::MsgType;

namespace
{
    // 记录 shutdown 次数，供断言未知类型的连接关闭行为
    class RecordingConnection : public BaseConnection
    {
    public:
        int shutdown_count = 0;
        void send(const BaseMessage::ptr &) override {}
        void shutdown() override { ++shutdown_count; }
        bool connected() override { return true; }
        std::string peerAddress() const override { return "rec:1"; }
    };
}

// 已注册的类型正确派发，handler 收到具体类型的 msg
TEST(DispacherTest, DispatchToRegisteredHandler)
{
    Dispacher d;
    auto conn = std::make_shared<RecordingConnection>();
    bool called = false;
    std::string got_method;

    d.registerhandler<RpcRequest>(MsgType::REQ_RPC,
        [&](const BaseConnection::ptr &, std::shared_ptr<RpcRequest> &msg) {
            called = true;
            got_method = msg->method();
        });

    auto msg = std::make_shared<RpcRequest>();
    msg->setMethod("echo");
    msg->setMsgType(MsgType::REQ_RPC);
    BaseMessage::ptr base = msg;
    d.onMessage(conn, base);

    EXPECT_TRUE(called);
    EXPECT_EQ(got_method, "echo");
    EXPECT_EQ(conn->shutdown_count, 0);
}

// 未注册的消息类型：关闭连接
TEST(DispacherTest, UnknownTypeShutsDown)
{
    Dispacher d;
    auto conn = std::make_shared<RecordingConnection>();

    auto msg = std::make_shared<RpcResponse>();
    msg->setMsgType(MsgType::RSP_RPC); // 未注册 RSP_RPC
    BaseMessage::ptr base = msg;
    d.onMessage(conn, base);

    EXPECT_EQ(conn->shutdown_count, 1);
}

// 空消息直接返回，不派发也不关连接
TEST(DispacherTest, NullMessageNoDispatch)
{
    Dispacher d;
    auto conn = std::make_shared<RecordingConnection>();
    BaseMessage::ptr null_msg;
    d.onMessage(conn, null_msg);

    EXPECT_EQ(conn->shutdown_count, 0);
}
