// Requestor 单元测试：send 注册 / onResponse 匹配 / onTimeout 超时 / 回调派发
#include <gtest/gtest.h>
#include <memory>
#include <chrono>
#include <jsoncpp/json/json.h>
#include "src/client/requestor.hpp"

using lcz_rpc::client::Requestor;
using lcz_rpc::BaseConnection;
using lcz_rpc::BaseMessage;
using lcz_rpc::MessageFactory;
using lcz_rpc::RpcRequest;
using lcz_rpc::RpcResponse;
using lcz_rpc::RespCode;

namespace
{
    // 非 MuduoConnection：Requestor 检测不到 EventLoop，跳过定时器，测试手动驱动 onResponse/onTimeout
    class FakeConnection : public BaseConnection
    {
    public:
        void send(const BaseMessage::ptr &) override {}
        void shutdown() override {}
        bool connected() override { return true; }
        std::string peerAddress() const override { return "10.0.0.1:8080"; }
    };

    BaseMessage::ptr makeRpcRequest(const std::string &rid)
    {
        auto req = MessageFactory::create<RpcRequest>();
        req->setId(rid);
        req->setMethod("add");
        Json::Value p(Json::objectValue);
        p["a"] = 1;
        req->setParams(p);
        return req;
    }
}

// 异步 send 成功注册，onResponse 命中 rid 后 future 就绪并携带响应
TEST(RequestorTest, OnResponseSetsFuture)
{
    Requestor req;
    auto conn = std::make_shared<FakeConnection>();
    auto req_msg = makeRpcRequest("rid-1");

    Requestor::AsyncResponse fut;
    ASSERT_TRUE(req.send(conn, req_msg, fut));

    auto resp = MessageFactory::create<RpcResponse>();
    resp->setId("rid-1");
    resp->setRcode(RespCode::SUCCESS);
    Json::Value result;
    result["x"] = 1;
    resp->setResult(result);
    BaseMessage::ptr r = resp;
    req.onResponse(conn, r);

    ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(fut.get()->rid(), "rid-1");
}

// onResponse 收到未知 rid → 忽略，不崩溃
TEST(RequestorTest, OnResponseUnknownIdIgnored)
{
    Requestor req;
    auto conn = std::make_shared<FakeConnection>();
    auto resp = MessageFactory::create<RpcResponse>();
    resp->setId("no-such-id");
    BaseMessage::ptr r = resp;
    EXPECT_NO_THROW(req.onResponse(conn, r));
}

// onTimeout 命中 rid → future 就绪，响应为 RpcResponse 且 rcode=TIMEOUT
TEST(RequestorTest, OnTimeoutSetsTimeoutResponse)
{
    Requestor req;
    auto conn = std::make_shared<FakeConnection>();
    auto req_msg = makeRpcRequest("rid-2");

    Requestor::AsyncResponse fut;
    ASSERT_TRUE(req.send(conn, req_msg, fut));

    req.onTimeout("rid-2");

    auto got = fut.get();
    auto rpc_resp = std::dynamic_pointer_cast<RpcResponse>(got);
    ASSERT_NE(rpc_resp, nullptr);
    EXPECT_EQ(rpc_resp->rcode(), RespCode::TIMEOUT);
    EXPECT_EQ(got->rid(), "rid-2");
}

// onTimeout 未知 rid → 忽略，不崩溃
TEST(RequestorTest, OnTimeoutUnknownIdIgnored)
{
    Requestor req;
    EXPECT_NO_THROW(req.onTimeout("no-such-id"));
}

// 回调式 send：onResponse 命中后触发回调并携带响应
TEST(RequestorTest, CallbackSendInvokesOnResponse)
{
    Requestor req;
    auto conn = std::make_shared<FakeConnection>();
    auto req_msg = makeRpcRequest("rid-3");

    bool called = false;
    std::string got_rid;
    ASSERT_TRUE(req.send(conn, req_msg, [&](const BaseMessage::ptr &m) {
        called = true;
        got_rid = m->rid();
    }));

    auto resp = MessageFactory::create<RpcResponse>();
    resp->setId("rid-3");
    resp->setRcode(RespCode::SUCCESS);
    Json::Value result;
    result["y"] = 2;
    resp->setResult(result);
    BaseMessage::ptr r = resp;
    req.onResponse(conn, r);

    EXPECT_TRUE(called);
    EXPECT_EQ(got_rid, "rid-3");
}
