// RpcRouter 单元测试：ServiceDescribe/ServiceFactory/ServiceManager + JSON/Proto 路由派发
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <stdexcept>
#include "src/server/rpc_router.hpp"
#include "src/general/abstract.hpp"

using lcz_rpc::server::ValType;
using lcz_rpc::server::ServiceDescribe;
using lcz_rpc::server::ServiceFactory;
using lcz_rpc::server::ServiceManager;
using lcz_rpc::server::RpcRouter;
using lcz_rpc::server::ProtoRpcRouter;
using lcz_rpc::BaseConnection;
using lcz_rpc::BaseMessage;
using lcz_rpc::RpcRequest;
using lcz_rpc::RpcResponse;
using lcz_rpc::ProtoRpcRequest;
using lcz_rpc::ProtoRpcResponse;
using lcz_rpc::MessageFactory;
using lcz_rpc::RespCode;
using lcz_rpc::TokenBucket;

namespace
{
    // 记录发送的消息，供断言响应 rcode/result
    class RecordingConnection : public BaseConnection
    {
    public:
        std::vector<BaseMessage::ptr> sent;
        void send(const BaseMessage::ptr &msg) override { sent.push_back(msg); }
        void shutdown() override {}
        bool connected() override { return true; }
        std::string peerAddress() const override { return "rec:1"; }
    };

    // method=echo，参数 a:string、b:int，返回 string（a + b）
    ServiceDescribe::ptr makeEchoService()
    {
        auto cb = [](const Json::Value &params, Json::Value &result) {
            result = params["a"].asString() + std::to_string(params["b"].asInt());
        };
        std::vector<ServiceDescribe::ParamsDescribe> desc = {
            {"a", ValType::STRING},
            {"b", ValType::INTEGRAL},
        };
        return std::make_shared<ServiceDescribe>("echo", std::move(cb), std::move(desc), ValType::STRING);
    }

    Json::Value makeParams(const std::string &a, int b)
    {
        Json::Value p(Json::objectValue);
        p["a"] = a;
        p["b"] = b;
        return p;
    }
}

// ===================== ServiceDescribe =====================

// 缺字段或字段类型不符 → checkParams false
TEST(ServiceDescribeTest, CheckParamsValidatesFields)
{
    auto svc = makeEchoService();
    EXPECT_FALSE(svc->checkParams(Json::Value(Json::objectValue))); // 缺 a、b

    Json::Value missing_b(Json::objectValue);
    missing_b["a"] = "x";
    EXPECT_FALSE(svc->checkParams(missing_b)); // 缺 b

    Json::Value wrong_type(Json::objectValue);
    wrong_type["a"] = 123; // a 应为 string
    wrong_type["b"] = 1;
    EXPECT_FALSE(svc->checkParams(wrong_type));

    EXPECT_TRUE(svc->checkParams(makeParams("x", 1)));
}

// 返回值类型匹配 → call true，否则 false
TEST(ServiceDescribeTest, CallChecksReturnType)
{
    auto svc = makeEchoService();
    Json::Value result;
    EXPECT_TRUE(svc->call(makeParams("x", 1), result));
    EXPECT_EQ(result.asString(), "x1");

    auto bad_cb = [](const Json::Value &, Json::Value &r) { r = 42; }; // 返回 int，但 return_type=STRING
    std::vector<ServiceDescribe::ParamsDescribe> desc = {{"a", ValType::STRING}};
    ServiceDescribe svc2("bad", std::move(bad_cb), std::move(desc), ValType::STRING);
    Json::Value result2;
    EXPECT_FALSE(svc2.call(Json::Value(Json::objectValue), result2));
}

TEST(ServiceDescribeTest, GetMethodname)
{
    EXPECT_EQ(makeEchoService()->getMethodname(), "echo");
}

// ===================== ServiceFactory =====================

TEST(ServiceFactoryTest, BuildProducesDescribe)
{
    ServiceFactory f;
    f.setMethodName("add");
    f.setReturntype(ValType::INTEGRAL);
    f.setParamdescribe("x", ValType::INTEGRAL);
    f.setParamdescribe("y", ValType::INTEGRAL);
    f.setServiceCallback([](const Json::Value &p, Json::Value &r) { r = p["x"].asInt() + p["y"].asInt(); });

    auto svc = f.build();
    EXPECT_EQ(svc->getMethodname(), "add");

    Json::Value params(Json::objectValue);
    params["x"] = 2;
    params["y"] = 3;
    EXPECT_TRUE(svc->checkParams(params));
    Json::Value result;
    EXPECT_TRUE(svc->call(params, result));
    EXPECT_EQ(result.asInt(), 5);
}

// ===================== ServiceManager =====================

TEST(ServiceManagerTest, AddSelectRemove)
{
    ServiceManager mgr;
    auto svc = makeEchoService();
    mgr.add(svc);

    EXPECT_EQ(mgr.select("echo"), svc);
    EXPECT_EQ(mgr.select("nope"), nullptr);

    EXPECT_TRUE(mgr.remove("echo"));
    EXPECT_EQ(mgr.select("echo"), nullptr);
    EXPECT_FALSE(mgr.remove("echo")); // 已删除
}

// ===================== RpcRouter =====================

TEST(RpcRouterTest, ServiceNotFoundReturnsCode)
{
    RpcRouter router;
    auto conn = std::make_shared<RecordingConnection>();
    auto req = MessageFactory::create<RpcRequest>();
    req->setMethod("nope");
    req->setParams(Json::Value(Json::objectValue));
    router.onrpcRequst(conn, req);

    ASSERT_EQ(conn->sent.size(), 1u);
    auto resp = std::dynamic_pointer_cast<RpcResponse>(conn->sent[0]);
    ASSERT_NE(resp, nullptr);
    EXPECT_EQ(resp->rcode(), RespCode::SERVICE_NOT_FOUND);
}

TEST(RpcRouterTest, InvalidParamsReturnsCode)
{
    RpcRouter router;
    router.registerMethod(makeEchoService());
    auto conn = std::make_shared<RecordingConnection>();
    auto req = MessageFactory::create<RpcRequest>();
    req->setMethod("echo");
    req->setParams(Json::Value(Json::objectValue)); // 缺 a、b
    router.onrpcRequst(conn, req);

    ASSERT_EQ(conn->sent.size(), 1u);
    auto resp = std::dynamic_pointer_cast<RpcResponse>(conn->sent[0]);
    EXPECT_EQ(resp->rcode(), RespCode::INVALID_PARAMS);
}

TEST(RpcRouterTest, SuccessReturnsResult)
{
    RpcRouter router;
    router.registerMethod(makeEchoService());
    auto conn = std::make_shared<RecordingConnection>();
    auto req = MessageFactory::create<RpcRequest>();
    req->setMethod("echo");
    req->setParams(makeParams("x", 1));
    router.onrpcRequst(conn, req);

    ASSERT_EQ(conn->sent.size(), 1u);
    auto resp = std::dynamic_pointer_cast<RpcResponse>(conn->sent[0]);
    EXPECT_EQ(resp->rcode(), RespCode::SUCCESS);
    EXPECT_EQ(resp->result().asString(), "x1");
}

TEST(RpcRouterTest, CallbackErrorReturnsInternalError)
{
    RpcRouter router;
    auto bad = std::make_shared<ServiceDescribe>(
        "bad",
        [](const Json::Value &, Json::Value &r) { r = 42; }, // 返回 int，但 return_type=STRING
        std::vector<ServiceDescribe::ParamsDescribe>{},
        ValType::STRING);
    router.registerMethod(bad);
    auto conn = std::make_shared<RecordingConnection>();
    auto req = MessageFactory::create<RpcRequest>();
    req->setMethod("bad");
    req->setParams(Json::Value(Json::objectValue));
    router.onrpcRequst(conn, req);

    ASSERT_EQ(conn->sent.size(), 1u);
    auto resp = std::dynamic_pointer_cast<RpcResponse>(conn->sent[0]);
    EXPECT_EQ(resp->rcode(), RespCode::INTERNAL_ERROR);
}

TEST(RpcRouterTest, RateLimiterBackoffWhenEmpty)
{
    RpcRouter router;
    router.setRateLimiter(std::make_shared<TokenBucket>(1.0, 0.0)); // 桶容量 0，始终拒绝
    router.registerMethod(makeEchoService());
    auto conn = std::make_shared<RecordingConnection>();
    auto req = MessageFactory::create<RpcRequest>();
    req->setMethod("echo");
    req->setParams(makeParams("x", 1));
    router.onrpcRequst(conn, req);

    ASSERT_EQ(conn->sent.size(), 1u);
    auto resp = std::dynamic_pointer_cast<RpcResponse>(conn->sent[0]);
    EXPECT_EQ(resp->rcode(), RespCode::BACKOFF);
    EXPECT_GT(resp->retryAfterMs(), 0);
}

// ===================== ProtoRpcRouter =====================

TEST(ProtoRpcRouterTest, UnknownMethodReturnsServiceNotFound)
{
    ProtoRpcRouter router;
    auto conn = std::make_shared<RecordingConnection>();
    auto req = MessageFactory::create<ProtoRpcRequest>();
    req->setMethod("nope");
    router.onProtoRequest(conn, req);

    ASSERT_EQ(conn->sent.size(), 1u);
    auto resp = std::dynamic_pointer_cast<ProtoRpcResponse>(conn->sent[0]);
    ASSERT_NE(resp, nullptr);
    EXPECT_EQ(resp->rcode(), RespCode::SERVICE_NOT_FOUND);
}

TEST(ProtoRpcRouterTest, HandlerSuccessSerializesResponse)
{
    ProtoRpcRouter router;
    router.registerProtoHandler<lcz_rpc::proto::RpcRequestEnvelope, lcz_rpc::proto::RpcResponseEnvelope>(
        "echo", [](const BaseConnection::ptr &, const lcz_rpc::proto::RpcRequestEnvelope &req,
                   lcz_rpc::proto::RpcResponseEnvelope *resp) {
            resp->set_body("echo:" + req.body());
        });

    lcz_rpc::proto::RpcRequestEnvelope env;
    env.set_body("hello");
    std::string body;
    ASSERT_TRUE(env.SerializeToString(&body));

    auto conn = std::make_shared<RecordingConnection>();
    auto req = MessageFactory::create<ProtoRpcRequest>();
    req->setMethod("echo");
    req->setBody(body);
    router.onProtoRequest(conn, req);

    ASSERT_EQ(conn->sent.size(), 1u);
    auto resp = std::dynamic_pointer_cast<ProtoRpcResponse>(conn->sent[0]);
    ASSERT_NE(resp, nullptr);
    EXPECT_EQ(resp->rcode(), RespCode::SUCCESS);
    // ProtoRpcRouter 把 handler 生成的 RpcResponseEnvelope 序列化后塞进 body，
    // 这里再解一层，断言内层 body 已被 handler 正确回填
    lcz_rpc::proto::RpcResponseEnvelope out;
    ASSERT_TRUE(out.ParseFromString(resp->body()));
    EXPECT_EQ(out.body(), "echo:hello");
}

TEST(ProtoRpcRouterTest, ParseFailedReturnsCode)
{
    ProtoRpcRouter router;
    router.registerProtoHandler<lcz_rpc::proto::RpcRequestEnvelope, lcz_rpc::proto::RpcResponseEnvelope>(
        "echo", [](const BaseConnection::ptr &, const lcz_rpc::proto::RpcRequestEnvelope &,
                   lcz_rpc::proto::RpcResponseEnvelope *) {});

    auto conn = std::make_shared<RecordingConnection>();
    auto req = MessageFactory::create<ProtoRpcRequest>();
    req->setMethod("echo");
    req->setBody(std::string("\xFF\xFF", 2)); // 非法 wire type，必然解析失败
    router.onProtoRequest(conn, req);

    ASSERT_EQ(conn->sent.size(), 1u);
    auto resp = std::dynamic_pointer_cast<ProtoRpcResponse>(conn->sent[0]);
    EXPECT_EQ(resp->rcode(), RespCode::PARSE_FAILED);
}

TEST(ProtoRpcRouterTest, HandlerExceptionReturnsInternalError)
{
    ProtoRpcRouter router;
    router.registerProtoHandler<lcz_rpc::proto::RpcRequestEnvelope, lcz_rpc::proto::RpcResponseEnvelope>(
        "boom", [](const BaseConnection::ptr &, const lcz_rpc::proto::RpcRequestEnvelope &,
                   lcz_rpc::proto::RpcResponseEnvelope *) { throw std::runtime_error("boom"); });

    lcz_rpc::proto::RpcRequestEnvelope env; // 空 body 也合法
    std::string body;
    ASSERT_TRUE(env.SerializeToString(&body));

    auto conn = std::make_shared<RecordingConnection>();
    auto req = MessageFactory::create<ProtoRpcRequest>();
    req->setMethod("boom");
    req->setBody(body);
    router.onProtoRequest(conn, req);

    ASSERT_EQ(conn->sent.size(), 1u);
    auto resp = std::dynamic_pointer_cast<ProtoRpcResponse>(conn->sent[0]);
    EXPECT_EQ(resp->rcode(), RespCode::INTERNAL_ERROR);
}
