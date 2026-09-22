// RpcCaller 单元测试：熔断拒绝 / 发送超时 / 成功 / 响应码→错误分类（fromRespCode 间接覆盖）
#include <gtest/gtest.h>
#include <memory>
#include <chrono>
#include <jsoncpp/json/json.h>
#include "src/client/caller.hpp"
#include "src/server/memory_circuit_store.hpp"

using lcz_rpc::client::RpcCaller;
using lcz_rpc::client::Requestor;
using lcz_rpc::client::CircuitBreaker;
using lcz_rpc::server::MemoryCircuitStore;
using lcz_rpc::BaseConnection;
using lcz_rpc::BaseMessage;
using lcz_rpc::MessageFactory;
using lcz_rpc::RpcResponse;
using lcz_rpc::RpcError;
using lcz_rpc::RespCode;
using lcz_rpc::CircuitConfig;

namespace
{
    // 可配置连接：echo=true 时在 send 内同步回灌一条 RpcResponse（rcode/result 可设），
    // 否则不响应 → 触发调用方超时。非 MuduoConnection，Requestor 跳过定时器、靠 wait_for 超时。
    class FakeConnection : public BaseConnection, public std::enable_shared_from_this<FakeConnection>
    {
    public:
        Requestor::ptr requestor;
        bool echo = false;
        RespCode rcode = RespCode::SUCCESS;
        Json::Value result;
        int send_count = 0;

        void send(const BaseMessage::ptr &msg) override
        {
            ++send_count;
            if (echo && requestor)
            {
                auto resp = MessageFactory::create<RpcResponse>();
                resp->setId(msg->rid());
                resp->setRcode(rcode);
                resp->setResult(result);
                BaseMessage::ptr r = resp;
                requestor->onResponse(shared_from_this(), r);
            }
        }
        void shutdown() override {}
        bool connected() override { return true; }
        std::string peerAddress() const override { return "10.0.0.1:8080"; }
    };

    Json::Value makeParams()
    {
        Json::Value p(Json::objectValue);
        p["a"] = 1;
        return p;
    }
}

// 熔断器打开（failure_threshold=1 + 一次失败）→ 拒绝请求，err=CIRCUIT_OPEN
TEST(CallerTest, CircuitOpenReturnsCircuitOpen)
{
    CircuitConfig cfg;
    cfg.failure_threshold = 1;
    cfg.open_duration_sec = 1000; // 冷却期内保持 OPEN
    auto breaker = std::make_shared<CircuitBreaker>(cfg, std::make_shared<MemoryCircuitStore>());
    auto requestor = std::make_shared<Requestor>();
    RpcCaller caller(requestor, breaker);
    auto conn = std::make_shared<FakeConnection>();

    breaker->onFailure("add", "10.0.0.1:8080"); // 触发 OPEN

    Json::Value result;
    RpcError err = RpcError::OK;
    EXPECT_FALSE(caller.call(conn, "add", makeParams(), result, &err));
    EXPECT_EQ(err, RpcError::CIRCUIT_OPEN);
    EXPECT_EQ(conn->send_count, 0); // 熔断直接拦截，不发请求
}

// 连接无响应 → 同步发送超时，err=TIMEOUT
TEST(CallerTest, SendTimeoutReturnsTimeout)
{
    auto requestor = std::make_shared<Requestor>();
    RpcCaller caller(requestor, nullptr); // 无熔断器
    auto conn = std::make_shared<FakeConnection>(); // echo=false

    Json::Value result;
    RpcError err = RpcError::OK;
    EXPECT_FALSE(caller.call(conn, "add", makeParams(), result, &err,
                             std::chrono::milliseconds(20)));
    EXPECT_EQ(err, RpcError::TIMEOUT);
}

// 正常响应 → 成功，err=OK，result 正确
TEST(CallerTest, SuccessSetsOkAndResult)
{
    auto requestor = std::make_shared<Requestor>();
    RpcCaller caller(requestor, nullptr);
    auto conn = std::make_shared<FakeConnection>();
    conn->requestor = requestor;
    conn->echo = true;
    conn->rcode = RespCode::SUCCESS;
    Json::Value expect;
    expect["x"] = 42;
    conn->result = expect;

    Json::Value result;
    RpcError err = RpcError::OK;
    EXPECT_TRUE(caller.call(conn, "add", makeParams(), result, &err));
    EXPECT_EQ(err, RpcError::OK);
    EXPECT_EQ(result["x"].asInt(), 42);
}

// 服务端 TIMEOUT 响应码 → 客户端分类为可重试的 TIMEOUT
TEST(CallerTest, RemoteTimeoutMapsToTimeout)
{
    auto requestor = std::make_shared<Requestor>();
    RpcCaller caller(requestor, nullptr);
    auto conn = std::make_shared<FakeConnection>();
    conn->requestor = requestor;
    conn->echo = true;
    conn->rcode = RespCode::TIMEOUT;

    Json::Value result;
    RpcError err = RpcError::OK;
    EXPECT_FALSE(caller.call(conn, "add", makeParams(), result, &err));
    EXPECT_EQ(err, RpcError::TIMEOUT);
}

// 服务端 BACKOFF 响应码 → 客户端分类为可重试的 BACKOFF
TEST(CallerTest, RemoteBackoffMapsToBackoff)
{
    auto requestor = std::make_shared<Requestor>();
    RpcCaller caller(requestor, nullptr);
    auto conn = std::make_shared<FakeConnection>();
    conn->requestor = requestor;
    conn->echo = true;
    conn->rcode = RespCode::BACKOFF;

    Json::Value result;
    RpcError err = RpcError::OK;
    EXPECT_FALSE(caller.call(conn, "add", makeParams(), result, &err));
    EXPECT_EQ(err, RpcError::BACKOFF);
}

// 服务端业务错误（INVALID_PARAMS）→ 客户端分类为不可重试的 SERVICE_ERROR
TEST(CallerTest, RemoteBusinessErrorMapsToServiceError)
{
    auto requestor = std::make_shared<Requestor>();
    RpcCaller caller(requestor, nullptr);
    auto conn = std::make_shared<FakeConnection>();
    conn->requestor = requestor;
    conn->echo = true;
    conn->rcode = RespCode::INVALID_PARAMS;

    Json::Value result;
    RpcError err = RpcError::OK;
    EXPECT_FALSE(caller.call(conn, "add", makeParams(), result, &err));
    EXPECT_EQ(err, RpcError::SERVICE_ERROR);
}
