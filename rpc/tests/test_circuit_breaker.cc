// 熔断器单元测试：NodeBreaker 状态机 + CircuitBreaker 管理/持久化
#include <gtest/gtest.h>
#include <ctime>
#include <memory>
#include "src/client/node_breaker.hpp"
#include "src/client/circuit_breaker.hpp"
#include "src/server/memory_circuit_store.hpp"

using lcz_rpc::client::NodeBreaker;
using lcz_rpc::client::CircuitBreaker;
using lcz_rpc::server::MemoryCircuitStore;
using lcz_rpc::CircuitConfig;
using lcz_rpc::CircuitStatus;
using lcz_rpc::CircuitState;

// ===================== NodeBreaker 状态机 =====================

// CLOSED 状态默认放行
TEST(NodeBreakerTest, ClosedAllowsRequest)
{
    CircuitConfig cfg;
    NodeBreaker nb(cfg);
    EXPECT_TRUE(nb.allowRequest());
    EXPECT_EQ(nb.status().state, CircuitState::CLOSED);
}

// 连续失败达到阈值才 OPEN，且第 threshold 次返回 true（状态转换）
TEST(NodeBreakerTest, FailuresOpenCircuitAtThreshold)
{
    CircuitConfig cfg;
    cfg.failure_threshold = 3;
    NodeBreaker nb(cfg);
    EXPECT_FALSE(nb.onFailure()); // failures=1
    EXPECT_FALSE(nb.onFailure()); // failures=2
    EXPECT_TRUE(nb.onFailure());  // failures=3 -> OPEN
    EXPECT_EQ(nb.status().state, CircuitState::OPEN);
    EXPECT_EQ(nb.status().failures, 3);
}

// OPEN 冷却期内拦截请求
TEST(NodeBreakerTest, OpenBlocksRequestBeforeDuration)
{
    CircuitConfig cfg;
    cfg.failure_threshold = 1;
    cfg.open_duration_sec = 1000;
    NodeBreaker nb(cfg);
    nb.onFailure(); // -> OPEN
    EXPECT_EQ(nb.status().state, CircuitState::OPEN);
    EXPECT_FALSE(nb.allowRequest());
}

// OPEN 超过冷却期后转 HALF_OPEN 并放行探测（open_duration_sec=0 立即触发）
TEST(NodeBreakerTest, OpenTransitionsToHalfOpenAfterDuration)
{
    CircuitConfig cfg;
    cfg.failure_threshold = 1;
    cfg.open_duration_sec = 0;
    NodeBreaker nb(cfg);
    nb.onFailure(); // -> OPEN
    EXPECT_EQ(nb.status().state, CircuitState::OPEN);
    EXPECT_TRUE(nb.allowRequest()); // -> HALF_OPEN，放行探测
    EXPECT_EQ(nb.status().state, CircuitState::HALF_OPEN);
}

// HALF_OPEN 探测成功 -> CLOSED，且清零失败计数
TEST(NodeBreakerTest, HalfOpenSuccessCloses)
{
    CircuitConfig cfg;
    cfg.failure_threshold = 1;
    cfg.open_duration_sec = 0;
    NodeBreaker nb(cfg);
    nb.onFailure();    // -> OPEN
    nb.allowRequest(); // -> HALF_OPEN
    EXPECT_TRUE(nb.onSuccess()); // -> CLOSED
    EXPECT_EQ(nb.status().state, CircuitState::CLOSED);
    EXPECT_EQ(nb.status().failures, 0);
    EXPECT_EQ(nb.status().half_open, 0);
}

// HALF_OPEN 探测失败 -> 重新 OPEN
TEST(NodeBreakerTest, HalfOpenFailureReopens)
{
    CircuitConfig cfg;
    cfg.failure_threshold = 1;
    cfg.open_duration_sec = 0;
    NodeBreaker nb(cfg);
    nb.onFailure();    // -> OPEN
    nb.allowRequest(); // -> HALF_OPEN
    EXPECT_TRUE(nb.onFailure()); // -> OPEN
    EXPECT_EQ(nb.status().state, CircuitState::OPEN);
}

// CLOSED 状态下 onSuccess 不产生转换
TEST(NodeBreakerTest, SuccessWhileClosedReturnsNoTransition)
{
    CircuitConfig cfg;
    NodeBreaker nb(cfg);
    EXPECT_FALSE(nb.onSuccess());
    EXPECT_EQ(nb.status().state, CircuitState::CLOSED);
}

// HALF_OPEN 只放行 half_open_max_req 条探测
TEST(NodeBreakerTest, HalfOpenLimitsProbeRequests)
{
    CircuitConfig cfg;
    cfg.failure_threshold = 1;
    cfg.open_duration_sec = 0;
    cfg.half_open_max_req = 1;
    NodeBreaker nb(cfg);
    nb.onFailure();    // -> OPEN
    EXPECT_TRUE(nb.allowRequest());  // HALF_OPEN，第 1 条探测放行
    EXPECT_FALSE(nb.allowRequest()); // 超过半开上限，拒绝
}

// ===================== CircuitBreaker 管理 + 持久化 =====================

// allowRequest 首次调用创建 CLOSED 节点
TEST(CircuitBreakerTest, AllowRequestCreatesClosedNode)
{
    auto store = std::make_shared<MemoryCircuitStore>();
    CircuitConfig cfg;
    CircuitBreaker cb(cfg, store);
    EXPECT_TRUE(cb.allowRequest("echo", "h:1"));
    EXPECT_EQ(cb.status("echo", "h:1").state, CircuitState::CLOSED);
}

// 失败达到阈值 -> OPEN 且持久化到 store
TEST(CircuitBreakerTest, FailureOpensAndPersists)
{
    auto store = std::make_shared<MemoryCircuitStore>();
    CircuitConfig cfg;
    cfg.failure_threshold = 3;
    CircuitBreaker cb(cfg, store);
    cb.onFailure("echo", "h:1");
    cb.onFailure("echo", "h:1");
    cb.onFailure("echo", "h:1"); // -> OPEN
    EXPECT_EQ(cb.status("echo", "h:1").state, CircuitState::OPEN);
    EXPECT_EQ(store->load("echo", "h:1").state, CircuitState::OPEN);
}

// 恢复后成功 -> CLOSED 且持久化
TEST(CircuitBreakerTest, SuccessClosesAndPersists)
{
    auto store = std::make_shared<MemoryCircuitStore>();
    CircuitConfig cfg;
    cfg.failure_threshold = 1;
    cfg.open_duration_sec = 0;
    CircuitBreaker cb(cfg, store);
    cb.onFailure("echo", "h:1");    // OPEN
    cb.allowRequest("echo", "h:1"); // HALF_OPEN
    cb.onSuccess("echo", "h:1");    // CLOSED
    EXPECT_EQ(cb.status("echo", "h:1").state, CircuitState::CLOSED);
    EXPECT_EQ(store->load("echo", "h:1").state, CircuitState::CLOSED);
}

// removeNode 清除内存与持久化状态
TEST(CircuitBreakerTest, RemoveNodeClearsState)
{
    auto store = std::make_shared<MemoryCircuitStore>();
    CircuitConfig cfg;
    CircuitBreaker cb(cfg, store);
    cb.allowRequest("echo", "h:1");
    cb.removeNode("echo", "h:1");
    EXPECT_EQ(cb.status("echo", "h:1").state, CircuitState::CLOSED);
    EXPECT_EQ(store->load("echo", "h:1").state, CircuitState::CLOSED);
}

// 恢复持久化的 OPEN（已过冷却期）-> HALF_OPEN 放行探测
TEST(CircuitBreakerTest, RecoversExpiredOpenToHalfOpen)
{
    auto store = std::make_shared<MemoryCircuitStore>();
    CircuitConfig cfg;
    cfg.open_duration_sec = 30;
    CircuitStatus s;
    s.state = CircuitState::OPEN;
    s.opened_at = std::time(nullptr) - 100; // 100s 前打开，已过 30s 冷却期
    store->save("echo", "h:1", s);
    CircuitBreaker cb(cfg, store);
    EXPECT_TRUE(cb.allowRequest("echo", "h:1")); // 过期 -> HALF_OPEN，放行
    EXPECT_EQ(cb.status("echo", "h:1").state, CircuitState::HALF_OPEN);
}

// 恢复持久化的 OPEN（未过冷却期）-> 仍拦截请求，不提前降级为 HALF_OPEN
// 回归 _opened_since 未随 loadStatus 恢复的 bug：epoch 默认值让 allowRequest 误判冷却期已过
TEST(CircuitBreakerTest, RecoversUnexpiredOpenStillBlocks)
{
    auto store = std::make_shared<MemoryCircuitStore>();
    CircuitConfig cfg;
    cfg.open_duration_sec = 30;
    CircuitStatus s;
    s.state = CircuitState::OPEN;
    s.opened_at = std::time(nullptr) - 10; // 10s 前打开，未过 30s 冷却期
    store->save("echo", "h:1", s);
    CircuitBreaker cb(cfg, store);
    EXPECT_FALSE(cb.allowRequest("echo", "h:1")); // 未过期 -> 仍 OPEN，拦截
    EXPECT_EQ(cb.status("echo", "h:1").state, CircuitState::OPEN);
}
