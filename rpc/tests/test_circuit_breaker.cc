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

// 窗口内失败率达到阈值才 OPEN，且状态转换发生在触发阈值的那一次
TEST(NodeBreakerTest, FailuresOpenAtMinCalls)
{
    CircuitConfig cfg;
    cfg.min_calls = 3;
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
    cfg.min_calls = 1;
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
    cfg.min_calls = 1;
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
    cfg.min_calls = 1;
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
    cfg.min_calls = 1;
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
    cfg.min_calls = 1;
    cfg.open_duration_sec = 0;
    cfg.half_open_max_req = 1;
    NodeBreaker nb(cfg);
    nb.onFailure();    // -> OPEN
    EXPECT_TRUE(nb.allowRequest());  // HALF_OPEN，第 1 条探测放行
    EXPECT_FALSE(nb.allowRequest()); // 超过半开上限，拒绝
}

// ===================== 滑动窗口失败率 =====================

// 失败被成功隔开也算数：旧「连续失败」实现在这里永远不熔断
TEST(NodeBreakerTest, InterleavedFailuresOpenOnRate)
{
    CircuitConfig cfg;
    cfg.window_size = 4;
    cfg.min_calls = 4;
    cfg.failure_ratio_percent = 50;
    NodeBreaker nb(cfg);
    EXPECT_FALSE(nb.onFailure()); // 1/1
    EXPECT_FALSE(nb.onFailure()); // 2/2
    EXPECT_FALSE(nb.onSuccess()); // 2/3，成功的插入不再清零失败计数
    EXPECT_TRUE(nb.onFailure());  // 3/4 = 75% >= 50% -> OPEN
    EXPECT_EQ(nb.status().state, CircuitState::OPEN);
    EXPECT_EQ(nb.status().failures, 3);
    EXPECT_EQ(nb.status().successes, 1);
}

// 失败率低于阈值：样本够了也不熔断
TEST(NodeBreakerTest, LowFailureRateStaysClosed)
{
    CircuitConfig cfg;
    cfg.window_size = 10;
    cfg.min_calls = 10;
    cfg.failure_ratio_percent = 50;
    NodeBreaker nb(cfg);
    for (int i = 0; i < 4; ++i) EXPECT_FALSE(nb.onFailure());
    for (int i = 0; i < 6; ++i) EXPECT_FALSE(nb.onSuccess());
    EXPECT_EQ(nb.status().state, CircuitState::CLOSED);
    EXPECT_EQ(nb.status().failures, 4);
    EXPECT_EQ(nb.status().successes, 6);
}

// 样本数不足 min_calls：即使 100% 失败也不熔断
TEST(NodeBreakerTest, BelowMinCallsStaysClosed)
{
    CircuitConfig cfg;
    cfg.window_size = 10;
    cfg.min_calls = 5;
    NodeBreaker nb(cfg);
    for (int i = 0; i < 4; ++i) EXPECT_FALSE(nb.onFailure());
    EXPECT_EQ(nb.status().state, CircuitState::CLOSED);
    EXPECT_TRUE(nb.onFailure()); // 第 5 次：5/5 -> OPEN
    EXPECT_EQ(nb.status().state, CircuitState::OPEN);
}

// 窗口滚动：旧失败被新成功挤出后，失败率随之回落
TEST(NodeBreakerTest, WindowRollsOffOldFailures)
{
    CircuitConfig cfg;
    cfg.window_size = 4;
    cfg.min_calls = 4;
    cfg.failure_ratio_percent = 75;
    NodeBreaker nb(cfg);
    EXPECT_FALSE(nb.onFailure()); // F . . .
    EXPECT_FALSE(nb.onFailure()); // F F . .
    EXPECT_FALSE(nb.onSuccess()); // F F S .  -> 2/3 = 66% < 75%
    EXPECT_FALSE(nb.onSuccess()); // F F S S  -> 2/4 = 50% < 75%
    EXPECT_FALSE(nb.onSuccess()); // . F S S  -> 1/4 = 25%
    EXPECT_FALSE(nb.onSuccess()); // . . S S  -> 0/4
    EXPECT_EQ(nb.status().failures, 0);
    EXPECT_EQ(nb.status().successes, 4);
    // 再进 3 次失败：S F F F -> 3/4 = 75% -> OPEN
    EXPECT_FALSE(nb.onFailure());
    EXPECT_FALSE(nb.onFailure());
    EXPECT_TRUE(nb.onFailure());
    EXPECT_EQ(nb.status().state, CircuitState::OPEN);
}

// 半开探测成功恢复后窗口清零：打开前的历史失败不能立刻再次熔断
TEST(NodeBreakerTest, RecoveryClearsWindow)
{
    CircuitConfig cfg;
    cfg.window_size = 4;
    cfg.min_calls = 3;
    cfg.failure_ratio_percent = 50;
    cfg.open_duration_sec = 0;
    NodeBreaker nb(cfg);
    nb.onFailure();
    nb.onFailure();
    EXPECT_TRUE(nb.onFailure());   // 3/3 -> OPEN
    EXPECT_TRUE(nb.allowRequest()); // -> HALF_OPEN
    EXPECT_TRUE(nb.onSuccess());    // -> CLOSED
    EXPECT_EQ(nb.status().failures, 0);
    EXPECT_EQ(nb.status().successes, 0);
    EXPECT_FALSE(nb.onFailure()); // 1/1，样本不足 min_calls=3，不熔断
    EXPECT_EQ(nb.status().state, CircuitState::CLOSED);
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
    cfg.min_calls = 3;
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
    cfg.min_calls = 1;
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
