// MemoryCircuitStore 单元测试：save/load/remove 纯内存 CRUD 行为
#include <gtest/gtest.h>
#include "src/server/memory_circuit_store.hpp"

using lcz_rpc::server::MemoryCircuitStore;
using lcz_rpc::CircuitStatus;
using lcz_rpc::CircuitState;

// 保存后能完整读回（所有字段）
TEST(MemoryCircuitStoreTest, SaveAndLoadRoundTrip)
{
    MemoryCircuitStore store;
    CircuitStatus s;
    s.state = CircuitState::OPEN;
    s.failures = 3;
    s.half_open = 1;
    s.opened_at = 12345;
    EXPECT_TRUE(store.save("echo", "10.0.0.1:8080", s));

    CircuitStatus loaded = store.load("echo", "10.0.0.1:8080");
    EXPECT_EQ(loaded.state, CircuitState::OPEN);
    EXPECT_EQ(loaded.failures, 3);
    EXPECT_EQ(loaded.half_open, 1);
    EXPECT_EQ(loaded.opened_at, 12345);
}

// 读不存在的 method×host 返回默认（CLOSED）
TEST(MemoryCircuitStoreTest, LoadMissingReturnsClosedDefault)
{
    MemoryCircuitStore store;
    CircuitStatus s = store.load("echo", "10.0.0.1:8080");
    EXPECT_EQ(s.state, CircuitState::CLOSED);
    EXPECT_EQ(s.failures, 0);
    EXPECT_EQ(s.half_open, 0);
    EXPECT_EQ(s.opened_at, 0);
}

// 删除已存在的记录返回 true，且读回默认
TEST(MemoryCircuitStoreTest, RemoveExistingReturnsTrue)
{
    MemoryCircuitStore store;
    CircuitStatus s;
    EXPECT_TRUE(store.save("echo", "10.0.0.1:8080", s));
    EXPECT_TRUE(store.remove("echo", "10.0.0.1:8080"));
    EXPECT_EQ(store.load("echo", "10.0.0.1:8080").state, CircuitState::CLOSED);
}

// 删除不存在的记录返回 false
TEST(MemoryCircuitStoreTest, RemoveMissingReturnsFalse)
{
    MemoryCircuitStore store;
    EXPECT_FALSE(store.remove("echo", "10.0.0.1:8080"));
    EXPECT_FALSE(store.remove("echo", "10.0.0.1:8080"));
}

// 后保存覆盖先前保存
TEST(MemoryCircuitStoreTest, SaveOverwritesPrevious)
{
    MemoryCircuitStore store;
    CircuitStatus open;
    open.state = CircuitState::OPEN;
    CircuitStatus closed;
    closed.state = CircuitState::CLOSED;
    store.save("echo", "h:1", open);
    store.save("echo", "h:1", closed);
    EXPECT_EQ(store.load("echo", "h:1").state, CircuitState::CLOSED);
}

// 不同 method / 不同 host 互不影响
TEST(MemoryCircuitStoreTest, MethodsAndHostsAreIndependent)
{
    MemoryCircuitStore store;
    CircuitStatus s;
    store.save("echo", "h1", s);
    store.save("add", "h2", s);

    // 交叉查不到
    EXPECT_EQ(store.load("echo", "h2").state, CircuitState::CLOSED);
    EXPECT_EQ(store.load("add", "h1").state, CircuitState::CLOSED);

    // 删除一个 host 不影响另一个 method 的记录
    EXPECT_TRUE(store.remove("echo", "h1"));
    EXPECT_EQ(store.load("echo", "h1").state, CircuitState::CLOSED);
    EXPECT_EQ(store.load("add", "h2").state, CircuitState::CLOSED);
}
