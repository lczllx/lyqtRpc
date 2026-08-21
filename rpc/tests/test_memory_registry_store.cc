// MemoryRegistryStore 单元测试：注册/发现/负载上报/心跳/超时扫描/连接断开
#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <memory>
#include <thread>
#include "src/server/memory_registry_store.hpp"
#include "src/general/abstract.hpp"

using lcz_rpc::server::MemoryRegistryStore;
using lcz_rpc::BaseConnection;
using lcz_rpc::BaseMessage;
using lcz_rpc::HostInfo;
using lcz_rpc::HostDetail;

namespace
{
    // 测试用最小 BaseConnection 实现：仅作为 Provider 的 map key，不真正收发
    class MockConnection : public BaseConnection
    {
    public:
        void send(const BaseMessage::ptr &) override {}
        void shutdown() override {}
        bool connected() override { return true; }
        std::string peerAddress() const override { return "mock"; }
    };
}

// 注册后可按 method 发现主机
TEST(MemoryRegistryStoreTest, RegisterAndDiscover)
{
    MemoryRegistryStore store;
    auto conn = std::make_shared<MockConnection>();
    HostInfo host("10.0.0.1", 8080);
    store.registerInstance(conn, host, "echo", 5);

    auto hosts = store.methodHost("echo");
    ASSERT_EQ(hosts.size(), 1u);
    EXPECT_EQ(hosts[0], host);
}

// 发现未注册的方法返回空
TEST(MemoryRegistryStoreTest, DiscoverUnknownMethodEmpty)
{
    MemoryRegistryStore store;
    EXPECT_TRUE(store.methodHost("nope").empty());
    EXPECT_TRUE(store.methodHostDetails("nope").empty());
}

// methodHostDetails 携带负载值
TEST(MemoryRegistryStoreTest, MethodHostDetailsCarriesLoad)
{
    MemoryRegistryStore store;
    auto conn = std::make_shared<MockConnection>();
    store.registerInstance(conn, HostInfo("10.0.0.1", 8080), "echo", 42);

    auto details = store.methodHostDetails("echo");
    ASSERT_EQ(details.size(), 1u);
    EXPECT_EQ(details[0].host, HostInfo("10.0.0.1", 8080));
    EXPECT_EQ(details[0].load, 42);
}

// 负载上报更新负载值
TEST(MemoryRegistryStoreTest, ReportLoadUpdatesLoad)
{
    MemoryRegistryStore store;
    auto conn = std::make_shared<MockConnection>();
    HostInfo host("10.0.0.1", 8080);
    store.registerInstance(conn, host, "echo", 5);

    EXPECT_TRUE(store.reportLoad("echo", host, 99));
    auto details = store.methodHostDetails("echo");
    ASSERT_EQ(details.size(), 1u);
    EXPECT_EQ(details[0].load, 99);
}

// 上报未知 method/host 的负载返回 false
TEST(MemoryRegistryStoreTest, ReportLoadUnknownReturnsFalse)
{
    MemoryRegistryStore store;
    EXPECT_FALSE(store.reportLoad("echo", HostInfo("10.0.0.1", 8080), 5));
}

// 心跳：已知 provider 返回 true，未知返回 false
TEST(MemoryRegistryStoreTest, HeartbeatKnownReturnsTrue)
{
    MemoryRegistryStore store;
    auto conn = std::make_shared<MockConnection>();
    HostInfo host("10.0.0.1", 8080);
    store.registerInstance(conn, host, "echo", 5);

    EXPECT_TRUE(store.heartbeat("echo", host));
    EXPECT_FALSE(store.heartbeat("echo", HostInfo("10.0.0.2", 8080)));
}

// 大超时下刚注册的 provider 不过期
TEST(MemoryRegistryStoreTest, SweepNotExpiredWithLargeTimeout)
{
    MemoryRegistryStore store;
    auto conn = std::make_shared<MockConnection>();
    store.registerInstance(conn, HostInfo("10.0.0.1", 8080), "echo", 5);

    auto expired = store.sweepExpired(std::chrono::seconds(3600));
    EXPECT_TRUE(expired.empty());
}

// 零超时下 provider 立即被判定过期
TEST(MemoryRegistryStoreTest, SweepExpiredWithZeroTimeout)
{
    MemoryRegistryStore store;
    auto conn = std::make_shared<MockConnection>();
    HostInfo host("10.0.0.1", 8080);
    store.registerInstance(conn, host, "echo", 5);

    std::this_thread::sleep_for(std::chrono::milliseconds(5)); // 确保 lastheartbeat 已过

    auto expired = store.sweepExpired(std::chrono::seconds(0));
    ASSERT_EQ(expired.size(), 1u);
    EXPECT_EQ(expired[0].first, "echo");
    EXPECT_EQ(expired[0].second, host);
}

// 连接断开返回下线列表并移除 provider
TEST(MemoryRegistryStoreTest, DisconnectProviderReturnsAndRemoves)
{
    MemoryRegistryStore store;
    auto conn = std::make_shared<MockConnection>();
    HostInfo host("10.0.0.1", 8080);
    store.registerInstance(conn, host, "echo", 5);

    auto offline = store.disconnectProvider(conn);
    ASSERT_EQ(offline.size(), 1u);
    EXPECT_EQ(offline[0].first, "echo");
    EXPECT_EQ(offline[0].second, host);
    EXPECT_TRUE(store.methodHost("echo").empty());
}

// cleanConnKeys 仅清理映射，不返回下线列表
TEST(MemoryRegistryStoreTest, CleanConnKeysRemoves)
{
    MemoryRegistryStore store;
    auto conn = std::make_shared<MockConnection>();
    store.registerInstance(conn, HostInfo("10.0.0.1", 8080), "echo", 5);

    store.cleanConnKeys(conn);
    EXPECT_TRUE(store.methodHost("echo").empty());
}

// 同一方法可注册多个主机
TEST(MemoryRegistryStoreTest, MultipleHostsForSameMethod)
{
    MemoryRegistryStore store;
    auto c1 = std::make_shared<MockConnection>();
    auto c2 = std::make_shared<MockConnection>();
    HostInfo h1("10.0.0.1", 8080);
    HostInfo h2("10.0.0.2", 8080);
    store.registerInstance(c1, h1, "echo", 1);
    store.registerInstance(c2, h2, "echo", 2);

    auto hosts = store.methodHost("echo");
    ASSERT_EQ(hosts.size(), 2u);
    // set 内部按 Provider::ptr 排序，顺序不保证，只验证包含
    EXPECT_NE(std::find(hosts.begin(), hosts.end(), h1), hosts.end());
    EXPECT_NE(std::find(hosts.begin(), hosts.end(), h2), hosts.end());
}
