// MethodHost 一致性哈希负载均衡单元测试：确定性 / 分布 / 增删节点最小重映射
#include <gtest/gtest.h>
#include <algorithm>
#include <limits>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "src/client/rpc_registry.hpp"

using lcz_rpc::client::MethodHost;
using lcz_rpc::HostInfo;
using lcz_rpc::LoadBalanceStrategy;

namespace
{
    // 向 mh 填充 n 台主机（10.0.0.1:8080 ~ 10.0.0.n:8080）
    // 注意：MethodHost 含 std::mutex，不可拷贝/移动，故用引用填充而非按值返回
    void fillHosts(MethodHost &mh, int n)
    {
        for (int i = 1; i <= n; ++i)
        {
            mh.appendHost(HostInfo("10.0.0." + std::to_string(i), 8080), 0);
        }
    }
}

// 同一 key 多次命中同一主机（一致性哈希的核心语义）
TEST(ConsistentHashTest, SameKeySameHost)
{
    MethodHost mh;
    fillHosts(mh, 5);
    auto a = mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, "user-42");
    auto b = mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, "user-42");
    EXPECT_EQ(a.host, b.host);
}

// 大量不同 key 应分散到多台主机（不是单点命中）
TEST(ConsistentHashTest, DifferentKeysSpread)
{
    MethodHost mh;
    fillHosts(mh, 5);
    std::set<HostInfo> seen;
    for (int i = 0; i < 1000; ++i)
    {
        seen.insert(mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, "k" + std::to_string(i)).host);
    }
    EXPECT_GT(seen.size(), 1u);
}

// 加 1 台节点后，仅少数 key 重映射（一致性哈希 vs 取模哈希的核心差异）
TEST(ConsistentHashTest, MinimalRemapOnAdd)
{
    MethodHost mh;
    fillHosts(mh, 4);
    std::vector<HostInfo> before;
    std::vector<std::string> keys;
    for (int i = 0; i < 1000; ++i)
    {
        std::string k = "k" + std::to_string(i);
        keys.push_back(k);
        before.push_back(mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, k).host);
    }

    mh.appendHost(HostInfo("10.0.0.5", 8080), 0); // 新增第 5 台

    int remapped = 0;
    for (int i = 0; i < 1000; ++i)
    {
        if (mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, keys[i]).host != before[i])
        {
            ++remapped;
        }
    }
    // 理论重映射比例 ≈ 1/(N+1) = 20%，远低于取模哈希的全量；放宽断言到 < 50%
    EXPECT_LT(remapped, 500);
}

// 移除主机后，仍能正常命中剩余主机，且同 key 保持稳定
TEST(ConsistentHashTest, MinimalRemapOnRemove)
{
    MethodHost mh;
    fillHosts(mh, 4);
    std::vector<HostInfo> before;
    std::vector<std::string> keys;
    for (int i = 0; i < 1000; ++i)
    {
        std::string k = "k" + std::to_string(i);
        keys.push_back(k);
        before.push_back(mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, k).host);
    }

    mh.removeHost(HostInfo("10.0.0.1", 8080)); // 移除 1 台

    int remapped = 0;
    for (int i = 0; i < 1000; ++i)
    {
        auto now = mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, keys[i]).host;
        EXPECT_NE(now, HostInfo("10.0.0.1", 8080)); // 不会命中已移除的主机
        if (now != before[i])
        {
            ++remapped;
        }
    }
    EXPECT_LT(remapped, 600); // 移除节点重映射比例 ≈ 1/N = 25%，放宽到 < 60%
}

// 空 key 退化：走随机/兜底路径不崩溃，且返回合法主机
TEST(ConsistentHashTest, EmptyKeyFallsBack)
{
    MethodHost mh;
    fillHosts(mh, 3);
    auto d = mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, "");
    EXPECT_NE(d.host, HostInfo());
}

// 分布均衡性：160 虚拟节点应让大量 key 相对均匀地落到各主机，max:min 不应过于悬殊
TEST(ConsistentHashTest, DistributionBalanced)
{
    MethodHost mh;
    fillHosts(mh, 10);
    std::unordered_map<std::string, int> cnt;
    for (int i = 0; i < 50000; ++i)
    {
        cnt[mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, "k" + std::to_string(i)).host.first]++;
    }
    int mn = std::numeric_limits<int>::max(), mx = 0;
    for (auto &kv : cnt)
    {
        mn = std::min(mn, kv.second);
        mx = std::max(mx, kv.second);
    }
    EXPECT_EQ(cnt.size(), 10u);       // 10 台主机都应被命中
    EXPECT_LT(mx / (double)mn, 2.5);  // 分布失衡不超过 2.5 倍（160 虚拟节点下应远小于此）
}

// 空主机列表：selectHost 返回空 HostDetail，不崩溃
TEST(ConsistentHashTest, EmptyHostListReturnsEmpty)
{
    MethodHost mh;
    auto d = mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, "k");
    EXPECT_EQ(d.host, HostInfo());
}

// 移除不存在的主机：不崩溃，且不影响已有 key 的命中（ring 不变）
TEST(ConsistentHashTest, RemoveAbsentHostNoop)
{
    MethodHost mh;
    fillHosts(mh, 3);
    auto before = mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, "user-42").host;
    mh.removeHost(HostInfo("10.0.0.99", 8080)); // 不存在的主机
    auto after = mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, "user-42").host;
    EXPECT_EQ(before, after);
}

// 一致性哈希加节点重映射远小于取模哈希（SOURCE_HASH）——核心优势的量化对比
TEST(ConsistentHashTest, ConsistentHashRemapsLessThanModulo)
{
    const int KEYS = 1000;
    MethodHost ch, sh;
    fillHosts(ch, 4);
    fillHosts(sh, 4);

    std::vector<HostInfo> ch_before, sh_before;
    std::vector<std::string> keys;
    for (int i = 0; i < KEYS; ++i)
    {
        std::string k = "k" + std::to_string(i);
        keys.push_back(k);
        ch_before.push_back(ch.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, k).host);
        sh_before.push_back(sh.selectHost(LoadBalanceStrategy::SOURCE_HASH, k).host);
    }

    ch.appendHost(HostInfo("10.0.0.5", 8080), 0);
    sh.appendHost(HostInfo("10.0.0.5", 8080), 0);

    int ch_remap = 0, sh_remap = 0;
    for (int i = 0; i < KEYS; ++i)
    {
        if (ch.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, keys[i]).host != ch_before[i]) ++ch_remap;
        if (sh.selectHost(LoadBalanceStrategy::SOURCE_HASH, keys[i]).host != sh_before[i]) ++sh_remap;
    }
    // 取模哈希加节点后约 (N-1)/(N+1)=80% 重映射，一致性哈希约 1/(N+1)=20%
    EXPECT_LT(ch_remap * 2, sh_remap);
}
