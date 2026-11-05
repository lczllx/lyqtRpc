// backoffDelayMs 指数退避 + 全抖动单测：边界 / 指数翻倍 / 封顶 / 抖动多样性
#include <gtest/gtest.h>
#include <set>
#include <algorithm>
#include "src/client/rpc_client.hpp"

using lcz_rpc::client::backoffDelayMs;
using lcz_rpc::client::pickUntriedCandidate;
using lcz_rpc::client::HostInfoHash;
using lcz_rpc::HostDetail;
using lcz_rpc::HostInfo;
using lcz_rpc::RetryConfig;

// 每次退避都落在 [0, min(max_ms, base_ms * 2^attempt)] 区间内
TEST(BackoffTest, BoundsPerAttempt)
{
    RetryConfig cfg;
    cfg.base_ms = 10;
    cfg.max_ms = 1000;
    for (int attempt = 0; attempt <= 8; ++attempt)
    {
        long long exp = std::min<long long>(10LL * (1LL << attempt), 1000LL);
        for (int i = 0; i < 200; ++i)
        {
            auto ms = backoffDelayMs(cfg, attempt).count();
            EXPECT_GE(ms, 0);
            EXPECT_LE(ms, exp);
        }
    }
}

// 大 attempt 被 max_ms 封顶，不会无限增长
TEST(BackoffTest, CapsAtMax)
{
    RetryConfig cfg;
    cfg.base_ms = 10;
    cfg.max_ms = 1000;
    long max_seen = 0; // std::chrono::milliseconds 的 rep 是 long
    for (int i = 0; i < 5000; ++i)
        max_seen = std::max(max_seen, backoffDelayMs(cfg, 30).count());
    EXPECT_LE(max_seen, 1000);
    EXPECT_GE(max_seen, 700); // uniform[0,1000] 5000 次采样几乎必然出现 >700
}

// 全抖动在区间内产生多个不同取值（非固定常数）
TEST(BackoffTest, FullJitterProducesVariety)
{
    RetryConfig cfg;
    cfg.base_ms = 10;
    cfg.max_ms = 1000;
    std::set<long long> seen;
    for (int i = 0; i < 1000; ++i)
        seen.insert(backoffDelayMs(cfg, 3).count()); // 区间 [0,80]
    EXPECT_GT(seen.size(), 1u);
}

namespace
{
    std::vector<HostDetail> threeHosts()
    {
        return {HostDetail{HostInfo("10.0.0.1", 8080), 0},
                HostDetail{HostInfo("10.0.0.2", 8080), 0},
                HostDetail{HostInfo("10.0.0.3", 8080), 0}};
    }
}

// 重试游标：依次跳过已试过的 host，不会反复打同一台
TEST(RetryCandidateTest, WalksEveryCandidateOnce)
{
    auto candidates = threeHosts();
    std::unordered_set<HostInfo, HostInfoHash> tried;
    for (const auto &expected : candidates)
    {
        const HostDetail *next = pickUntriedCandidate(candidates, tried);
        ASSERT_NE(next, nullptr);
        EXPECT_EQ(next->host, expected.host);
        tried.insert(next->host);
    }
}

// 全部候选都试过：重置后从头开始，单实例集群的重试次数不被候选数截断
TEST(RetryCandidateTest, ResetsAfterAllTried)
{
    auto candidates = threeHosts();
    std::unordered_set<HostInfo, HostInfoHash> tried;
    for (const auto &c : candidates)
        tried.insert(c.host);

    const HostDetail *next = pickUntriedCandidate(candidates, tried);
    ASSERT_NE(next, nullptr);
    EXPECT_EQ(next->host, candidates.front().host);
    EXPECT_TRUE(tried.empty()); // 游标已重置，下一轮重新计
}

// 单候选：每次重试都拿到同一台（但调用方仍会按 max_retries 重试）
TEST(RetryCandidateTest, SingleCandidateRepeats)
{
    std::vector<HostDetail> candidates{HostDetail{HostInfo("10.0.0.1", 8080), 0}};
    std::unordered_set<HostInfo, HostInfoHash> tried;
    for (int i = 0; i < 3; ++i)
    {
        const HostDetail *next = pickUntriedCandidate(candidates, tried);
        ASSERT_NE(next, nullptr);
        EXPECT_EQ(next->host, candidates.front().host);
        tried.insert(next->host);
    }
}

// 空候选：返回 nullptr，不崩溃
TEST(RetryCandidateTest, EmptyCandidatesReturnNull)
{
    std::vector<HostDetail> candidates;
    std::unordered_set<HostInfo, HostInfoHash> tried;
    EXPECT_EQ(pickUntriedCandidate(candidates, tried), nullptr);
}
