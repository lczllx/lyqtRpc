// backoffDelayMs 指数退避 + 全抖动单测：边界 / 指数翻倍 / 封顶 / 抖动多样性
#include <gtest/gtest.h>
#include <set>
#include <algorithm>
#include "src/client/rpc_client.hpp"

using lcz_rpc::client::backoffDelayMs;
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
