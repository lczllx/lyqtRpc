// =============================================================================
// test_selection_profile.cc — 选主机在位采样器
// -----------------------------------------------------------------------------
// 只测采样器本身：开关、线程缓冲合并、分位数口径。
// 「getCandidates 每次请求都记录」由 example/benchmark/run_selection_attribution.sh
// --check 覆盖（采样点被删掉时采样次数为 0，脚本会失败）。
// =============================================================================

#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <vector>

#include "src/client/selection_profile.hpp"

using lcz_rpc::profile::SelectionScope;

namespace
{
    class SelectionProfileTest : public ::testing::Test
    {
    protected:
        void SetUp() override
        {
            lcz_rpc::profile::setSelectionProfileEnabled(false);
            lcz_rpc::profile::resetSelectionProfile();
        }
        void TearDown() override
        {
            lcz_rpc::profile::setSelectionProfileEnabled(false);
            lcz_rpc::profile::resetSelectionProfile();
        }
    };
}

TEST_F(SelectionProfileTest, DisabledRecordsNothing)
{
    EXPECT_FALSE(lcz_rpc::profile::selectionProfileEnabled());
    lcz_rpc::profile::recordSelectionNs(100);
    {
        SelectionScope scope; // 关闭状态下也应无副作用
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(lcz_rpc::profile::takeSelectionSamples().empty());
}

TEST_F(SelectionProfileTest, PercentilesUseNearestRank)
{
    std::vector<uint64_t> samples;
    for (uint64_t v = 1; v <= 100; ++v)
        samples.push_back(v);

    const auto s = lcz_rpc::profile::summarizeSelectionSamples(samples);
    EXPECT_EQ(s.count, 100u);
    EXPECT_EQ(s.min_ns, 1u);
    EXPECT_EQ(s.max_ns, 100u);
    EXPECT_EQ(s.sum_ns, 5050u);
    EXPECT_DOUBLE_EQ(s.mean_ns, 50.5);
    // nearest-rank: rank = ceil(p*n)，取第 rank 个（1-based）
    EXPECT_DOUBLE_EQ(s.p50_ns, 50.0);
    EXPECT_DOUBLE_EQ(s.p90_ns, 90.0);
    EXPECT_DOUBLE_EQ(s.p99_ns, 99.0);

    const auto empty = lcz_rpc::profile::summarizeSelectionSamples({});
    EXPECT_EQ(empty.count, 0u);
    EXPECT_DOUBLE_EQ(empty.mean_ns, 0.0);
    EXPECT_DOUBLE_EQ(empty.p99_ns, 0.0);
}

TEST_F(SelectionProfileTest, ThreadBuffersAreMergedAfterJoin)
{
    lcz_rpc::profile::setSelectionProfileEnabled(true);

    auto record_range = [](uint64_t base) {
        for (uint64_t i = 0; i < 1000; ++i)
            lcz_rpc::profile::recordSelectionNs(base + i);
    };
    std::thread a(record_range, 0);
    std::thread b(record_range, 1000);
    a.join();
    b.join();

    const auto samples = lcz_rpc::profile::takeSelectionSamples();
    ASSERT_EQ(samples.size(), 2000u);
    EXPECT_EQ(samples.front(), 0u);
    EXPECT_EQ(samples.back(), 1999u);
    for (size_t i = 1; i < samples.size(); ++i)
        EXPECT_LE(samples[i - 1], samples[i]);
}

TEST_F(SelectionProfileTest, ScopeRecordsElapsedOnlyWhenEnabled)
{
    lcz_rpc::profile::setSelectionProfileEnabled(true);
    {
        SelectionScope scope;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto enabled_samples = lcz_rpc::profile::takeSelectionSamples();
    ASSERT_EQ(enabled_samples.size(), 1u);
    // 下界放宽到 100us，避免机器负载高时 sleep 被提前唤醒导致抖动
    EXPECT_GE(enabled_samples[0], 100000u);

    lcz_rpc::profile::resetSelectionProfile();
    lcz_rpc::profile::setSelectionProfileEnabled(false);
    {
        SelectionScope scope;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(lcz_rpc::profile::takeSelectionSamples().empty());
}
