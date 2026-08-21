// MemoryLeaderElector 单元测试：内存模式单实例恒为 leader，start/stop 无副作用
#include <gtest/gtest.h>
#include <memory>
#include "src/server/memory_leader_election.hpp"

using lcz_rpc::server::MemoryLeaderElector;

// 单实例部署：isLeader 恒真，且不依赖 muduo EventLoop
TEST(MemoryLeaderElectorTest, AlwaysLeader)
{
    MemoryLeaderElector elector;
    EXPECT_TRUE(elector.isLeader());
    EXPECT_TRUE(elector.isLeader()); // 重复查询稳定
}

// start/stop 为无操作：传 nullptr 与回调均不崩溃、不影响 leader 状态
TEST(MemoryLeaderElectorTest, StartStopAreNoop)
{
    MemoryLeaderElector elector;
    bool cb_called = false;

    elector.start(nullptr, [&cb_called](bool is_leader) { cb_called = true; });
    EXPECT_FALSE(cb_called); // 内存模式不触发回调
    EXPECT_TRUE(elector.isLeader());

    elector.stop(); // 无操作
    EXPECT_TRUE(elector.isLeader());
}
