#pragma once
#include "leader_election.hpp"

namespace lcz_rpc
{
    namespace server
    {

        // 内存模式：单实例部署，永远为 leader，无选举逻辑
        // 行为不变 —— sweep 定时器始终运行，所有功能等价于未引入选举模块之前
        class MemoryLeaderElector : public ILeaderElector
        {
        public:
            using ptr = std::shared_ptr<MemoryLeaderElector>;

            bool isLeader() const override { return true; }                     // 始终返回 true
            void start(muduo::net::EventLoop *, LeadershipCallback) override {} // 无操作
            void stop() override {}                                             // 无操作
        };

    } // namespace server
} // namespace lcz_rpc
