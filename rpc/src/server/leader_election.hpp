#pragma once
#include <memory>
#include <functional>

namespace muduo
{
    namespace net
    {
        class EventLoop;
    }
}

namespace lcz_rpc
{
    namespace server
    {

        // Leader 选举抽象接口
        // 多实例部署时，通过 etcd lease + CAS 事务保证同一时刻只有一个 leader
        // leader 负责 sweep 过期 provider 并通知本地 discoverer
        // follower 不执行 sweep，其 discoverer 依赖客户端健康检查（每 10s）兜底
        class ILeaderElector
        {
        public:
            using ptr = std::shared_ptr<ILeaderElector>;
            using LeadershipCallback = std::function<void(bool is_leader)>;

            virtual bool isLeader() const = 0;                                                    // 当前是否持有 leader 身份
            virtual void start(muduo::net::EventLoop *loop, LeadershipCallback cb = nullptr) = 0; // 启动选举定时器
            virtual void stop() = 0;                                                              // 停止选举、释放 lease
            virtual ~ILeaderElector() = default;
        };

    } // namespace server
} // namespace lcz_rpc
