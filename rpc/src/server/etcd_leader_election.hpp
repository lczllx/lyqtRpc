#pragma once
#include "leader_election.hpp"
#include "curl/curl.h"
#include <muduo/net/TimerId.h>
#include <string>
#include <mutex>
#include <memory>

namespace lcz_rpc
{
    namespace server
    {

        // etcd lease 选举：通过 CAS 事务原子创建 /lcz-rpc/v1/leader key
        //
        // 选举流程：
        //   1. 每个实例每 1s 调用 electionTick()
        //   2. 非 leader 调用 tryBecomeLeader()：grant lease（5s TTL）→ CAS create key → 成功则成为 leader
        //   3. Leader 调用 refreshLease()：keepalive 续约，失败则退为 follower
        //   4. CAS 失败说明已有其他实例持有 leader，本实例保持 follower
        //
        // 故障转移：
        //   Leader 崩溃后 lease 5s 过期，etcd 自动删除 key
        //   下一个实例的 CAS（version==0）将成功，接管 leader
        //
        // 注意：本类拥有独立的 CURL* 句柄，不与 EtcdRegistryStore 共享，避免锁争用
        class EtcdLeaderElector : public ILeaderElector
        {
        public:
            using ptr = std::shared_ptr<EtcdLeaderElector>;

            explicit EtcdLeaderElector(const std::string &etcd_endpoints);
            ~EtcdLeaderElector() override;

            EtcdLeaderElector(const EtcdLeaderElector &) = delete;
            EtcdLeaderElector &operator=(const EtcdLeaderElector &) = delete;

            bool isLeader() const override;
            void start(muduo::net::EventLoop *loop, LeadershipCallback cb = nullptr) override; // 注册 1s 定时器，启动选举
            void stop() override;                                                              // 撤销 lease、清理状态

        private:
            void electionTick();    // 每 1s 回调：leader 续约 / follower 尝试竞选
            void tryBecomeLeader(); // grant lease（5s TTL）→ CAS 原子创建 /lcz-rpc/v1/leader
            void refreshLease();    // keepalive 续约一次，失败则退为 follower
            void becomeFollower();  // 撤销 lease、触发 LeadershipCallback(false)

            std::string curl_post(const std::string &path, const std::string &json_body); // 独立的 HTTP POST
            static std::string base64_encode(const std::string &s);                       // 复用 EtcdRegistryStore 的编码逻辑

            std::string _etcd_pos;    // etcd v3 HTTP API 地址，如 "http://127.0.0.1:2379"
            std::string _instance_id; // "hostname:pid"，写入 leader key 作为 value，便于排查谁在持有

            CURL *_curl;                 // 独立 CURL 句柄（不与 EtcdRegistryStore 共享）
            struct curl_slist *_headers; // HTTP 请求头
            std::mutex _curl_mutex;      // 保护 CURL*（libcurl 要求同一句柄禁止并发）

            muduo::net::EventLoop *_loop;  // 选举定时器所在事件循环
            muduo::net::TimerId _timer_id; // 选举定时器 ID
            LeadershipCallback _callback;  // leader/follower 身份变更时回调

            bool _is_leader;                                 // 当前是否持有 leader
            int64_t _lease_id;                               // etcd lease ID，-1 表示未持有
            static constexpr int LEASE_TTL_SEC = 5;          // lease 5s 过期，崩溃后快速故障转移
            static constexpr double TICK_INTERVAL_SEC = 1.0; // 每 1s 续约/重试，TTL 的 1/5
        };

    } // namespace server
} // namespace lcz_rpc
