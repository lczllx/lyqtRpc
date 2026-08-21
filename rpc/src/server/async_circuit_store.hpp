#pragma once
#include <string>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include "circuit_store.hpp"

namespace lcz_rpc
{
    namespace server
    {
        // 异步熔断器状态存储
        // save 写本地缓存后立即返回，由后台线程批量刷入底层 store，不阻塞 RPC 调用路径
        // load 优先查本地缓存，穿透到底层 store 时回填，后续同 key 读取零开销
        // 同 key 多次 save 在待刷队列中自动去重，只保留最后一次状态
        class AsyncCircuitStore : public ICircuitStateStore
        {
        public:
            using ptr = std::shared_ptr<AsyncCircuitStore>;

            // 构造注入底层 store + 刷盘间隔
            explicit AsyncCircuitStore(ICircuitStateStore::ptr underlying,
                                       std::chrono::milliseconds flush_interval = std::chrono::milliseconds(100));
            ~AsyncCircuitStore() override;

            AsyncCircuitStore(const AsyncCircuitStore &) = delete;
            AsyncCircuitStore &operator=(const AsyncCircuitStore &) = delete;

            // 状态变更时保存：先写本地缓存（调用线程，O(1)），异步入队等后台刷盘
            bool save(const std::string &method,
                      const std::string &host, const CircuitStatus &status) override;

            // 启动时加载：本地缓存命中直接返回，穿透到底层 store 后回填
            CircuitStatus load(const std::string &method,
                               const std::string &host) override;

            // provider 下线清理：同步删底层 + 清本地缓存 + 取消待刷队列中对应 key
            bool remove(const std::string &method,
                        const std::string &host) override;

        private:
            static std::string cacheKey(const std::string &method, const std::string &host);
            void workerLoop();

            ICircuitStateStore::ptr _underlying; // 底层持久化 store（EtcdCircuitStore 或 MemoryCircuitStore）

            std::mutex _cache_mutex;                               // 保护 _cache
            std::unordered_map<std::string, CircuitStatus> _cache; // 本地缓存，method\0host → 状态

            std::mutex _pending_mutex;                               // 保护 _pending 及 _cv
            std::condition_variable _cv;                             // 后台线程等待 / 唤醒
            std::unordered_map<std::string, CircuitStatus> _pending; // 待刷队列，同 key 覆盖即去重

            std::thread _worker;                       // 后台刷盘线程
            std::atomic<bool> _running{true};          // 控制 worker 退出
            std::chrono::milliseconds _flush_interval; // 刷盘间隔
        };

    } // namespace server
} // namespace lcz_rpc
