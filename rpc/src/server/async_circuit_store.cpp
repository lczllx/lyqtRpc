#include "async_circuit_store.hpp"
#include "../general/log_system/lcz_log.h"

namespace lcz_rpc
{
    namespace server
    {

        AsyncCircuitStore::AsyncCircuitStore(ICircuitStateStore::ptr underlying,
                                             std::chrono::milliseconds flush_interval)
            : _underlying(std::move(underlying)), _flush_interval(flush_interval)
        {
            // 启动后台刷盘线程
            _worker = std::thread(&AsyncCircuitStore::workerLoop, this);
            LCZ_INFO("[AsyncCircuitStore] 后台写入线程启动, flush_interval=%lldms",
                     static_cast<long long>(flush_interval.count()));
        }

        AsyncCircuitStore::~AsyncCircuitStore()
        {
            // 通知后台线程退出
            _running = false;
            _cv.notify_all();
            if (_worker.joinable())
                _worker.join();

            // 兜底：把残留的待刷数据全部落盘，防止进程退出丢状态
            std::unordered_map<std::string, CircuitStatus> remaining;
            {
                std::lock_guard<std::mutex> lock(_pending_mutex);
                remaining.swap(_pending);
            }
            for (const auto &kv : remaining)
            {
                const std::string &key = kv.first;
                auto null_pos = key.find('\0');
                if (null_pos != std::string::npos)
                {
                    std::string method = key.substr(0, null_pos);
                    std::string host = key.substr(null_pos + 1);
                    _underlying->save(method, host, kv.second);
                }
            }
            LCZ_INFO("[AsyncCircuitStore] 后台写入线程退出, 最终 flush %zu 条", remaining.size());
        }

        // key 构造：method\0host，\0 分隔避免 "a"+"bc" 和 "ab"+"c" 冲突
        std::string AsyncCircuitStore::cacheKey(const std::string &method, const std::string &host)
        {
            return method + '\0' + host;
        }

        // 状态变更时保存
        bool AsyncCircuitStore::save(const std::string &method,
                                     const std::string &host, const CircuitStatus &status)
        {
            std::string key = cacheKey(method, host);

            // 先写本地缓存，后续 load 立即可见最新状态
            {
                std::lock_guard<std::mutex> lock(_cache_mutex);
                _cache[key] = status;
            }

            // 入待刷队列，同 key 覆盖旧值，自然去重
            {
                std::lock_guard<std::mutex> lock(_pending_mutex);
                _pending[key] = status;
            }
            _cv.notify_one();
            return true;
        }

        // 启动时加载
        CircuitStatus AsyncCircuitStore::load(const std::string &method,
                                              const std::string &host)
        {
            std::string key = cacheKey(method, host);

            // 先查本地缓存
            {
                std::lock_guard<std::mutex> lock(_cache_mutex);
                auto it = _cache.find(key);
                if (it != _cache.end())
                    return it->second;
            }

            // 缓存不命中，穿透读底层 store
            CircuitStatus status = _underlying->load(method, host);

            // 回填缓存，后续同 key 读取走缓存
            {
                std::lock_guard<std::mutex> lock(_cache_mutex);
                _cache[key] = status;
            }
            return status;
        }

        // provider 下线清理
        bool AsyncCircuitStore::remove(const std::string &method,
                                       const std::string &host)
        {
            std::string key = cacheKey(method, host);

            // 清本地缓存
            {
                std::lock_guard<std::mutex> lock(_cache_mutex);
                _cache.erase(key);
            }

            // 取消待刷队列中该 key
            {
                std::lock_guard<std::mutex> lock(_pending_mutex);
                _pending.erase(key);
            }

            // 同步删底层 store
            return _underlying->remove(method, host);
        }

        void AsyncCircuitStore::workerLoop()
        {
            while (_running)
            {
                std::unordered_map<std::string, CircuitStatus> batch;
                {
                    std::unique_lock<std::mutex> lock(_pending_mutex);
                    // 等待 _flush_interval 或立即被 save 唤醒
                    _cv.wait_for(lock, _flush_interval, [this]
                                 { return !_running || !_pending.empty(); });

                    if (!_running && _pending.empty())
                        break;

                    // swap 出待刷数据，缩临界区
                    batch.swap(_pending);
                }

                if (batch.empty())
                    continue;

                // 逐条刷入底层 store
                for (const auto &kv : batch)
                {
                    const std::string &key = kv.first;
                    auto null_pos = key.find('\0');
                    if (null_pos == std::string::npos)
                        continue;

                    std::string method = key.substr(0, null_pos);
                    std::string host = key.substr(null_pos + 1);
                    if (!_underlying->save(method, host, kv.second))
                    {
                        LCZ_WARN("[AsyncCircuitStore] 后台写入失败 method=%s host=%s state=%d",
                                 method.c_str(), host.c_str(), static_cast<int>(kv.second.state));
                    }
                }
            }
        }

    } // namespace server
} // namespace lcz_rpc
