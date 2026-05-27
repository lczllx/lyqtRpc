#include <string>
#include <memory>
#include <ctime>
#include "circuit_breaker.hpp"
#include "../general/log_system/lcz_log.h"

namespace lcz_rpc
{
    namespace client
    {
        // 查找或创建 method×host 对应的 NodeBreaker
        // 并发安全：锁外创建 + 二次检查（double-check），避免持锁期间做 I/O
        // 首次创建时从 store 恢复已持久化状态，OPEN 过期则转为 HALF_OPEN 允许探测
        NodeBreaker::ptr CircuitBreaker::getOrCreate(const std::string &method, const std::string &host)
        {
            {
                std::lock_guard<std::mutex> lock(_mutex);
                auto method_it = _nodes.find(method);
                if (method_it != _nodes.end())
                {
                    auto host_it = method_it->second.find(host);
                    if (host_it != method_it->second.end())
                        return host_it->second;
                }
            }

            // 没有找到，新建 NodeBreaker 并从 store 恢复状态
            auto node = std::make_shared<NodeBreaker>(_cfg);
            CircuitStatus saved = _store->load(method, host);
            if (saved.state == CircuitState::OPEN)
            {
                int64_t now = static_cast<int64_t>(std::time(nullptr));
                if (now - saved.opened_at >= _cfg.open_duration_sec)
                {
                    // 冷却期已过，转为半开允许探测
                    saved.state = CircuitState::HALF_OPEN;
                    saved.failures = 0;
                    saved.half_open = 0;
                    LCZ_INFO("[CircuitBreaker] 加载持久化状态 method=%s host=%s: OPEN 已过期 => HALF_OPEN",
                             method.c_str(), host.c_str());
                }
                else
                {
                    LCZ_INFO("[CircuitBreaker] 加载持久化状态 method=%s host=%s: OPEN 未过期，继续熔断",
                             method.c_str(), host.c_str());
                }
            }
            node->loadStatus(saved);

            {
                std::lock_guard<std::mutex> lock(_mutex);
                // 二次检查：并发下可能另一个线程已创建
                auto &inner = _nodes[method];
                auto it = inner.find(host);
                if (it != inner.end())
                    return it->second;
                inner[host] = node;
            }
            return node;
        }

        // 检查是否允许本次请求通过
        bool CircuitBreaker::allowRequest(const std::string &method, const std::string &host)
        {
            auto node = getOrCreate(method, host);
            return node->allowRequest();
        }

        // 调用成功——仅状态转换时持久化
        void CircuitBreaker::onSuccess(const std::string &method, const std::string &host)
        {
            auto node = getOrCreate(method, host);
            bool changed = node->onSuccess();
            if (changed)
            {
                _store->save(method, host, node->status());
                LCZ_INFO("[CircuitBreaker] 熔断器恢复 method=%s host=%s state=CLOSED",
                         method.c_str(), host.c_str());
            }
        }

        // 调用失败——仅状态转换时持久化
        void CircuitBreaker::onFailure(const std::string &method, const std::string &host)
        {
            auto node = getOrCreate(method, host);
            bool changed = node->onFailure();
            if (changed)
            {
                _store->save(method, host, node->status());
                LCZ_INFO("[CircuitBreaker] 熔断器打开 method=%s host=%s state=OPEN failures=%d",
                         method.c_str(), host.c_str(), node->status().failures);
            }
        }

        // 获取指定 method×host 的熔断器状态
        CircuitStatus CircuitBreaker::status(const std::string &method, const std::string &host)
        {
            {
                std::lock_guard<std::mutex> lock(_mutex);
                auto method_it = _nodes.find(method);
                if (method_it != _nodes.end())
                {
                    auto host_it = method_it->second.find(host);
                    if (host_it != method_it->second.end())
                        return host_it->second->status();
                }
            }
            // 没有记录说明从未调用过，默认 CLOSED
            return CircuitStatus{};
        }

        // provider 下线时移除对应熔断器
        void CircuitBreaker::removeNode(const std::string &method, const std::string &host)
        {
            _store->remove(method, host);
            std::lock_guard<std::mutex> lock(_mutex);
            auto method_it = _nodes.find(method);
            if (method_it == _nodes.end())
                return;
            method_it->second.erase(host);
            if (method_it->second.empty())
                _nodes.erase(method_it);
            LCZ_INFO("[CircuitBreaker] 移除熔断器 method=%s host=%s", method.c_str(), host.c_str());
        }

        // provider 下线时按 host 清理所有 method（用于仅知 host 不知 method 的 OFFLINE 回调）
        void CircuitBreaker::removeHost(const std::string &host)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            for (auto mit = _nodes.begin(); mit != _nodes.end(); )
            {
                mit->second.erase(host);
                _store->remove(mit->first, host);
                if (mit->second.empty())
                    mit = _nodes.erase(mit);
                else
                    ++mit;
            }
            LCZ_INFO("[CircuitBreaker] 按 host 清理熔断器 host=%s", host.c_str());
        }
    }
}
