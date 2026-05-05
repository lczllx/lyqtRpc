#pragma once
#include <string>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "../server/circuit_store.hpp"
#include "../general/publicconfig.hpp"
#include "node_breaker.hpp"
namespace lcz_rpc
{
    namespace client
    {
        // 全局熔断器管理——管理所有 method×host 粒度的 NodeBreaker，负责持久化
        class CircuitBreaker
        {
        public:
            using ptr = std::shared_ptr<CircuitBreaker>;

            // 构造注入：配置 + 状态存储后端
            CircuitBreaker(const CircuitConfig &cfg, lcz_rpc::server::ICircuitStateStore::ptr store)
                : _cfg(cfg), _store(std::move(store)) {}

            // 检查是否允许本次请求通过
            bool allowRequest(const std::string &method, const std::string &host);

            // 调用成功——转发给对应 NodeBreaker，状态转换时持久化
            void onSuccess(const std::string &method, const std::string &host);

            // 调用失败——转发给对应 NodeBreaker，状态转换时持久化
            void onFailure(const std::string &method, const std::string &host);

            // 获取指定 method×host 的熔断器状态
            CircuitStatus status(const std::string &method, const std::string &host);

            // provider 下线时移除对应熔断器
            void removeNode(const std::string &method, const std::string &host);
            // provider 下线时按 host 移除所有 method 的熔断器（用于仅知 host 不知 method 的场景）
            void removeHost(const std::string &host);

        private:
            NodeBreaker::ptr getOrCreate(const std::string &method, const std::string &host);

        private:
            std::mutex _mutex;
            CircuitConfig _cfg;
            lcz_rpc::server::ICircuitStateStore::ptr _store;
            // method → host → NodeBreaker，每个 provider 的每个方法独立一个熔断器
            std::unordered_map<std::string, std::unordered_map<std::string, NodeBreaker::ptr>> _nodes;
        };

    }
}
