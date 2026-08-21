#pragma once
#include <string>
#include <memory>
#include "circuit_store.hpp"
#include <mutex>
#include <unordered_map>
namespace lcz_rpc
{
    namespace server
    {
        //熔断器内存存储
        class MemoryCircuitStore : public ICircuitStateStore
        {
        public:
            using ptr = std::shared_ptr<MemoryCircuitStore>;

            // 状态变更时保存
            virtual bool save(const std::string &method,
                              const std::string &host, const CircuitStatus &status) override;
            // 启动时读加载
            virtual CircuitStatus load(const std::string &method,
                                       const std::string &host) override;
            // provider下线清理
            virtual bool remove(const std::string &method,
                                const std::string &host) override;

            virtual ~MemoryCircuitStore() = default;
            private:
            std::mutex _mutex;
            std::unordered_map<std::string, std::unordered_map<std::string, CircuitStatus>> _method_status;

          
        };

    }
}
