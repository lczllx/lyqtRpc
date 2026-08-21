#pragma once
#include <string>
#include <string_view>
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
            virtual bool save(std::string_view method,
                              std::string_view host, const CircuitStatus &status) override;
            // 启动时读加载
            virtual CircuitStatus load(std::string_view method,
                                       std::string_view host) override;
            // provider下线清理
            virtual bool remove(std::string_view method,
                                std::string_view host) override;

            virtual ~MemoryCircuitStore() = default;
            private:
            std::mutex _mutex;
            std::unordered_map<std::string, std::unordered_map<std::string, CircuitStatus>> _method_status;

          
        };

    }
}
