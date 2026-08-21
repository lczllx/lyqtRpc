#include <string>
#include <memory>
#include "memory_circuit_store.hpp"

namespace lcz_rpc
{
    namespace server
    {
        // 状态变更时保存
        bool MemoryCircuitStore::save(std::string_view method,
                                      std::string_view host, const CircuitStatus &status)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _method_status[std::string(method)][std::string(host)] = status;
            return true;
        }

        // 启动时读加载
        CircuitStatus MemoryCircuitStore::load(std::string_view method,
                                               std::string_view host)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto it = _method_status.find(std::string(method));
            if (it == _method_status.end())
                return CircuitStatus{};
            auto host_it = it->second.find(std::string(host));
            if (host_it == it->second.end())
                return CircuitStatus{};
            return host_it->second;
        }

        // provider下线清理
        bool MemoryCircuitStore::remove(std::string_view method,
                                        std::string_view host)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto it = _method_status.find(std::string(method));
            if (it == _method_status.end())
                return false;
            auto &host_map = it->second;
            auto host_it = host_map.find(std::string(host));
            if (host_it == host_map.end())
                return false;

            host_map.erase(host_it);
            if (host_map.empty()) {
                _method_status.erase(it);
            }
            return true;
        }
    }
}
