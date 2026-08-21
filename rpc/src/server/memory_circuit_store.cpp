#include <string>
#include <memory>
#include "memory_circuit_store.hpp"

namespace lcz_rpc
{
    namespace server
    {
        // 状态变更时保存
        bool MemoryCircuitStore::save(const std::string &method,
                                      const std::string &host, const CircuitStatus &status)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _method_status[method][host] = status;
            return true;
        }

        // 启动时读加载
        CircuitStatus MemoryCircuitStore::load(const std::string &method,
                                               const std::string &host)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto it = _method_status.find(method);
            if (it == _method_status.end())
                return CircuitStatus{};
            auto host_it = it->second.find(host);
            if (host_it == it->second.end())
                return CircuitStatus{};
            return host_it->second;
        }

        // provider下线清理
        bool MemoryCircuitStore::remove(const std::string &method,
                                        const std::string &host)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto it = _method_status.find(method);
            if (it == _method_status.end())
                return false;
            auto &host_map = it->second;
            auto host_it = host_map.find(host);
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
