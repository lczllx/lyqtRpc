#pragma once
#include <string>
#include <memory>
#include "../general/publicconfig.hpp"

namespace lcz_rpc
{
    namespace server
    {
        // 熔断器状态持久化接口
        // host 使用 "IP:port" 字符串格式，与 caller 侧统一
        class ICircuitStateStore
        {
        public:
            using ptr = std::shared_ptr<ICircuitStateStore>;

            // 状态变更时保存
            virtual bool save(const std::string &method,
                              const std::string &host, const CircuitStatus &status) = 0;
            // 启动时读加载
            virtual CircuitStatus load(const std::string &method,
                                       const std::string &host) = 0;
            // provider下线清理
            virtual bool remove(const std::string &method,
                                const std::string &host) = 0;

            virtual ~ICircuitStateStore() = default;
        };
    }
}
