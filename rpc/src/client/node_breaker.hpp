#pragma once
#include <string>
#include <memory>
#include "../general/publicconfig.hpp"
#include "../server/circuit_store.hpp"
#include <chrono>
#include <mutex>
namespace lcz_rpc
{
    namespace client
    {
        // 一个 NodeBreaker 管一个远端的熔断状态
        // 管理一个 provider 的一个方法
        class NodeBreaker
        {
        public:
            using ptr = std::shared_ptr<NodeBreaker>;
            // 接收收到的请求——检查当前状态是否允许本次请求通过
            bool allowRequest();

            // 成功调用——返回true表示状态发生了转换（外层据此决定是否 save）
            bool onSuccess();

            // 失败调用——返回true表示状态发生了转换（外层据此决定是否 save）
            bool onFailure();

            // 获取熔断器当前状态
            CircuitStatus status() const { return _status; }

            // 构造时传入配置，可选初始状态用于 etcd 恢复
            explicit NodeBreaker(const CircuitConfig &cfg) : _cfg(cfg) {}
            void loadStatus(const CircuitStatus &s) { _status = s; }
       
        private:
            std::mutex _mutex;
            CircuitConfig _cfg;                                  // 当前熔断器的限制参数
            CircuitStatus _status;                               // 当前熔断器状态
            std::chrono::steady_clock::time_point _opened_since; // 进入 OPEN 的时刻（运行时用，重启即丢）
        };

    }
}
