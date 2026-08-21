#pragma once
#include <string>
#include <memory>
#include "general/publicconfig.hpp"
#include "server/circuit_store.hpp"
#include <chrono>
#include <mutex>
#include <ctime>
#include <algorithm>
#include <cstdint>
namespace lcz_rpc
{
    namespace client
    {
        // 熔断器状态机（单节点粒度，method×host）：
        // CLOSED → (连续失败>=threshold) → OPEN → (等待open_duration_sec) → HALF_OPEN
        // HALF_OPEN → (探测成功) → CLOSED  |  HALF_OPEN → (探测失败) → OPEN
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
            void loadStatus(const CircuitStatus &s)
            {
                _status = s;
                // 恢复 OPEN 的冷却起点：_opened_since 是运行时 steady_clock，重启即丢。
                // 若不恢复，_opened_since 停留在 epoch，allowRequest 会误判冷却期已过，
                // 首次请求就把 OPEN 降级成 HALF_OPEN，绕过冷却期。
                if (_status.state == CircuitState::OPEN)
                {
                    auto now = std::chrono::steady_clock::now();
                    int64_t wall_now = static_cast<int64_t>(std::time(nullptr));
                    auto ago = std::chrono::seconds(std::max<int64_t>(0, wall_now - _status.opened_at));
                    _opened_since = now - ago;
                }
            }
            void setIdentity(const std::string& method, const std::string& host)
                { _method = method; _host = host; }

        private:
            std::mutex _mutex;
            CircuitConfig _cfg;                                  // 当前熔断器的限制参数
            CircuitStatus _status;                               // 当前熔断器状态
            std::chrono::steady_clock::time_point _opened_since; // 进入 OPEN 的时刻（运行时用，重启即丢）
            std::string _method, _host;                          // 用于 Prometheus 标签
        };

    }
}
