#include <string>
#include <memory>
#include <ctime>
#include "node_breaker.hpp"
#include "../general/log_system/lcz_log.h"
#include "../general/metrics_hooks.hpp"

namespace lcz_rpc
{
    namespace client
    {
        // 接收收到的请求——检查当前状态是否允许本次请求通过
        bool NodeBreaker::allowRequest()
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_status.state == CircuitState::CLOSED)
                return true;
            else if (_status.state == CircuitState::OPEN)
            {
                auto elapsed = std::chrono::steady_clock::now() - _opened_since;
                // 如果超过了开放限制时间，也放行，但设为半开状态，++half_open
                if (elapsed >= std::chrono::seconds(_cfg.open_duration_sec))
                {
                    _status.state = CircuitState::HALF_OPEN;
                    _status.half_open++;
                    lcz_rpc::metrics::MetricHooks::onCircuitState(_method, _host, 2);  // HALF_OPEN
                    LCZ_INFO("[NodeBreaker] OPEN => HALF_OPEN (elapsed=%llds >= %ds), 放行探测请求",
                             static_cast<long long>(
                                 std::chrono::duration_cast<std::chrono::seconds>(elapsed).count()),
                             _cfg.open_duration_sec);
                    return true;
                }
                else
                    return false;
            }
            else if (_status.state == CircuitState::HALF_OPEN)
            {
                // 如果实际半开放放行数小于，可以放行，++half_open
                if (_status.half_open < _cfg.half_open_max_req)
                {
                    _status.half_open++;
                    return true;
                }
                else
                    return false;
            }
            return false;
        }

        // 成功调用——返回true表示状态发生了转换（外层据此决定是否 save）
        bool NodeBreaker::onSuccess()
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _status.failures = 0;
            _status.half_open = 0;
            if (_status.state != CircuitState::CLOSED)
            {
                LCZ_INFO("[NodeBreaker] HALF_OPEN/OPEN => CLOSED, 熔断器恢复");
                _status.state = CircuitState::CLOSED;
                lcz_rpc::metrics::MetricHooks::onCircuitState(_method, _host, 0);  // CLOSED
                return true;
            }
            else
                return false;
        }

        // 失败调用——返回true表示状态发生了转换（外层据此决定是否 save）
        bool NodeBreaker::onFailure()
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _status.failures++;
            if (_status.state == CircuitState::CLOSED)
            {
                if (_status.failures >= _cfg.failure_threshold)
                {
                    _status.state = CircuitState::OPEN;
                    _opened_since = std::chrono::steady_clock::now();
                    lcz_rpc::metrics::MetricHooks::onCircuitState(_method, _host, 1);  // OPEN
                    _status.opened_at = static_cast<int64_t>(std::time(nullptr));
                    LCZ_INFO("[NodeBreaker] CLOSED => OPEN, 连续失败%d次达到阈值%d",
                             _status.failures, _cfg.failure_threshold);
                    return true;
                }
                else
                    return false;
            }
            else if (_status.state == CircuitState::OPEN)
            {
                // allowRequest 已经拒绝了所有请求，但可能因为并发进来
                return false;
            }
            else if (_status.state == CircuitState::HALF_OPEN)
            {
                _status.opened_at = static_cast<int64_t>(std::time(nullptr));
                _status.state = CircuitState::OPEN;
                _opened_since = std::chrono::steady_clock::now(); // 重设定时器
                lcz_rpc::metrics::MetricHooks::onCircuitState(_method, _host, 1);  // OPEN
                LCZ_INFO("[NodeBreaker] HALF_OPEN => OPEN, 探测请求失败");
                return true;
            }
            return false;
        }

    }

}
