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
#include <vector>
namespace lcz_rpc
{
    namespace client
    {
        // 熔断器状态机（单节点粒度，method×host）：
        // CLOSED → (窗口内样本数>=min_calls 且失败率>=failure_ratio_percent) → OPEN
        // OPEN → (等待open_duration_sec) → HALF_OPEN
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
            explicit NodeBreaker(const CircuitConfig &cfg) : _cfg(cfg)
            {
                // 配置自愈：窗口/最小样本/失败率越界会让熔断器永远不触发或首次调用就触发
                if (_cfg.window_size < 1) _cfg.window_size = 1;
                if (_cfg.min_calls < 1) _cfg.min_calls = 1;
                if (_cfg.min_calls > _cfg.window_size) _cfg.min_calls = _cfg.window_size;
                if (_cfg.failure_ratio_percent < 1) _cfg.failure_ratio_percent = 1;
                if (_cfg.failure_ratio_percent > 100) _cfg.failure_ratio_percent = 100;
                _window.assign(_cfg.window_size, EMPTY);
            }
            void loadStatus(const CircuitStatus &s)
            {
                _status = s;
                // 窗口内容不持久化：恢复后从空窗口重新统计，避免历史失败把刚恢复的节点立刻重新熔断
                clearWindow();
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
            static constexpr int8_t EMPTY = -1;
            static constexpr int8_t FAILED = 0;
            static constexpr int8_t SUCCEEDED = 1;

            // 记录一次调用结果到滑动窗口，覆盖最旧的位置（调用方必须已持有 _mutex）
            void record(bool success)
            {
                int8_t &slot = _window[_window_pos];
                if (slot == FAILED) --_window_failures;
                else if (slot == SUCCEEDED) --_window_successes;
                slot = success ? SUCCEEDED : FAILED;
                if (success) ++_window_successes;
                else ++_window_failures;
                _window_pos = (_window_pos + 1) % _window.size();
                _status.failures = _window_failures;
                _status.successes = _window_successes;
            }
            // 清空窗口（调用方必须已持有 _mutex）
            void clearWindow()
            {
                std::fill(_window.begin(), _window.end(), EMPTY);
                _window_pos = 0;
                _window_failures = 0;
                _window_successes = 0;
                _status.failures = 0;
                _status.successes = 0;
            }
            // 窗口内样本够多且失败率达到阈值（调用方必须已持有 _mutex）
            bool shouldOpen() const
            {
                int total = _window_failures + _window_successes;
                if (total < _cfg.min_calls) return false;
                return _window_failures * 100 >= total * _cfg.failure_ratio_percent;
            }

            std::mutex _mutex;
            CircuitConfig _cfg;                                  // 当前熔断器的限制参数
            CircuitStatus _status;                               // 当前熔断器状态
            std::chrono::steady_clock::time_point _opened_since; // 进入 OPEN 的时刻（运行时用，重启即丢）
            std::string _method, _host;                          // 用于 Prometheus 标签
            std::vector<int8_t> _window;                         // 最近 window_size 次结果（环形覆盖）
            size_t _window_pos = 0;                              // 下一个待覆盖的位置
            int _window_failures = 0;                            // 窗口内失败数
            int _window_successes = 0;                           // 窗口内成功数
        };

    }
}
