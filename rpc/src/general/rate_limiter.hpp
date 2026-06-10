#pragma once
#include <chrono>
#include <atomic>
#include <mutex>

namespace lcz_rpc
{
    // 令牌桶限流器：固定速率生成令牌，桶满则拒绝，供 Provider 端保护自身不被过载
    class TokenBucket
    {
    public:
        using ptr = std::shared_ptr<TokenBucket>;

        // rate: 令牌生成速率（个/秒），burst: 桶容量（允许瞬时突发）
        TokenBucket(double rate, double burst)
            : _rate(rate), _burst(burst), _tokens(burst),
              _last_refill(nowSeconds()) {}

        // 尝试消费一个令牌，成功返回 true 并扣减，失败返回 false（需退避）
        bool allow()
        {
            double now_sec = nowSeconds();
            {
                std::lock_guard<std::mutex> lock(_mutex);
                // 按时间差补充令牌
                double elapsed = now_sec - _last_refill;
                _tokens += elapsed * _rate;
                if (_tokens > _burst)
                    _tokens = _burst;
                _last_refill = now_sec;
                if (_tokens >= 1.0)
                {
                    _tokens -= 1.0;
                    return true;
                }
            }
            return false;
        }

        // 返回建议的退避等待时间（毫秒），当前令牌耗尽时用
        int64_t retryAfterMs() const
        {
            // 等一个令牌生成出来：1/_rate 秒转换为毫秒，至少 1ms
            return static_cast<int64_t>(1000.0 / _rate) + 1;
        }

    private:
        static double nowSeconds()
        {
            auto now = std::chrono::steady_clock::now();
            return std::chrono::duration<double>(now.time_since_epoch()).count();
        }

    private:
        double _rate;        // 令牌生成速率（个/秒）
        double _burst;       // 桶容量（突发上限）
        double _tokens;      // 当前令牌数
        double _last_refill; // 上次补充时间（秒）
        std::mutex _mutex;
    };
} // namespace lcz_rpc
