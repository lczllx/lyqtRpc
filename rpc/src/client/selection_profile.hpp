#pragma once
// =============================================================================
// selection_profile.hpp — 选主机（getCandidates）的在位耗时采样
// -----------------------------------------------------------------------------
// benchmark_consistent_hash 测的是 MethodHost::selectHost 单个函数
// （10 台主机 86.6 ns/op），但真实请求并不直接调它：
//   RpcClient::call → getCandidates → Discover::serviceDiscover
//     → MethodHost::selectHosts（持锁、建/排序/去重候选向量）
// 两者不是同一段代码，单函数微基准不能外推成「选主机占端到端 P99 多少」。
// 本文件把计时直接放在 getCandidates 里，按真实调用路径采样。
//
// 用法：
//   LCZ_PROFILE_SELECTION=1 ./bin/benchmark_client ...
//   或在代码里 setSelectionProfileEnabled(true)。
//   压测结束后 takeSelectionSamples() 取排序样本，再交给
//   summarizeSelectionSamples() 算分位数。
//
// 开销：关闭时每次 getCandidates 只有一次缓存 bool 判断；打开时每请求多
//   两次 steady_clock::now()（本机约 20 ns/次），相对百微秒级 RTT 可忽略。
//
// 线程模型：每个线程写自己的 thread_local 缓冲，线程退出时并入全局汇总，
//   因此 takeSelectionSamples() 既能看到已 join 线程的样本，也能看到调用
//   线程自己的样本。不要在采样线程仍在并发写的时候调用 take（live 缓冲
//   没有加锁，只应在压测结束后汇总）。
// =============================================================================

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

namespace lcz_rpc
{
    namespace profile
    {
        struct SelectionStats
        {
            size_t count = 0;
            uint64_t min_ns = 0;
            uint64_t max_ns = 0;
            uint64_t sum_ns = 0;
            double mean_ns = 0;
            double p50_ns = 0;
            double p90_ns = 0;
            double p99_ns = 0;
        };

        namespace detail
        {
            struct Registry
            {
                std::mutex mutex;
                std::vector<uint64_t> aggregate;           // 已退出线程并入的样本
                std::vector<std::vector<uint64_t> *> live; // 仍存活线程的缓冲
                std::atomic<int> override_state{-1};       // -1=跟随环境变量, 0=关, 1=开
            };

            inline Registry &registry()
            {
                // 故意泄漏：线程局部对象的析构可能发生在静态对象析构之后，
                // 用 new 保证汇总表活到进程退出。
                static Registry *r = new Registry();
                return *r;
            }

            inline bool envEnabled()
            {
                const char *v = std::getenv("LCZ_PROFILE_SELECTION");
                if (!v || v[0] == '\0')
                    return false;
                std::string s(v);
                return s != "0" && s != "false";
            }

            struct ThreadBuffer
            {
                std::vector<uint64_t> samples;

                ThreadBuffer()
                {
                    Registry &r = registry();
                    std::lock_guard<std::mutex> lk(r.mutex);
                    r.live.push_back(&samples);
                }

                ~ThreadBuffer()
                {
                    Registry &r = registry();
                    std::lock_guard<std::mutex> lk(r.mutex);
                    r.aggregate.insert(r.aggregate.end(), samples.begin(), samples.end());
                    auto it = std::find(r.live.begin(), r.live.end(), &samples);
                    if (it != r.live.end())
                        r.live.erase(it);
                }

                ThreadBuffer(const ThreadBuffer &) = delete;
                ThreadBuffer &operator=(const ThreadBuffer &) = delete;
            };

            inline ThreadBuffer &threadBuffer()
            {
                thread_local ThreadBuffer buf;
                return buf;
            }

            inline std::vector<uint64_t> takeLocked()
            {
                Registry &r = registry();
                std::vector<uint64_t> out = r.aggregate;
                for (auto *buf : r.live)
                    out.insert(out.end(), buf->begin(), buf->end());
                std::sort(out.begin(), out.end());
                return out;
            }
        } // namespace detail

        // 默认值来自 LCZ_PROFILE_SELECTION（进程内只读一次），可被显式开关覆盖
        inline bool selectionProfileEnabled()
        {
            static const bool default_enabled = detail::envEnabled();
            const int override_state =
                detail::registry().override_state.load(std::memory_order_relaxed);
            return override_state < 0 ? default_enabled : override_state != 0;
        }

        inline void setSelectionProfileEnabled(bool on)
        {
            detail::registry().override_state.store(on ? 1 : 0, std::memory_order_relaxed);
        }

        inline void recordSelectionNs(uint64_t ns)
        {
            if (!selectionProfileEnabled())
                return;
            detail::threadBuffer().samples.push_back(ns);
        }

        // 返回按升序排好的样本（ns）。只应在压测线程结束后调用。
        inline std::vector<uint64_t> takeSelectionSamples()
        {
            std::lock_guard<std::mutex> lk(detail::registry().mutex);
            return detail::takeLocked();
        }

        // 清空已收集样本（开关状态不变），测试和多次压测之间使用
        inline void resetSelectionProfile()
        {
            std::lock_guard<std::mutex> lk(detail::registry().mutex);
            auto &r = detail::registry();
            r.aggregate.clear();
            for (auto *buf : r.live)
                buf->clear();
        }

        // sorted_ns 必须是 takeSelectionSamples() 的返回（升序）。
        // 分位数用 nearest-rank：rank = ceil(p*n)，取第 rank 个样本。
        inline SelectionStats summarizeSelectionSamples(const std::vector<uint64_t> &sorted_ns)
        {
            SelectionStats s;
            if (sorted_ns.empty())
                return s;

            s.count = sorted_ns.size();
            s.min_ns = sorted_ns.front();
            s.max_ns = sorted_ns.back();
            for (uint64_t v : sorted_ns)
                s.sum_ns += v;
            s.mean_ns = static_cast<double>(s.sum_ns) / static_cast<double>(s.count);

            auto percentile = [&](double p) -> double {
                size_t rank = static_cast<size_t>(std::ceil(p * sorted_ns.size()));
                if (rank == 0)
                    rank = 1;
                if (rank > sorted_ns.size())
                    rank = sorted_ns.size();
                return static_cast<double>(sorted_ns[rank - 1]);
            };
            s.p50_ns = percentile(0.50);
            s.p90_ns = percentile(0.90);
            s.p99_ns = percentile(0.99);
            return s;
        }

        // 作用域计时：放进 getCandidates() 第一行即可按真实请求路径采样
        class SelectionScope
        {
        public:
            SelectionScope()
                : _enabled(selectionProfileEnabled())
            {
                if (_enabled)
                    _start = std::chrono::steady_clock::now();
            }

            ~SelectionScope()
            {
                if (!_enabled)
                    return;
                const auto elapsed = std::chrono::steady_clock::now() - _start;
                recordSelectionNs(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()));
            }

            SelectionScope(const SelectionScope &) = delete;
            SelectionScope &operator=(const SelectionScope &) = delete;

        private:
            bool _enabled;
            std::chrono::steady_clock::time_point _start;
        };
    } // namespace profile
} // namespace lcz_rpc
