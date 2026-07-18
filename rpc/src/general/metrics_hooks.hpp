#pragma once
// =============================================================================
// metrics_hooks.hpp — 在业务模块中注入 Prometheus 指标
// =============================================================================
// 不需要修改业务代码，在启动/回调中调用这些函数即可埋入指标。
// 按 brpc /vars 对标，覆盖：server uptime, error code, circuit breaker, token bucket
// =============================================================================
#include "metrics.hpp"
#include "../general/fields.hpp"
#include <chrono>

namespace lcz_rpc
{
    namespace metrics
    {

        class MetricHooks
        {
            // 进程加载时（静态初始化）即记录起点：
            // 未调用 onServerStart 的进程 uptime 也是正确的"进程存活时长"；
            // 若默认构造（steady_clock 零点≈开机时刻），uptime 会算出十几天的巨值
            static inline std::chrono::steady_clock::time_point _start_time =
                std::chrono::steady_clock::now();

        public:
            // 服务端启动时调用一次，记录 uptime 起始点
            static void onServerStart(int worker_threads)
            {
                _start_time = std::chrono::steady_clock::now();
                METRICS_GAUGE("rpc_worker_threads", "Worker thread count", {}).set(worker_threads);
                METRICS_GAUGE("rpc_connection_count", "Active connections", {}).set(0);
                // uptime 不在 exportText 里算(exportText 持锁不应调外部函数)，
                // 而是在每次 exportText 前由调用方更新
                auto &u = METRICS_GAUGE("rpc_server_uptime_seconds", "Server uptime in seconds", {});
                u.set(0); // 占位，每次 scrape 前更新
            }

            // 每 15s Prometheus scrape 时导出当前 uptime
            static double uptimeSeconds()
            {
                return std::chrono::duration<double>(
                           std::chrono::steady_clock::now() - _start_time)
                    .count();
            }

            // Handler 入/出: 自动记录请求数+延迟+错误码+并发度
            // 用法: 在 handler 入口调 onRequestStart(method), 出口调 onRequestEnd(method, rcode, lat_us)
            static void onRequestStart(const std::string &method)
            {
                Labels ls = {{"method", method}};
                METRICS_GAUGE("rpc_concurrency", "In-flight requests", ls).inc();
            }
            static void onRequestEnd(const std::string &method, int rcode, double lat_us)
            {
                Labels ls = {{"method", method}};
                METRICS_COUNTER("rpc_requests_total", "Total RPC requests", ls).inc();
                METRICS_HISTO("rpc_request_duration_us", "Handler latency in us", ls).observe(lat_us);
                METRICS_GAUGE("rpc_concurrency", "In-flight requests", ls).dec();

                if (rcode != 0)
                {
                    // 用 errReason 将 RespCode int 转为可读字符串（如 5→TIMEOUT, 10→BACKOFF），
                    // 替换之前裸整数 "code=5" 无法直读的问题
                    std::string reason = errReason(static_cast<RespCode>(rcode));
                    Labels els = {{"method", method}, {"code", reason}};
                    METRICS_COUNTER("rpc_errors_total", "RPC handler errors", els).inc();
                }
            }

            // 客户端侧: send() 时记录
            static void onClientSend(const std::string &method)
            {
                Labels ls = {{"method", method}};
                METRICS_COUNTER("rpc_client_requests_total", "Client requests", ls).inc();
                METRICS_GAUGE("rpc_client_concurrency", "Client in-flight", ls).inc();
            }
            // error_code 空串 = 成功；非空 = 具体错误分类名 (send_failed/backoff/remote_TIMEOUT ...)
            static void onClientRecv(const std::string &method, double lat_us, const std::string& error_code)
            {
                Labels ls = {{"method", method}};
                METRICS_HISTO("rpc_client_latency_us", "Client RTT in us", ls).observe(lat_us);
                METRICS_GAUGE("rpc_client_concurrency", "Client in-flight", ls).dec();
                if (!error_code.empty())
                {
                    Labels els = {{"method", method}, {"code", error_code}};
                    METRICS_COUNTER("rpc_client_errors_total", "Client errors", els).inc();
                }
                // 预注册最常见的失败原因为 0，确保 Prometheus 在零错误期也能看到序列
                else
                {
                    for (const char* ec : {"send_failed", "backoff", "parse_failed", "circuit_open"})
                    {
                        Labels els = {{"method", method}, {"code", ec}};
                        METRICS_COUNTER("rpc_client_errors_total", "Client errors", els).add(0);
                    }
                }
            }

            // 连接新建/关闭
            static void onConnectionOpen() { METRICS_GAUGE("rpc_connection_count", "Active connections", {}).inc(); }
            static void onConnectionClose() { METRICS_GAUGE("rpc_connection_count", "Active connections", {}).dec(); }

            // 熔断器状态上报(0=CLOSED 1=OPEN 2=HALF_OPEN)
            static void onCircuitState(const std::string &method, const std::string &host, int state)
            {
                Labels ls = {{"method", method}, {"host", host}};
                METRICS_GAUGE("circuit_breaker_state", "Circuit breaker state", ls).set(state);
            }

            // 令牌桶余量
            static void onTokenBucket(const std::string &service, double available)
            {
                Labels ls = {{"service", service}};
                METRICS_GAUGE("token_bucket_available", "Token bucket available tokens", ls).set(available);
            }

            // 限流拒绝计数（对标 brpc 被拒绝请求数）
            static void onRateLimited(const std::string &service)
            {
                Labels ls = {{"service", service}};
                METRICS_COUNTER("rpc_rate_limited_total", "Requests rejected by rate limiter", ls).inc();
            }

            // 注册中心: provider 数 + 心跳成功/失败
            static void onRegistryProviderCount(int count)
            {
                METRICS_GAUGE("registry_providers_total", "Registered providers", {}).set(count);
            }
            static void onRegistryHeartbeat(const std::string &method, bool success)
            {
                Labels ls = {{"method", method}};
                METRICS_COUNTER("registry_heartbeats_total", "Registry heartbeats", ls).inc();
                if (!success)
                {
                    Labels els = {{"method", method}, {"code", "failed"}};
                    METRICS_COUNTER("registry_heartbeat_errors_total", "Failed heartbeats", els).inc();
                }
            }

            // lyqtrpc_build_info：版本/commit/构建时间，恒为 1 的 gauge，
            // 供 Grafana 按版本关联指标、定位部署变更时间点
            static void buildInfo()
            {
#ifdef LCZ_RPC_VERSION
                Labels ls = {{"version", LCZ_RPC_VERSION},
                              {"commit", LCZ_GIT_COMMIT},
                              {"build_time", LCZ_BUILD_TIME}};
#else
                Labels ls = {{"version", "unknown"},
                              {"commit", "unknown"},
                              {"build_time", "unknown"}};
#endif
                METRICS_GAUGE("lyqtrpc_build_info", "Build metadata (value=1)", ls).set(1);
            }

            // exportText 时自动注入 uptime(无需单独注册)
            static double getUptime() { return uptimeSeconds(); }
        };

    } // namespace metrics
} // namespace lcz_rpc
