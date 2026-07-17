#pragma once
// =============================================================================
// metrics_hooks.hpp — 在业务模块中注入 Prometheus 指标
// =============================================================================
// 不需要修改业务代码，在启动/回调中调用这些函数即可埋入指标。
// 按 brpc /vars 对标，覆盖：server uptime, error code, circuit breaker, token bucket
// =============================================================================
#include "metrics.hpp"
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
                    Labels els = {{"method", method}, {"code", std::to_string(rcode)}};
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
            static void onClientRecv(const std::string &method, double lat_us, bool success)
            {
                Labels ls = {{"method", method}};
                METRICS_HISTO("rpc_client_latency_us", "Client RTT in us", ls).observe(lat_us);
                METRICS_GAUGE("rpc_client_concurrency", "Client in-flight", ls).dec();
                // 无论成败都 touch 错误计数（成功时 +0）：
                // Counter 是懒注册的，若只在失败时写，零错误期间序列不存在，
                // Prometheus 无法区分"没有错误"和"没有该指标"，报警规则也无法预建
                Labels els = {{"method", method}, {"code", "error"}};
                METRICS_COUNTER("rpc_client_errors_total", "Client errors", els).add(success ? 0 : 1);
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

            // exportText 时自动注入 uptime(无需单独注册)
            static double getUptime() { return uptimeSeconds(); }
        };

    } // namespace metrics
} // namespace lcz_rpc
