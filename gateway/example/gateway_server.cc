// =============================================================================
// gateway_server.cc — API Gateway 启动入口
// =============================================================================
// Phase 4: 限流（TokenBucket → 429）+ 熔断（RpcClient 自带）+ 指标（Prometheus）
#include "../src/http_server.hpp"
#include "../src/http_router.hpp"
#include "../src/gateway_handler.hpp"
#include "../src/diagnose_handler.hpp"
#include "../../rpc/src/general/rate_limiter.hpp"
#include "../../rpc/src/general/metrics.hpp"
#include "../../rpc/src/general/metrics_server.hpp"
#include "../../rpc/src/general/metrics_hooks.hpp"
#include <muduo/net/EventLoop.h>
#include <iostream>
#include <csignal>
#include <atomic>

using namespace lcz_rpc::metrics; // Labels, METRICS_* 宏
using lcz_rpc::TokenBucket;

// Signal handler 需要访问 EventLoop::quit()，但 sigaction 回调是 C 函数，
// 不能传 lambda capture。用文件作用域的静态指针桥接——和 MetricsServer 同款模式。
// muduo::EventLoop::quit() 是线程安全的（内部往 eventfd 写字节唤醒 epoll_wait），
// 解决了"手写 while(accept) 阻塞时 signal 无法退出"的问题（见 shm-serialization-pitfalls.md 第 4 条）。
static muduo::net::EventLoop *g_loop = nullptr;

static void onSignal(int)
{
    if (g_loop)
        g_loop->quit();
}

int main()
{
    signal(SIGINT, onSignal);
    signal(SIGTERM, onSignal);

    muduo::net::EventLoop loop;
    g_loop = &loop;

    // ---- 后端 RPC 连接 ----
    lcz_gateway::GatewayHandler handler(/*discover=*/false, "127.0.0.1", 8889);

    // ---- 限流：每秒 1000 请求，burst=2000 ----
    TokenBucket limiter(/*rate=*/1000.0, /*burst=*/2000.0);

    // ---- 指标初始化 ----
    MetricHooks::onServerStart(4);
    MetricHooks::buildInfo();
    MetricsServer::start(/*port=*/9091); // 网关独立端口，不和 RPC Server 的 9090 冲突
    // 预注册网关错误码
    for (const char *path : {"/api/echo", "/api/health"})
        for (const char *code : {"429", "502", "500"})
        {
            Labels els = {{"path", path}, {"code", code}};
            METRICS_COUNTER("gateway_errors_total", "Gateway error count", els);
        }

    // ---- 路由表 ----
    lcz_gateway::HttpRouter router;
    router.addRoute("POST", "/api/echo", handler.echoRoute());
    lcz_gateway::DiagnoseHandler diag(limiter);
    router.addRoute("GET", "/diagnose", diag.route());

    router.addRoute("GET", "/api/health", [](const lcz_gateway::HttpReq &, lcz_gateway::HttpResp *resp)
                    { resp->setBody(R"({"status":"ok"})"); });

    // ---- HTTP Server ----
    lcz_gateway::HttpServer srv(&loop, 8080, 4);
    srv.setCallback([&](const lcz_gateway::HttpReq &req,
                        lcz_gateway::HttpResp *resp)
                    {
        auto t1 = std::chrono::steady_clock::now();

        // ① 限流
        if (!limiter.allow()) {
            resp->status = 429;
            resp->setBody(R"({"error":"rate limited","retry_after_ms":)" +
                          std::to_string(limiter.retryAfterMs()) + "}");
            Labels els = {{"path", req.path}, {"code", "429"}};
            METRICS_COUNTER("gateway_errors_total", "Gateway error count", els).inc();
            return;
        }

        // ② 路由分发
        auto h = router.dispatch(req.method, req.path);
        if (!h) {
            resp->status = 404;
            resp->setBody(R"({"error":"not found","path":")" + req.path + "\"}");
            return;
        }

        // ③ 执行业务 handler（可能调 RPC 后端，熔断由 RpcClient 内部处理）
        h(req, resp);

        auto t2 = std::chrono::steady_clock::now();
        double lat_us = std::chrono::duration_cast<std::chrono::microseconds>(
            t2 - t1).count();

        // ④ 指标埋点：和 benchmark_server.cc 的 wrap() 完全同款——
        // 每次 HTTP 请求记一次 gateway_requests_total +1，延迟进直方图桶。
        // gateway_request_duration_us 测的是"网关端到端耗时"（含限流判定 + 路由
        // 查表 + RPC 调用 + JSON 互转），与后端自己的 rpc_request_duration_us
        // （仅 handler 耗时）互补——两者相减可得网关自身开销。
        Labels ls = {{"path", req.path}, {"method", req.method}};
        METRICS_COUNTER("gateway_requests_total", "Gateway total requests", ls).inc();
        METRICS_HISTO("gateway_request_duration_us",
                      "Gateway request latency in us", ls).observe(lat_us);

        if (resp->status >= 400) {
            std::string code = std::to_string(resp->status);
            Labels els = {{"path", req.path}, {"code", code}};
            METRICS_COUNTER("gateway_errors_total", "Gateway error count", els).inc();
        } });

    srv.start();
    std::cout << "[gateway] :8080 ready (backend :8889, metrics :9091)" << std::endl;
    std::cout << "  curl -d '{\"data\":\"hello\"}' http://localhost:8080/api/echo" << std::endl;
    std::cout << "  curl localhost:9091/metrics | grep gateway" << std::endl;

    loop.loop();
    MetricsServer::stop();
    return 0;
}
