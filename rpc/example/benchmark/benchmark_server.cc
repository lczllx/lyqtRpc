#include "../../src/server/rpc_server.hpp"
#include "../../src/general/message.hpp"
#include "../../src/general/metrics.hpp"
#include "../../src/general/metrics_server.hpp"
#include "rpc_envelope.pb.h"
#include <chrono>
#include <thread>
#include <csignal>

using namespace lcz_rpc::proto;

static void add_proto(const lcz_rpc::BaseConnection::ptr&, const AddRequest& req, AddResponse* resp) {
    resp->set_result(req.num1() + req.num2());
}
static void echo_proto(const lcz_rpc::BaseConnection::ptr&, const EchoRequest& req, EchoResponse* resp) {
    resp->set_data(req.data());
}
static void heavy_compute_proto(const lcz_rpc::BaseConnection::ptr&, const HeavyRequest& req, HeavyResponse* resp) {
    int result = 0;
    for (int i = 0; i < req.value(); ++i) result += i;
    resp->set_result(result);
}

int main(int argc, char* argv[]) {
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);

    int port = 8889, registry_port = 8080, rate_limit = 0;
    int metrics_port = 9090; // 可配置：同机多服务时避免 9090 冲突
    bool enable_discover = false;
    if (argc > 1) port = std::atoi(argv[1]);
    if (argc > 2) enable_discover = std::atoi(argv[2]) != 0;
    if (argc > 3) registry_port = std::atoi(argv[3]);
    if (argc > 4) rate_limit = std::atoi(argv[4]);
    if (argc > 5) metrics_port = std::atoi(argv[5]);

    std::cout << "启动性能测试服务端（默认序列化: Protobuf）..." << std::endl;
    std::cout << "端口: " << port << std::endl;
    if (rate_limit > 0) std::cout << "限流: " << rate_limit << " req/s" << std::endl;

    // 指标收集 lambda
    auto wrap = [](auto fn, std::string method) {
        return [fn, method](const lcz_rpc::BaseConnection::ptr& c, const auto& req, auto* resp) {
            lcz_rpc::metrics::Labels ls = {{"method", method}};
            auto& conc = METRICS_GAUGE("rpc_concurrency", "In-flight requests", ls);
            conc.inc();
            auto t1 = std::chrono::steady_clock::now();
            try { fn(c, req, resp); } catch (...) {
                METRICS_COUNTER("rpc_errors_total", "RPC handler errors", ls).inc();
                conc.dec(); throw;
            }
            auto t2 = std::chrono::steady_clock::now();
            conc.dec();
            METRICS_COUNTER("rpc_requests_total", "Total RPC requests", ls).inc();
            METRICS_HISTO("rpc_request_duration_us", "Handler latency in us", ls)
                .observe(std::chrono::duration_cast<std::chrono::microseconds>(t2-t1).count());
        };
    };

    METRICS_GAUGE("rpc_connection_count", "Current TCP connections", {}).set(0);
    // 预注册各 method 的错误计数为 0：Counter 懒注册，
    // 不预注册则首个错误发生前 /metrics 里看不到该序列，
    // Prometheus 无法区分"零错误"和"指标不存在"
    for (const std::string m : {"add", "echo", "heavy"}) {
        lcz_rpc::metrics::Labels els = {{"method", m}};
        METRICS_COUNTER("rpc_errors_total", "RPC handler errors", els);
    }
    lcz_rpc::metrics::MetricHooks::onServerStart(4);
    lcz_rpc::metrics::MetricsServer::start(metrics_port);

    // 后台线程: RpcServer 必须在自己的线程构造+启动
    // (muduo EventLoop 要求 线程亲和性: 构造和 loop() 在同一线程)
    std::shared_ptr<lcz_rpc::server::RpcServer> srv;
    std::mutex srv_mtx; std::condition_variable srv_cv;

    std::thread svr_thread([&]() {
        auto s = std::make_shared<lcz_rpc::server::RpcServer>(
            lcz_rpc::HostInfo("127.0.0.1", port),
            enable_discover, lcz_rpc::HostInfo("127.0.0.1", registry_port));
        s->registerProtoHandler<AddRequest, AddResponse>("add", wrap(add_proto, "add"));
        s->registerProtoHandler<EchoRequest, EchoResponse>("echo", wrap(echo_proto, "echo"));
        s->registerProtoHandler<HeavyRequest, HeavyResponse>("heavy_compute", wrap(heavy_compute_proto, "heavy"));
        if (rate_limit > 0) s->setRateLimiter(rate_limit, rate_limit * 2);
        {
            std::lock_guard lk(srv_mtx);
            srv = s;
        }
        srv_cv.notify_one();
        s->start();
    });

    // 等 server 构造完成(此时 handler 已注册)
    { std::unique_lock lk(srv_mtx); srv_cv.wait(lk, [&]{ return srv != nullptr; }); }

    std::cout << "服务端启动成功，等待请求... (Ctrl+C 退出)" << std::endl;
    std::cout << "  Prometheus /metrics → http://localhost:" << metrics_port << "/metrics" << std::endl;

    // 全局 flag + sigaction(所有线程共享 handler)
    static std::atomic<bool> quit{false};
    struct sigaction sa{}; sa.sa_handler = [](int){ quit.store(true); };
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    while (!quit.load()) std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::cout << "\n收到信号，正在退出..." << std::endl;
    lcz_rpc::metrics::MetricsServer::stop();
    _exit(0);
    return 0;
}
