// ==================================================================
// metrics_proto_server.cc — SHM Proto ZC 服务端 + Prometheus /metrics 端点
// ==================================================================
// 启动后:
//   - SHM Proto ZC 服务在 Unix socket lcz_shm_proto_bench_notify 上监听
//   - HTTP /metrics 端点在 :9090（Prometheus 可以抓取）
//   curl http://localhost:9090/metrics 查看所有指标
#include "src/server/shm_server_proto.hpp"
#include "src/general/message.hpp"
#include "src/general/metrics.hpp"
#include "src/general/metrics_server.hpp"
#include "src/general/log_system/lcz_log.h"
#include <iostream>
#include <thread>
#include <signal.h>
#include <chrono>

std::atomic<bool> running{true};

int main() {
    signal(SIGINT,  [](int){ running = false; });
    signal(SIGTERM, [](int){ running = false; });
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::ERROR);

    // 初始化指标（进程启动时间起点 + 预注册错误计数 + 版本号）
    lcz_rpc::metrics::MetricHooks::onServerStart(4);
    for (const std::string m : {"add", "echo"}) {
        lcz_rpc::metrics::Labels els = {{"method", m}, {"code", "INTERNAL_ERROR"}};
        METRICS_COUNTER("rpc_errors_total", "RPC handler errors", els);
    }
    lcz_rpc::metrics::MetricHooks::buildInfo();
    // 启动 Prometheus /metrics HTTP 端点（后台线程，端口 9090）
    lcz_rpc::metrics::MetricsServer::start(9090);
    std::cout << "[metrics] Prometheus /metrics endpoint on :9090" << std::endl;

    lcz_rpc::ShmServerProto server("lcz_shm_proto_bench_notify", "lcz_shm_proto_bench",
                                    1*1024*1024, 1*1024*1024, 32, 4);

    // 在业务回调中埋入指标
    server.setMessageCallback([](const lcz_rpc::BaseConnection::ptr& conn,
                                  lcz_rpc::BaseMessage::ptr& msg) {
        auto req = std::dynamic_pointer_cast<lcz_rpc::ProtoRpcRequest>(msg);
        if (!req) return;

        auto t1 = std::chrono::steady_clock::now();
        const std::string& method = req->method();

        auto resp = lcz_rpc::MessageFactory::create<lcz_rpc::ProtoRpcResponse>();
        resp->setId(req->rid());
        resp->setMsgType(lcz_rpc::MsgType::RSP_RPC_PROTO);

        if (method == "add") {
            lcz_rpc::proto::AddRequest add_req;
            add_req.ParseFromString(req->body());
            lcz_rpc::proto::AddResponse add_resp;
            add_resp.set_result(add_req.num1() + add_req.num2());
            resp->setRcode(lcz_rpc::RespCode::SUCCESS);
            resp->setBody(add_resp.SerializeAsString());
        } else if (method == "echo") {
            resp->setRcode(lcz_rpc::RespCode::SUCCESS);
            resp->setBody(req->body());
        } else {
            resp->setRcode(lcz_rpc::RespCode::SERVICE_NOT_FOUND);
        }

        conn->send(resp);

        auto t2 = std::chrono::steady_clock::now();
        double lat_us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

        // 记录指标（带 method 标签）
        lcz_rpc::metrics::Labels ls = {{"method", method}};
        METRICS_COUNTER("rpc_requests_total", "Total RPC requests", ls).inc();
        METRICS_HISTO("rpc_request_duration_us", "RPC handler latency in us", ls).observe(lat_us);
    });

    std::thread([&]() { server.start(); }).detach();
    std::cout << "[server] SHM Proto ZC 服务已启动，等待请求... (Ctrl+C 退出)" << std::endl;
    while (running) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    server.stop();
    lcz_rpc::metrics::MetricsServer::stop();
    return 0;
}
