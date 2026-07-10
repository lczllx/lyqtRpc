// ==================================================================
// shm_benchmark_server.cc — SHM 性能测试服务端
// 处理 add (两数求和) 和 echo (原样返回)
// 编译: 由 CMakeLists.txt 自动构建
// ==================================================================
#include "src/server/shm_server.hpp"
#include "src/general/message.hpp"
#include "src/general/log_system/lcz_log.h"
#include <iostream>
#include <signal.h>
#include <atomic>
#include <thread>

std::atomic<bool> running{true};

int main() {
    signal(SIGINT,  [](int){ running = false; });
    signal(SIGTERM, [](int){ running = false; });

    // 关闭框架日志，避免干扰 benchmark 输出
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);

    lcz_rpc::ShmServer server("lcz_shm_bench_notify", "lcz_shm_bench",
                               1 * 1024 * 1024, 1 * 1024 * 1024, 32);

    server.setMessageCallback([](const lcz_rpc::BaseConnection::ptr& conn,
                                  lcz_rpc::BaseMessage::ptr& msg) {
        auto req = std::dynamic_pointer_cast<lcz_rpc::RpcRequest>(msg);
        if (!req) return;

        const std::string& method = req->method();
        auto resp = lcz_rpc::MessageFactory::create<lcz_rpc::RpcResponse>();
        resp->setId(req->rid());
        resp->setMsgType(lcz_rpc::MsgType::RSP_RPC);

        if (method == "add") {
            int a = req->params()["num1"].asInt();
            int b = req->params()["num2"].asInt();
            resp->setRcode(lcz_rpc::RespCode::SUCCESS);
            resp->setResult(a + b);
        } else if (method == "echo") {
            resp->setRcode(lcz_rpc::RespCode::SUCCESS);
            resp->setResult(req->params()["data"]);
        } else {
            resp->setRcode(lcz_rpc::RespCode::SERVICE_NOT_FOUND);
            resp->setResult(Json::Value());
        }

        conn->send(resp);
    });

    // 后台线程跑 server（start 阻塞在 epoll 轮询循环）
    std::thread([&]() { server.start(); }).detach();

    std::cout << "[shm_bench_server] 共享内存已创建 (lcz_shm_bench), 等待请求..." << std::endl;
    while (running) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    server.stop();
    std::cout << "[shm_bench_server] 退出" << std::endl;
    return 0;
}
