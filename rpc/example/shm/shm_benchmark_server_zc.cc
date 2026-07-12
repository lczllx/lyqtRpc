// ==================================================================
// shm_benchmark_server_zc.cc — FlatBuffers 零拷贝 SHM 压测服务端
// ==================================================================
#include "src/server/shm_server_zc.hpp"
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

    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::ERROR);

    lcz_rpc::ShmServerZc server("lcz_shm_bench_zc_notify", "lcz_shm_bench_zc",
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
            resp->setRcode(lcz_rpc::RespCode::SUCCESS);
            resp->setResult(req->params()["num1"].asInt() + req->params()["num2"].asInt());
        } else if (method == "echo") {
            resp->setRcode(lcz_rpc::RespCode::SUCCESS);
            resp->setResult(req->params()["data"]);
        } else {
            resp->setRcode(lcz_rpc::RespCode::SERVICE_NOT_FOUND);
            resp->setResult(Json::Value());
        }

        conn->send(resp);
    });

    std::thread([&]() { server.start(); }).detach();

    std::cout << "[shm_bench_server_zc] FlatBuffers 零拷贝服务已启动, 等待请求..." << std::endl;
    while (running) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    server.stop();
    std::cout << "[shm_bench_server_zc] 退出" << std::endl;
    return 0;
}
