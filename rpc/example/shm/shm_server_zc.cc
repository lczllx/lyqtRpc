// ==================================================================
// shm_server_zc.cc — FlatBuffers 零拷贝 SHM Server 示例
// ==================================================================
#include "src/server/shm_server_zc.hpp"
#include "src/general/message.hpp"
#include <iostream>
#include <thread>
#include <signal.h>

std::atomic<bool> running{true};

int main() {
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);
    signal(SIGINT,  [](int){ running = false; });
    signal(SIGTERM, [](int){ running = false; });

    lcz_rpc::ShmServerZc server("lcz_shm_zc_notify", "lcz_shm_zc",
                                 64 * 1024 * 1024, 64 * 1024 * 1024, 16);

    server.setMessageCallback([](const lcz_rpc::BaseConnection::ptr& conn,
                                  lcz_rpc::BaseMessage::ptr& msg) {
        auto req = std::dynamic_pointer_cast<lcz_rpc::RpcRequest>(msg);
        if (!req) return;

        std::cout << "[server_zc] recv method=" << req->method()
                  << " id=" << req->rid()
                  << " trace_id=" << req->trace_id() << std::endl;

        int a = req->params()["num1"].asInt();
        int b = req->params()["num2"].asInt();
        int result = a + b;

        auto resp = lcz_rpc::MessageFactory::create<lcz_rpc::RpcResponse>();
        resp->setId(req->rid());
        resp->setMsgType(lcz_rpc::MsgType::RSP_RPC);
        resp->setRcode(lcz_rpc::RespCode::SUCCESS);
        resp->setResult(result);

        conn->send(resp);
        std::cout << "[server_zc] send result=" << result
                  << " id=" << resp->rid() << std::endl;
    });

    std::thread([&]() { server.start(); }).detach();

    std::cout << "[server_zc] 零拷贝服务已启动，等待请求... (Ctrl+C 退出)" << std::endl;
    while (running) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    server.stop();
    std::cout << "[server_zc] 退出" << std::endl;
    return 0;
}
