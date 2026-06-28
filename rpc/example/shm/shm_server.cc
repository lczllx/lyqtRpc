// ==================================================================
// shm_server.cc — Phase 4: ShmServer 完整示例（轮询模式）
// 编译: cd rpc && g++ -std=c++17 -I. -Ibuild example/shm/shm_server.cc \
//         src/general/shm_channel.cc -lpthread -o build/bin/shm_server
// ==================================================================
#include "src/server/shm_server.hpp"
#include "src/general/message.hpp"
#include <iostream>
#include <signal.h>

std::atomic<bool> running{true};

int main() {
    signal(SIGINT,  [](int){ running = false; });
    signal(SIGTERM, [](int){ running = false; });

    lcz_rpc::ShmServer server("lcz_shm", "lcz_shm_notify", 64*1024*1024, 64*1024*1024);

    server.setMessageCallback([](const lcz_rpc::BaseConnection::ptr& conn,
                                  lcz_rpc::BaseMessage::ptr& msg) {
        auto req = std::dynamic_pointer_cast<lcz_rpc::RpcRequest>(msg);
        if (!req) return;

        std::cout << "[server] recv method=" << req->method()
                  << " trace_id=" << req->trace_id() << std::endl;

        // 处理 add: 把两个参数加起来
        int a = req->params()["num1"].asInt();
        int b = req->params()["num2"].asInt();
        int result = a + b;

        auto resp = lcz_rpc::MessageFactory::create<lcz_rpc::RpcResponse>();
        resp->setId(req->rid());
        resp->setMsgType(lcz_rpc::MsgType::RSP_RPC);
        resp->setRcode(lcz_rpc::RespCode::SUCCESS);
        resp->setResult(result);

        conn->send(resp);
        std::cout << "[server] send result=" << result << std::endl;
    });

    // 后台线程跑 server（start 阻塞在轮询循环）
    std::thread([&]() {
        server.start();
    }).detach();

    std::cout << "[server] 共享内存已创建，等待请求... (Ctrl+C 退出)" << std::endl;
    while (running) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    server.stop();
    std::cout << "[server] 退出" << std::endl;
    return 0;
}
