// SHM + Protobuf 零拷贝 Server 示例
#include "src/server/shm_server_proto.hpp"
#include "src/general/message.hpp"
#include <iostream>
#include <thread>
#include <signal.h>

std::atomic<bool> running{true};

int main() {
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);
    signal(SIGINT,  [](int){ running = false; });
    signal(SIGTERM, [](int){ running = false; });

    lcz_rpc::ShmServerProto server("lcz_shm_proto_notify", "lcz_shm_proto",
                                    1*1024*1024, 1*1024*1024, 32, 4);

    server.setMessageCallback([](const lcz_rpc::BaseConnection::ptr& conn,
                                  lcz_rpc::BaseMessage::ptr& msg) {
        auto req = std::dynamic_pointer_cast<lcz_rpc::ProtoRpcRequest>(msg);
        if (!req) return;
        std::cout << "[server_proto] recv method=" << req->method()
                  << " id=" << req->rid() << std::endl;

        // 解析 body 中的 add 参数（protobuf AddRequest）
        lcz_rpc::proto::AddRequest add_req;
        add_req.ParseFromString(req->body());

        auto resp = lcz_rpc::MessageFactory::create<lcz_rpc::ProtoRpcResponse>();
        resp->setId(req->rid());
        resp->setMsgType(lcz_rpc::MsgType::RSP_RPC_PROTO);
        resp->setRcode(lcz_rpc::RespCode::SUCCESS);

        lcz_rpc::proto::AddResponse add_resp;
        add_resp.set_result(add_req.num1() + add_req.num2());
        resp->setBody(add_resp.SerializeAsString());

        conn->send(resp);
        std::cout << "[server_proto] send result=" << add_resp.result() << std::endl;
    });

    std::thread([&]() { server.start(); }).detach();
    std::cout << "[server_proto] Protobuf 零拷贝服务已启动 (4 workers)" << std::endl;
    while (running) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    server.stop();
    return 0;
}
