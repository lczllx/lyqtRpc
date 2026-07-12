// SHM + Protobuf 零拷贝 压测 Server
#include "src/server/shm_server_proto.hpp"
#include "src/general/message.hpp"
#include "src/general/log_system/lcz_log.h"
#include <iostream>
#include <thread>
#include <signal.h>

std::atomic<bool> running{true};

int main() {
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::ERROR);
    signal(SIGINT,  [](int){ running = false; });
    signal(SIGTERM, [](int){ running = false; });

    lcz_rpc::ShmServerProto server("lcz_shm_proto_bench_notify", "lcz_shm_proto_bench",
                                    1*1024*1024, 1*1024*1024, 32, 4);

    server.setMessageCallback([](const lcz_rpc::BaseConnection::ptr& conn,
                                  lcz_rpc::BaseMessage::ptr& msg) {
        auto req = std::dynamic_pointer_cast<lcz_rpc::ProtoRpcRequest>(msg);
        if (!req) return;

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
    });

    std::thread([&]() { server.start(); }).detach();
    std::cout << "[shm_bench_proto_server] Protobuf 零拷贝服务已启动" << std::endl;
    while (running) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    server.stop();
    return 0;
}
