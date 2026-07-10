// SHM + Protobuf 零拷贝 Client 示例
#include "src/client/shm_client_proto.hpp"
#include "src/general/message.hpp"
#include "src/general/detail.hpp"
#include <iostream>
#include <mutex>
#include <condition_variable>

int main() {
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);
    lcz_rpc::ShmClientProto client("lcz_shm_proto_notify");

    std::mutex mtx; std::condition_variable cv;
    std::string expected_rid; bool got_resp = false;

    client.setMessageCallback([&](const lcz_rpc::BaseConnection::ptr&,
                                   lcz_rpc::BaseMessage::ptr& msg) {
        auto resp = std::dynamic_pointer_cast<lcz_rpc::ProtoRpcResponse>(msg);
        if (!resp) return;
        lcz_rpc::proto::AddResponse add_resp;
        add_resp.ParseFromString(resp->body());
        std::lock_guard<std::mutex> lk(mtx);
        std::cout << "[client_proto] recv id=" << resp->rid()
                  << " result=" << add_resp.result() << std::endl;
        if (resp->rid() == expected_rid) { got_resp = true; cv.notify_one(); }
    });

    client.connect();
    std::cout << "[client_proto] 已连接" << std::endl;

    for (int i = 1; i <= 3; ++i) {
        auto req = lcz_rpc::MessageFactory::create<lcz_rpc::ProtoRpcRequest>();
        req->setId(uuid());
        req->setMsgType(lcz_rpc::MsgType::REQ_RPC_PROTO);
        req->setMethod("add");

        lcz_rpc::proto::AddRequest add_req;
        add_req.set_num1(i * 10);
        add_req.set_num2(i);
        req->setBody(add_req.SerializeAsString());

        { std::lock_guard<std::mutex> lk(mtx); expected_rid = req->rid(); got_resp = false; }
        std::cout << "[client_proto] send " << (i*10) << "+" << i
                  << " id=" << req->rid() << std::endl;
        client.send(req);
        { std::unique_lock<std::mutex> lk(mtx); cv.wait(lk, [&]{ return got_resp; }); }
    }
    std::cout << "[client_proto] 完成" << std::endl;
    return 0;
}
