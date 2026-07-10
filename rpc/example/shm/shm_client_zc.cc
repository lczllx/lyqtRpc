// ==================================================================
// shm_client_zc.cc — FlatBuffers 零拷贝 SHM Client 示例
// ==================================================================
#include "src/client/shm_client_zc.hpp"
#include "src/general/message.hpp"
#include "src/general/detail.hpp"
#include <iostream>
#include <mutex>
#include <condition_variable>

int main() {
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);
    lcz_rpc::ShmClientZc client("lcz_shm_zc", "lcz_shm_zc_notify");

    std::mutex mtx;
    std::condition_variable cv;
    std::string expected_rid;
    int result = 0;
    bool got_response = false;

    client.setMessageCallback([&](const lcz_rpc::BaseConnection::ptr&,
                                   lcz_rpc::BaseMessage::ptr& msg) {
        auto resp = std::dynamic_pointer_cast<lcz_rpc::RpcResponse>(msg);
        if (!resp) return;
        std::lock_guard<std::mutex> lk(mtx);
        std::cout << "[client_zc] recv response id=" << resp->rid()
                  << " result=" << resp->result().asInt() << std::endl;
        if (resp->rid() == expected_rid) {
            result = resp->result().asInt();
            got_response = true;
            cv.notify_one();
        }
    });

    client.connect();
    std::cout << "[client_zc] 零拷贝客户端已连接" << std::endl;

    for (int i = 1; i <= 3; ++i) {
        auto req = lcz_rpc::MessageFactory::create<lcz_rpc::RpcRequest>();
        req->setId(uuid());
        req->setMsgType(lcz_rpc::MsgType::REQ_RPC);
        req->setMethod("add");
        req->setTraceId(uuid());
        Json::Value params;
        params["num1"] = i * 10;
        params["num2"] = i;
        req->setParams(params);

        {
            std::lock_guard<std::mutex> lk(mtx);
            expected_rid = req->rid();
            got_response = false;
        }

        std::cout << "[client_zc] send " << (i * 10) << "+" << i
                  << " id=" << req->rid() << std::endl;
        if (!client.send(req)) {
            std::cerr << "[client_zc] send failed" << std::endl;
            continue;
        }

        {
            std::unique_lock<std::mutex> lk(mtx);
            cv.wait(lk, [&]{ return got_response; });
        }
    }

    std::cout << "[client_zc] 完成" << std::endl;
    return 0;
}
