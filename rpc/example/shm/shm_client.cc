// ==================================================================
// shm_client.cc — Phase 4: ShmClient 完整示例（轮询模式）
// 编译: cd rpc && g++ -std=c++17 -I. -Ibuild example/shm/shm_client.cc \
//         src/general/shm_channel.cc -lpthread -o build/bin/shm_client
// ==================================================================
#include "src/client/shm_client.hpp"
#include "src/general/message.hpp"
#include "src/general/detail.hpp"   // uuid()
#include <iostream>
#include <mutex>
#include <condition_variable>

int main() {
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);
    lcz_rpc::ShmClient client("lcz_shm_notify");

    // 收响应：匹配 rid，用 condition_variable 等通知
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
        if (resp->rid() == expected_rid) {
            result = resp->result().asInt();
            got_response = true;
            cv.notify_one();
            std::cout << "[client] recv response rid=" << resp->rid()
                      << " result=" << result << std::endl;
        }
    });

    client.connect();
    std::cout << "[client] 已连接" << std::endl;

    // 发 3 个请求
    for (int i = 1; i <= 3; ++i) {
        auto req = lcz_rpc::MessageFactory::create<lcz_rpc::RpcRequest>();
        req->setId(uuid());
        req->setMsgType(lcz_rpc::MsgType::REQ_RPC);
        req->setMethod("add");
        req->setTraceId(uuid());
        req->setSpanId("0");
        Json::Value params;
        params["num1"] = i * 10;
        params["num2"] = i;
        req->setParams(params);

        {
            std::lock_guard<std::mutex> lk(mtx);
            expected_rid = req->rid();
            got_response = false;
        }

        std::cout << "[client] send " << (i*10) << "+" << i
                  << " rid=" << req->rid() << std::endl;
        if (!client.send(req)) {
            std::cerr << "[client] send failed" << std::endl;
            continue;
        }

        // 等响应（后台 epoll 线程收到通知 → _cb_message 回调 → cv.notify）
        {
            std::unique_lock<std::mutex> lk(mtx);
            cv.wait(lk, [&]{ return got_response; });
        }
    }

    std::cout << "[client] 完成" << std::endl;
    return 0;
}
