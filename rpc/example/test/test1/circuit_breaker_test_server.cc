// 熔断器测试 server：前3次正常 → 5次慢响应(触发客户端5s超时) → 之后正常（验证恢复）
#include "src/server/rpc_server.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>

std::atomic<int> call_count{0};

void add(const Json::Value &req, Json::Value &resp)
{
    int n = call_count.fetch_add(1) + 1;
    int num1 = req["num1"].asInt();
    int num2 = req["num2"].asInt();

    if (n <= 3)
    {
        resp = num1 + num2;  // 正常响应
    }
    else if (n <= 8)
    {
        // 慢响应：sleep 6s，客户端默认超时5s → 触发超时
        std::this_thread::sleep_for(std::chrono::seconds(6));
        resp = num1 + num2;
    }
    else
    {
        resp = num1 + num2;  // 恢复正常
    }
}

int main()
{
    std::cout << "=== 熔断器测试 Server ===" << std::endl;
    std::cout << "前3次正常 → 5次慢响应(6s, 触发客户端5s超时) → 之后恢复正常" << std::endl;

    std::unique_ptr<lcz_rpc::server::ServiceFactory> factory(new lcz_rpc::server::ServiceFactory());
    factory->setMethodName("add");
    factory->setParamdescribe("num1", lcz_rpc::server::ValType::INTEGRAL);
    factory->setParamdescribe("num2", lcz_rpc::server::ValType::INTEGRAL);
    factory->setReturntype(lcz_rpc::server::ValType::INTEGRAL);
    factory->setServiceCallback(add);

    lcz_rpc::server::RpcServer server(lcz_rpc::HostInfo("127.0.0.1", 8889));
    server.registerMethod(factory->build());
    server.start();

    std::cout << "Server 已启动" << std::endl;
    while (true) std::this_thread::sleep_for(std::chrono::seconds(60));
    return 0;
}
