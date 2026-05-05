// 熔断器测试：直连模式循环调用，通过 server 返回错误触发熔断，然后恢复验证
#include "src/client/rpc_client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>

std::atomic<bool> running{true};

void onSignal(int) { running = false; }

int main()
{
    signal(SIGINT, onSignal);
    signal(SIGTERM, onSignal);

    std::cout << "=== 熔断器测试客户端 ===" << std::endl;
    std::cout << "直连模式 -> 127.0.0.1:8889" << std::endl;
    std::cout << "连续调用 add(1,2)，1次/秒" << std::endl;
    std::cout << "================================" << std::endl;

    lcz_rpc::client::RpcClient client(false, "127.0.0.1", 8889);

    int seq = 0;
    while (running)
    {
        seq++;
        Json::Value params, result;
        params["num1"] = 1;
        params["num2"] = 2;

        bool ok = client.call("add", params, result);
        if (ok)
            std::cout << "[" << seq << "] OK: " << result.asInt() << std::endl;
        else
            std::cout << "[" << seq << "] FAIL" << std::endl;

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "结束，共 " << seq << " 次调用" << std::endl;
    return 0;
}
