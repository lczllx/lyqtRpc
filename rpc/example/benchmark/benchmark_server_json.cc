#include "../../src/server/rpc_server.hpp"
#include "../../src/general/detail.hpp"
#include <chrono>
#include <thread>

// 简单的计算服务，用于性能测试（JSON 序列化，原版副本）
void add(const Json::Value &req, Json::Value &resp)
{
    int num1 = req["num1"].asInt();
    int num2 = req["num2"].asInt();
    resp = num1 + num2;
}

// 模拟耗时操作
void heavy_compute(const Json::Value &req, Json::Value &resp)
{
    int value = req["value"].asInt();
    int result = 0;
    for (int i = 0; i < value; ++i) {
        result += i;
    }
    resp = result;
}

// 空操作，测试框架开销
void echo(const Json::Value &req, Json::Value &resp)
{
    resp = req;
}

int main(int argc, char* argv[])
{
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);

    int port = 8889;
    bool enable_discover = false;
    int registry_port = 8080;

    if (argc > 1) {
        port = std::atoi(argv[1]);
    }
    if (argc > 2) {
        enable_discover = std::atoi(argv[2]) != 0;
    }
    if (argc > 3) {
        registry_port = std::atoi(argv[3]);
    }

    std::cout << "启动性能测试服务端（JSON 序列化，原版副本）..." << std::endl;
    std::cout << "端口: " << port << std::endl;
    std::cout << "服务发现: " << (enable_discover ? "启用" : "禁用") << std::endl;

    // 注册 add 服务
    {
        std::unique_ptr<lcz_rpc::server::ServiceFactory> factory(new lcz_rpc::server::ServiceFactory());
        factory->setMethodName("add");
        factory->setParamdescribe("num1", lcz_rpc::server::ValType::INTEGRAL);
        factory->setParamdescribe("num2", lcz_rpc::server::ValType::INTEGRAL);
        factory->setReturntype(lcz_rpc::server::ValType::INTEGRAL);
        factory->setServiceCallback(add);

        lcz_rpc::server::RpcServer server(
            lcz_rpc::HostInfo("127.0.0.1", port),
            enable_discover,
            lcz_rpc::HostInfo("127.0.0.1", registry_port)
        );
        server.registerMethod(factory->build());

        // 注册 echo 服务（空操作）
        {
            std::unique_ptr<lcz_rpc::server::ServiceFactory> echo_factory(new lcz_rpc::server::ServiceFactory());
            echo_factory->setMethodName("echo");
            echo_factory->setParamdescribe("data", lcz_rpc::server::ValType::OBJECT);
            echo_factory->setReturntype(lcz_rpc::server::ValType::OBJECT);
            echo_factory->setServiceCallback(echo);
            server.registerMethod(echo_factory->build());
        }

        // 注册 heavy_compute 服务（耗时操作）
        {
            std::unique_ptr<lcz_rpc::server::ServiceFactory> heavy_factory(new lcz_rpc::server::ServiceFactory());
            heavy_factory->setMethodName("heavy_compute");
            heavy_factory->setParamdescribe("value", lcz_rpc::server::ValType::INTEGRAL);
            heavy_factory->setReturntype(lcz_rpc::server::ValType::INTEGRAL);
            heavy_factory->setServiceCallback(heavy_compute);
            server.registerMethod(heavy_factory->build());
        }

        std::cout << "服务端启动成功，等待请求..." << std::endl;
        server.start();
    }

    return 0;
}
