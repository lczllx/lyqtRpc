#include "src/server/rpc_server.hpp"
#include <iostream>
#include <thread>
#include <cstdlib>
void add(const Json::Value &req, Json::Value &resp)
{
    int num1 = req["num1"].asInt();
    int num2 = req["num2"].asInt();
    resp = num1 + num2;
}
int main()
{
    // 支持环境变量覆盖，兼容 docker compose 容器间通信
    const char *env_provider_host = std::getenv("LCZ_PROVIDER_HOST");
    const char *env_provider_port = std::getenv("LCZ_PROVIDER_PORT");
    const char *env_registry_host = std::getenv("LCZ_REGISTRY_HOST");
    const char *env_registry_port = std::getenv("LCZ_REGISTRY_PORT");

    std::string provider_host = env_provider_host ? env_provider_host : "127.0.0.1";
    int provider_port = env_provider_port ? std::stoi(env_provider_port) : 8889;
    std::string registry_host = env_registry_host ? env_registry_host : "127.0.0.1";
    int registry_port = env_registry_port ? std::stoi(env_registry_port) : 8080;

    std::cout << "=== RPC 服务提供者启动（test1）===" << std::endl;
    std::cout << "服务地址: " << provider_host << ":" << provider_port << std::endl;
    std::cout << "注册中心: " << registry_host << ":" << registry_port << std::endl;
    std::cout << "提供服务: add(num1:int, num2:int) -> int" << std::endl;
    std::cout << "心跳间隔: 10s（Provider -> Registry）" << std::endl;
    std::cout << "=================================" << std::endl;

    std::unique_ptr<lcz_rpc::server::ServiceFactory> req_factory(new lcz_rpc::server::ServiceFactory());
    req_factory->setMethodName("add");
    req_factory->setParamdescribe("num1", lcz_rpc::server::ValType::INTEGRAL);
    req_factory->setParamdescribe("num2", lcz_rpc::server::ValType::INTEGRAL);
    req_factory->setReturntype(lcz_rpc::server::ValType::INTEGRAL);
    req_factory->setServiceCallback(add);

    lcz_rpc::server::RpcServer server(
        lcz_rpc::HostInfo(provider_host, provider_port),
        true,
        lcz_rpc::HostInfo(registry_host, registry_port));
    server.registerMethod(req_factory->build());

    server.start();
    return 0;
}