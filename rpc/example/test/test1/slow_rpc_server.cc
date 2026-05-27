// 用于超时测试：add() 延迟 10 秒再返回，以便触发客户端超时
#include "src/server/rpc_server.hpp"
#include "src/general/detail.hpp"
#include <thread>
#include <chrono>

void add_slow(const Json::Value &req, Json::Value &resp)
{
    std::this_thread::sleep_for(std::chrono::seconds(10));
    int num1 = req["num1"].asInt();
    int num2 = req["num2"].asInt();
    resp = num1 + num2;
}

int main()
{
    const char *env_provider_host = std::getenv("LCZ_PROVIDER_HOST");
    const char *env_provider_port = std::getenv("LCZ_PROVIDER_PORT");
    const char *env_registry_host = std::getenv("LCZ_REGISTRY_HOST");
    const char *env_registry_port = std::getenv("LCZ_REGISTRY_PORT");

    std::string provider_host = env_provider_host ? env_provider_host : "127.0.0.1";
    int provider_port = env_provider_port ? std::stoi(env_provider_port) : 8889;
    std::string registry_host = env_registry_host ? env_registry_host : "127.0.0.1";
    int registry_port = env_registry_port ? std::stoi(env_registry_port) : 8080;

    std::unique_ptr<lcz_rpc::server::ServiceFactory> req_factory(new lcz_rpc::server::ServiceFactory());
    req_factory->setMethodName("add");
    req_factory->setParamdescribe("num1", lcz_rpc::server::ValType::INTEGRAL);
    req_factory->setParamdescribe("num2", lcz_rpc::server::ValType::INTEGRAL);
    req_factory->setReturntype(lcz_rpc::server::ValType::INTEGRAL);
    req_factory->setServiceCallback(add_slow);

    lcz_rpc::server::RpcServer server(
        lcz_rpc::HostInfo(provider_host, provider_port),
        true,
        lcz_rpc::HostInfo(registry_host, registry_port));
    server.registerMethod(req_factory->build());
    server.start();
    return 0;
}
