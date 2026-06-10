#include "../../src/server/rpc_server.hpp"
#include "../../src/general/message.hpp"
#include "rpc_envelope.pb.h"
#include <chrono>
#include <thread>

using namespace lcz_rpc::proto;

// 纯 Proto 处理：add
static void add_proto(const lcz_rpc::BaseConnection::ptr&, const AddRequest& req, AddResponse* resp) {
    resp->set_result(req.num1() + req.num2());
}

// 纯 Proto 处理：echo
static void echo_proto(const lcz_rpc::BaseConnection::ptr&, const EchoRequest& req, EchoResponse* resp) {
    resp->set_data(req.data());
}

// 纯 Proto 处理：heavy_compute
static void heavy_compute_proto(const lcz_rpc::BaseConnection::ptr&, const HeavyRequest& req, HeavyResponse* resp) {
    int value = req.value();
    int result = 0;
    for (int i = 0; i < value; ++i) result += i;
    resp->set_result(result);
}

int main(int argc, char* argv[])
{
    //lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);

    int port = 8889;
    bool enable_discover = false;
    int registry_port = 8080;
    int rate_limit = 0; // 0 表示不限流

    if (argc > 1) port = std::atoi(argv[1]);
    if (argc > 2) enable_discover = std::atoi(argv[2]) != 0;
    if (argc > 3) registry_port = std::atoi(argv[3]);
    if (argc > 4) rate_limit = std::atoi(argv[4]);

    std::cout << "启动性能测试服务端（默认序列化: Protobuf）..." << std::endl;
    std::cout << "端口: " << port << std::endl;
    std::cout << "服务发现: " << (enable_discover ? "启用" : "禁用") << std::endl;
    if (rate_limit > 0) std::cout << "限流: " << rate_limit << " req/s" << std::endl;

    lcz_rpc::server::RpcServer server(
        lcz_rpc::HostInfo("127.0.0.1", port),
        enable_discover,
        lcz_rpc::HostInfo("127.0.0.1", registry_port)
    );
    server.registerProtoHandler<AddRequest, AddResponse>("add", add_proto);
    server.registerProtoHandler<EchoRequest, EchoResponse>("echo", echo_proto);
    server.registerProtoHandler<HeavyRequest, HeavyResponse>("heavy_compute", heavy_compute_proto);

    if (rate_limit > 0) server.setRateLimiter(rate_limit, rate_limit * 2);

    std::cout << "服务端启动成功，等待请求..." << std::endl;
    server.start();

    return 0;
}
