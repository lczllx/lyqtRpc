#pragma once
// =============================================================================
// gateway_handler.hpp — HTTP ↔ RPC 协议转换（Phase 3：协议转换）
// =============================================================================
// 持有 RpcClient 实例，把 HTTP JSON body 转成 Proto EchoRequest → 调 RPC 后端 →
// Proto EchoResponse → 转回 HTTP JSON 返回。
//
// 依赖关系：直接复用 lyqtRpc/rpc/src/client/rpc_client.hpp，零改动。
// 熔断器由 RpcClient 内部的 CircuitBreaker 自动处理，网关侧无需介入。
// JSON ↔ Protobuf 互转使用 google::protobuf::util 官方工具，无需手写。
//
// CMake target_include_directories 已设置：
//   rpc/src/  → #include "client/rpc_client.hpp"
//   rpc/build/ → #include "rpc_envelope.pb.h"
//   gateway/src/ → #include "http_server.hpp" / "http_router.hpp"
// =============================================================================
#include "client/rpc_client.hpp"
#include "http_server.hpp"
#include "http_router.hpp"
#include "rpc_envelope.pb.h"
#include <google/protobuf/util/json_util.h>

namespace lcz_gateway
{

    class GatewayHandler
    {
    public:
        // use_discover: 是否走 etcd 服务发现（与 RpcClient 构造函数语义一致）
        // server_ip / server_port: 后端 RPC 地址
        GatewayHandler(bool use_discover, const std::string &server_ip, int server_port)
            : _client(use_discover, server_ip, use_discover ? server_port : server_port) {}

        // ---- HTTP JSON → Proto EchoRequest → RPC echo → Proto EchoResponse → HTTP JSON ----
        void handleEcho(const HttpReq &req, HttpResp *resp)
        {
            lcz_rpc::proto::EchoRequest proto_req;
            lcz_rpc::proto::EchoResponse proto_resp;

            // JsonStringToMessage：google 官方 JSON→Proto 工具，一次调用完成字段映射
            auto status = google::protobuf::util::JsonStringToMessage(req.body, &proto_req);
            if (!status.ok())
            {
                resp->status = 400;
                resp->setBody(R"({"error":"invalid JSON: ")" +
                              status.message().as_string() + "\"}");
                return;
            }

            // RpcClient::call_proto：模板参数指定 Req/Resp 类型，编译器自动生成
            // ProtoRpcRequest envelope 的序列化/反序列化代码。内部走 LV 变长帧协议，
            // 自带超时控制（默认 5s）、熔断器（CircuitBreaker）、BACKOFF 退避重试、
            // 以及客户端指标埋点（rpc_client_requests_total / rpc_client_latency_us）。
            // 这条调用路径和 benchmark_client 压测的是完全相同的代码。
            bool ok = _client.call_proto<lcz_rpc::proto::EchoRequest,
                                         lcz_rpc::proto::EchoResponse>(
                "echo", proto_req, &proto_resp);

            if (ok)
            {
                std::string json_out;
                google::protobuf::util::MessageToJsonString(proto_resp, &json_out);
                resp->setBody(json_out);
            }
            else
            {
                resp->status = 502; // Bad Gateway：后端可达但调用失败
                resp->setBody(R"({"error":"backend call failed","method":"echo"})");
            }
        }

        // 返回可直接塞给 HttpRouter::addRoute 的 handler
        RouteHandler echoRoute()
        {
            return [this](const HttpReq &r, HttpResp *w)
            { handleEcho(r, w); };
        }

    private:
        lcz_rpc::client::RpcClient _client; // 内部自带熔断器 CircuitBreaker
    };

} // namespace lcz_gateway
