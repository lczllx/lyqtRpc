#pragma once
// =============================================================================
// diagose_handler.hpp — /diagnose 排障端点（Phase 6：全链路追踪与排障）
// =============================================================================
// 一次 curl http://localhost:8080/diagnose 返回：
//   - 限流器余量（剩余令牌 / 速率 / 桶容量）
//   - 熔断器状态（每个 method×host 的 OPEN/CLOSED/HALF_OPEN）
//
// 不调 LLM（Phase 6 基础版），纯本地从 Registry 读指标快照 + TokenBucket getter。
// 诊断信息完全来自进程内已有数据，不发起额外网络请求。
//
// 与 benchmark_server 的诊断能力对比：
//   benchmark_server 只能在 /metrics 导出的数值里人工排查；
//   gateway 的 /diagnose 把治理状态聚合为一份 JSON，一次 curl 定位问题。
// =============================================================================
#include "http_server.hpp"
#include "http_router.hpp"
#include "../../rpc/src/general/metrics.hpp"
#include "../../rpc/src/general/rate_limiter.hpp"
#include <sstream>

namespace lcz_gateway
{

    using namespace lcz_rpc::metrics;

    class DiagnoseHandler
    {
    public:
        DiagnoseHandler(lcz_rpc::TokenBucket &limiter) : _limiter(limiter) {}

        RouteHandler route()
        {
            return [this](const HttpReq &, HttpResp *resp)
            {
                std::ostringstream json;
                json << "{";

                // ---- 限流器状态 ----
                // 从 MetricHooks 已写入的 token_bucket_available gauge 中读数，
                // 加上 TokenBucket 自身的配置参数（rate/burst）
                json << "\"rate_limiter\":{";
                Labels tok_ls = {{"service", "default"}};
                auto &gauge = METRICS_GAUGE("token_bucket_available",
                                            "Token bucket available tokens", tok_ls);
                json << "\"tokens\":" << gauge.value() << ",";
                json << "\"rate\":" << _limiter.getRate() << ",";
                json << "\"burst\":" << _limiter.getBurst();
                json << "},";

                // ---- 熔断器状态 ----
                // 设计取舍：不在 DiagnoseHandler 里维护一份 breaker 列表的引用
                // （避免侵入 NodeBreaker/CircuitBreaker 的内部数据结构），
                // 而是直接从 Prometheus 导出文本中 grep——
                // MetricsServer 每 15s 刮一次必定包含最新的 circuit_breaker_state，
                // 这里拿到的和 curl :9091/metrics 看到的完全一致。
                // state 值含义：0=CLOSED（正常）, 1=OPEN（熔断中）, 2=HALF_OPEN（探测）
                json << "\"circuit_breakers\":[";
                std::string body = Registry::instance().exportText();
                size_t pos = 0;
                bool first = true;
                std::string search = "circuit_breaker_state{";
                while ((pos = body.find(search, pos)) != std::string::npos)
                {
                    size_t end = body.find('\n', pos);
                    if (end == std::string::npos)
                        break;
                    std::string line = body.substr(pos, end - pos);
                    if (!first)
                        json << ",";

                    size_t m = line.find("method=\"");
                    size_t h = line.find("host=\"");
                    if (m != std::string::npos && h != std::string::npos)
                    {
                        size_t me = line.find('"', m + 8);
                        size_t he = line.find('"', h + 6);
                        std::string method = line.substr(m + 8, me - m - 8);
                        std::string host = line.substr(h + 6, he - h - 6);
                        size_t sp = line.rfind(' ');
                        std::string state = (sp != std::string::npos)
                                                ? line.substr(sp + 1)
                                                : "?";
                        json << "{\"method\":\"" << method << "\","
                             << "\"host\":\"" << host << "\","
                             << "\"state\":" << state << "}";
                    }
                    first = false;
                    pos = end + 1;
                }
                json << "]";

                json << "}";
                resp->setBody(json.str());
            };
        }

    private:
        lcz_rpc::TokenBucket &_limiter;
    };

} // namespace lcz_gateway
