#pragma once
#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
namespace lcz_rpc
{
    typedef std::pair<std::string, int32_t> HostInfo; // 主机信息

    // 心跳配置结构体：检查频率、空闲超时、心跳间隔
    struct HeartbeatConfig
    {
        double check_interval_sec = 5.0; // 检查频率：每5秒扫描一次
        int idle_timeout_sec = 15;       // 空闲超时：15秒没收到心跳则视为离线
        int heartbeat_interval_sec = 10; // 心跳间隔：提供者每10秒发一次心跳
    };
    // 主机详情结构体：主机地址 + 负载值
    struct HostDetail
    {
        HostInfo host;
        int load = 0;
        HostDetail(const HostInfo &host, int load) : host(host), load(load) {}
        HostDetail() : host(HostInfo()), load(0) {}
    };
    static std::string hostKey(const HostInfo &host)
    {
        return host.first + ":" + std::to_string(host.second);
    }
    // 熔断器阶段枚举
    enum class CircuitState : uint8_t
    {
        CLOSED = 0,   // 关闭
        OPEN = 1,     // 打开
        HALF_OPEN = 2 // 半开
    };

    // 熔断器当前状态结果
    struct CircuitStatus
    {
        CircuitState state = CircuitState::CLOSED; // 熔断器状态
        int failures = 0;                          // 滑动窗口内失败次数
        int successes = 0;                         // 滑动窗口内成功次数
        int half_open = 0;                         // 半开已放行请求数
        int64_t opened_at = 0;                     // Unix 秒时间戳
    };
    // 熔断器默认限制参数（滑动窗口失败率）
    struct CircuitConfig
    {
        int window_size = 20;           // 滑动窗口保留最近多少次调用结果
        int min_calls = 10;             // 窗口内样本数达到该值才评估失败率（小样本不熔断）
        int failure_ratio_percent = 50; // 失败率 >= 该百分比 → OPEN
        int open_duration_sec = 30;     // OPEN 持续多久 → HALF_OPEN
        int half_open_max_req = 1;      // 半开最多放几条探测
    };
    // RPC 调用失败原因分类：区分「可重试的瞬时故障」与「重试也白搭的业务错误」，
    // 供请求层重试循环判定是否指数退避重试。
    enum class RpcError : uint8_t
    {
        OK = 0,           // 成功
        TIMEOUT,          // 等待响应超时 → 可重试
        CONN_CLOSED,      // 连接未建立/已断开 → 可重试
        CIRCUIT_OPEN,     // 熔断器打开拒绝 → 可重试（换 host 绕过）
        BACKOFF,          // 服务端限流 / 本地发送队列满 → 可重试
        SERVICE_ERROR,    // 服务端业务/协议错误（SERVICE_NOT_FOUND/INVALID_PARAMS/PARSE_FAILED 等）→ 不可重试
    };
    // 判断错误是否可重试（瞬时故障可重试，业务错误不可重试）
    static inline bool isRetryable(RpcError e)
    {
        return e == RpcError::TIMEOUT || e == RpcError::CONN_CLOSED ||
               e == RpcError::CIRCUIT_OPEN || e == RpcError::BACKOFF;
    }
    // 指数退避重试配置：退避序列 delay = random(0, min(max_ms, base_ms * 2^attempt))
    struct RetryConfig
    {
        int max_retries = 3;         // 重试次数（不含首次），共最多 1+3=4 次尝试
        int base_ms = 10;            // 首次退避基数
        int max_ms = 1000;           // 退避封顶
        int retry_timeout_ms = 500;  // 重试尝试（attempt>0）的单次超时；首次仍用默认 5s
    };
}