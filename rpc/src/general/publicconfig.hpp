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
        int failures = 0;                          // 当前连续失败次数
        int half_open = 0;                         // 半开已放行请求数
        int64_t opened_at = 0;                     // Unix 秒时间戳
    };
    // 熔断器默认限制参数
    struct CircuitConfig
    {
        int failure_threshold = 5;  // 连续失败几次 → OPEN
        int open_duration_sec = 30; // OPEN 持续多久 → HALF_OPEN
        int half_open_max_req = 1;  // 半开最多放几条探测
    };
}