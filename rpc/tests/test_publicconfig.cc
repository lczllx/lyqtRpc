// publicconfig 单元测试：RpcError 分类 / 重试·熔断·心跳配置默认值 / 主机工具函数
#include <gtest/gtest.h>
#include "src/general/publicconfig.hpp"

using lcz_rpc::RpcError;
using lcz_rpc::isRetryable;
using lcz_rpc::RetryConfig;
using lcz_rpc::CircuitConfig;
using lcz_rpc::HeartbeatConfig;
using lcz_rpc::HostInfo;
using lcz_rpc::HostDetail;
using lcz_rpc::hostKey;

// 瞬时故障可重试：TIMEOUT / CONN_CLOSED / CIRCUIT_OPEN / BACKOFF
TEST(PublicConfigTest, IsRetryableInstantFaults)
{
    EXPECT_TRUE(isRetryable(RpcError::TIMEOUT));
    EXPECT_TRUE(isRetryable(RpcError::CONN_CLOSED));
    EXPECT_TRUE(isRetryable(RpcError::CIRCUIT_OPEN));
    EXPECT_TRUE(isRetryable(RpcError::BACKOFF));
}

// 成功 / 业务错误不可重试
TEST(PublicConfigTest, IsRetryableNonRetryable)
{
    EXPECT_FALSE(isRetryable(RpcError::OK));
    EXPECT_FALSE(isRetryable(RpcError::SERVICE_ERROR));
}

// 重试配置默认值：3 次重试 + 全抖动退避 + 重试短超时 500ms
TEST(PublicConfigTest, RetryConfigDefaults)
{
    RetryConfig cfg;
    EXPECT_EQ(cfg.max_retries, 3);
    EXPECT_EQ(cfg.base_ms, 10);
    EXPECT_EQ(cfg.max_ms, 1000);
    EXPECT_EQ(cfg.retry_timeout_ms, 500);
}

// 熔断器默认参数
TEST(PublicConfigTest, CircuitConfigDefaults)
{
    CircuitConfig cfg;
    EXPECT_EQ(cfg.failure_threshold, 5);
    EXPECT_EQ(cfg.open_duration_sec, 30);
    EXPECT_EQ(cfg.half_open_max_req, 1);
}

// 心跳配置默认值
TEST(PublicConfigTest, HeartbeatConfigDefaults)
{
    HeartbeatConfig cfg;
    EXPECT_DOUBLE_EQ(cfg.check_interval_sec, 5.0);
    EXPECT_EQ(cfg.idle_timeout_sec, 15);
    EXPECT_EQ(cfg.heartbeat_interval_sec, 10);
}

// hostKey 拼接为 "ip:port"
TEST(PublicConfigTest, HostKeyFormat)
{
    EXPECT_EQ(hostKey(HostInfo("10.0.0.1", 8080)), "10.0.0.1:8080");
}

// HostDetail 默认构造与带参构造
TEST(PublicConfigTest, HostDetailCtor)
{
    HostDetail d;
    EXPECT_EQ(d.host, HostInfo());
    EXPECT_EQ(d.load, 0);

    HostDetail d2(HostInfo("10.0.0.2", 9090), 7);
    EXPECT_EQ(d2.host.first, "10.0.0.2");
    EXPECT_EQ(d2.host.second, 9090);
    EXPECT_EQ(d2.load, 7);
}
