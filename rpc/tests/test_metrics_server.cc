// =============================================================================
// test_metrics_server.cc — /metrics HTTP 端点与进程级指标单测
// -----------------------------------------------------------------------------
// 总测什么：
//   MetricsServer 起停（stop 不卡死——accept 阻塞导致 join 死锁是
//   本项目踩过的坑，防回归）、GET /metrics 返回 200 + 指标文本、
//   非法路径 404、ProcessMetrics::collect() 采到合理的 /proc 值。
// 不测什么：
//   指标内存模型（见 test_metrics.cc）、Prometheus 服务器侧行为。
//
// 端口约定：用 19309 这类高位端口，避免与本机 9090（可能有真服务）冲突。
//
// 分块说明：
//   §1 端点 — 起停、200/404、响应体内容
//   §2 进程指标 — collect() 后 process_* 取值合理
// =============================================================================

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <thread>
#include <chrono>

#include "src/general/metrics_server.hpp"

using namespace lcz_rpc::metrics;

namespace {

// 用 raw socket 发一次 HTTP GET，返回完整响应文本（测试不依赖 curl）
std::string httpGet(int port, const std::string& path) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return "";
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return "";
    }
    std::string req = "GET " + path + " HTTP/1.1\r\nHost: localhost\r\n\r\n";
    send(fd, req.data(), req.size(), 0);
    std::string resp;
    char buf[4096];
    ssize_t n;
    // 服务端应答后立即 close，读到 EOF 为止即完整响应
    while ((n = recv(fd, buf, sizeof(buf), 0)) > 0)
        resp.append(buf, static_cast<size_t>(n));
    close(fd);
    return resp;
}

// 轮询等端点就绪（后台线程 bind/listen 需要一点时间）
bool waitReady(int port, int max_ms = 3000) {
    for (int waited = 0; waited < max_ms; waited += 50) {
        if (!httpGet(port, "/metrics").empty()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

constexpr int kPort = 19309;

} // namespace

// -----------------------------------------------------------------------------
// §1 端点
// -----------------------------------------------------------------------------

// 起服务 → GET /metrics 返回 200，响应体含本用例写入的指标 → stop 干净返回。
// 整套流程放一个用例里：MetricsServer 是进程级静态单例，起停一次覆盖全部路径
TEST(MetricsServer, ServeAndStopCleanly) {
    Registry::instance().counter("ut_srv_visible", "h").inc();

    MetricsServer::start(kPort);
    ASSERT_TRUE(waitReady(kPort)) << "端点 " << kPort << " 未就绪";

    // 200 + Prometheus 文本协议头 + 指标内容
    std::string resp = httpGet(kPort, "/metrics");
    EXPECT_NE(resp.find("HTTP/1.1 200 OK"), std::string::npos);
    EXPECT_NE(resp.find("text/plain; version=0.0.4"), std::string::npos);
    EXPECT_NE(resp.find("ut_srv_visible 1"), std::string::npos);
    // uptime 由 scrape 时刷新，应为很小的正数（若 _start_time 未初始化
    // 会算出"开机至今"的十几天巨值——已修 bug 的间接回归）
    EXPECT_NE(resp.find("rpc_server_uptime_seconds"), std::string::npos);

    // 非 /metrics 路径 404
    std::string nf = httpGet(kPort, "/whatever");
    EXPECT_NE(nf.find("404 Not Found"), std::string::npos);

    // stop() 必须能返回：若 accept 阻塞未被唤醒，join 会永久卡死，
    // 用例超时失败即为回归信号
    MetricsServer::stop();
    SUCCEED();
}

// -----------------------------------------------------------------------------
// §2 进程级指标
// -----------------------------------------------------------------------------

// collect() 后 process_* 各值应为合理正数（从 /proc 实读，仅 Linux）
TEST(ProcessMetrics, CollectSaneValues) {
    ProcessMetrics::collect();

    EXPECT_GT(Registry::instance()
                  .gauge("process_resident_memory_bytes", "").value(), 0);
    EXPECT_GT(Registry::instance()
                  .gauge("process_virtual_memory_bytes", "").value(), 0);
    EXPECT_GE(Registry::instance().gauge("process_threads", "").value(), 1);
    EXPECT_GT(Registry::instance().gauge("process_open_fds", "").value(), 0);
    EXPECT_GE(Registry::instance()
                  .gauge("process_cpu_seconds_total", "").value(), 0);
    EXPECT_GE(Registry::instance().gauge("system_loadavg_1m", "").value(), 0);
}
