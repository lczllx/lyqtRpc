// =============================================================================
// test_metrics.cc — Prometheus 指标内存模型单测
// -----------------------------------------------------------------------------
// 总测什么：
//   formatLabels/mergeLabels 边界、Counter/Gauge/Histogram 读写语义、
//   Histogram 分桶边界与累计导出、Registry 懒注册与序列区分、
//   exportText 格式（三个已修 bug 各配一条回归：科学计数法截断/
//   _total Gauge 的 TYPE 修正/le 标签合并）、MetricHooks 预注册、并发无丢失。
// 不测什么：
//   /metrics HTTP 端点（见 test_metrics_server.cc）、PromQL 查询侧行为。
//
// 注意：Registry 是进程级单例，用例间共享状态——
//   每个用例使用独一无二的指标名（ut_ 前缀 + 用例名），避免互相污染。
//
// 分块说明：
//   §1 标签格式化 — formatLabels / mergeLabels
//   §2 三类指标   — Counter / Gauge / Histogram 基础语义
//   §3 分桶边界   — 恰好压线 / 越界只进 +Inf
//   §4 Registry   — 懒注册、同一序列同一对象、标签区分序列
//   §5 导出格式   — 累计桶、TYPE 修正、精度（bug 回归）
//   §6 Hooks      — 错误计数预注册、熔断/限流埋点
//   §7 并发       — 多线程 inc 无丢失
// =============================================================================

#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include "src/general/metrics.hpp"
#include "src/general/metrics_hooks.hpp"

using namespace lcz_rpc::metrics;

// -----------------------------------------------------------------------------
// §1 标签格式化
// -----------------------------------------------------------------------------

// 空标签返回空串（导出时指标名后直接跟值）
TEST(MetricsLabels, FormatEmpty) {
    EXPECT_EQ(formatLabels({}), "");
}

// 单标签与多标签的花括号格式
TEST(MetricsLabels, FormatOneAndMany) {
    EXPECT_EQ(formatLabels({{"method", "echo"}}), "{method=\"echo\"}");
    EXPECT_EQ(formatLabels({{"a", "1"}, {"b", "2"}}), "{a=\"1\",b=\"2\"}");
}

// mergeLabels：已有标签时并入大括号内；为空时新建大括号
// （若不合并会输出非法的 {a="1"}{le="10"} 两组括号——历史 bug 回归）
TEST(MetricsLabels, MergeIntoExisting) {
    EXPECT_EQ(mergeLabels("{a=\"1\"}", "le=\"10\""), "{a=\"1\",le=\"10\"}");
    EXPECT_EQ(mergeLabels("", "le=\"10\""), "{le=\"10\"}");
}

// -----------------------------------------------------------------------------
// §2 三类指标基础语义
// -----------------------------------------------------------------------------

// Counter 只增：inc 加一、add 加 n
TEST(MetricsCounter, IncAndAdd) {
    Counter c("ut_counter_basic", "h");
    c.inc();
    c.add(41);
    EXPECT_EQ(c.value(), 42);
}

// Gauge 可增可减可赋值
TEST(MetricsGauge, SetIncDec) {
    Gauge g("ut_gauge_basic", "h");
    g.set(10.5);
    g.inc();
    g.dec();
    g.dec();
    EXPECT_DOUBLE_EQ(g.value(), 9.5);
}

// Histogram：count/sum 与观测一致
TEST(MetricsHistogram, CountAndSum) {
    Histogram h("ut_histo_basic", "h");
    h.observe(5);
    h.observe(20);
    EXPECT_EQ(h.count(), 2);
    EXPECT_DOUBLE_EQ(h.sum(), 25.0);
}

// -----------------------------------------------------------------------------
// §3 分桶边界
// -----------------------------------------------------------------------------

// 恰好等于桶上界时计入该桶（le 是 "less or equal"）
TEST(MetricsHistogram, BoundaryExactlyOnBucket) {
    Histogram h("ut_histo_boundary", "h");
    h.observe(10);   // == 第一个桶上界，进 le=10
    h.observe(10.1); // 刚超一点，进 le=25
    EXPECT_EQ(h.bucket(0), 1); // le=10
    EXPECT_EQ(h.bucket(1), 1); // le=25
}

// 超过最大桶(50000)只计入 count/sum，不进任何显式桶（导出时体现在 +Inf）
TEST(MetricsHistogram, OverflowGoesOnlyToInf) {
    Histogram h("ut_histo_overflow", "h");
    h.observe(60000);
    EXPECT_EQ(h.count(), 1);
    for (size_t i = 0; i < Histogram::bucketCount(); ++i)
        EXPECT_EQ(h.bucket(i), 0) << "bucket " << i;
}

// -----------------------------------------------------------------------------
// §4 Registry 懒注册
// -----------------------------------------------------------------------------

// 同名同标签两次获取返回同一个对象（值累计在一处）
TEST(MetricsRegistry, SameSeriesSameObject) {
    auto& a = Registry::instance().counter("ut_reg_same", "h", {{"m", "x"}});
    auto& b = Registry::instance().counter("ut_reg_same", "h", {{"m", "x"}});
    EXPECT_EQ(&a, &b);
    a.inc();
    EXPECT_EQ(b.value(), 1);
}

// 同名不同标签是两条独立序列
TEST(MetricsRegistry, DifferentLabelsDifferentSeries) {
    auto& a = Registry::instance().counter("ut_reg_diff", "h", {{"m", "x"}});
    auto& b = Registry::instance().counter("ut_reg_diff", "h", {{"m", "y"}});
    EXPECT_NE(&a, &b);
    a.inc();
    EXPECT_EQ(b.value(), 0);
}

// -----------------------------------------------------------------------------
// §5 导出格式（含三个已修 bug 的回归）
// -----------------------------------------------------------------------------

// 累计桶语义：观测 5/20/30 → le=10 是 1、le=25 是 2、le=50 是 3、+Inf==count
TEST(MetricsExport, HistogramCumulativeBuckets) {
    auto& h = Registry::instance().histogram("ut_exp_cum", "h", {{"m", "e"}});
    h.observe(5);
    h.observe(20);
    h.observe(30);
    std::string out = Registry::instance().exportText();
    EXPECT_NE(out.find("ut_exp_cum_bucket{m=\"e\",le=\"10\"} 1"), std::string::npos) << out;
    EXPECT_NE(out.find("ut_exp_cum_bucket{m=\"e\",le=\"25\"} 2"), std::string::npos);
    EXPECT_NE(out.find("ut_exp_cum_bucket{m=\"e\",le=\"50\"} 3"), std::string::npos);
    EXPECT_NE(out.find("ut_exp_cum_bucket{m=\"e\",le=\"+Inf\"} 3"), std::string::npos);
    EXPECT_NE(out.find("ut_exp_cum_count{m=\"e\"} 3"), std::string::npos);
}

// bug 回归①：_total 后缀的 Gauge 导出 TYPE 必须是 counter
// （OpenMetrics 规定 _total 专属 counter，否则 promtool lint 告警）
TEST(MetricsExport, TotalSuffixGaugeExportsAsCounter) {
    Registry::instance().gauge("ut_exp_cpu_seconds_total", "h").set(1.5);
    std::string out = Registry::instance().exportText();
    EXPECT_NE(out.find("# TYPE ut_exp_cpu_seconds_total counter"), std::string::npos);
    // 普通 gauge 不受影响
    Registry::instance().gauge("ut_exp_plain_gauge", "h").set(1);
    out = Registry::instance().exportText();
    EXPECT_NE(out.find("# TYPE ut_exp_plain_gauge gauge"), std::string::npos);
}

// bug 回归②：大数值不得退化为科学计数法（默认 6 位精度会截成 1.09076e+07）
TEST(MetricsExport, LargeValueNoScientificNotation) {
    Registry::instance().gauge("ut_exp_bigval", "h").set(10907648);
    std::string out = Registry::instance().exportText();
    EXPECT_NE(out.find("ut_exp_bigval 10907648"), std::string::npos) << out;
}

// HELP/TYPE 头齐全
TEST(MetricsExport, HelpAndTypeHeaders) {
    Registry::instance().counter("ut_exp_headers", "my help text").inc();
    std::string out = Registry::instance().exportText();
    EXPECT_NE(out.find("# HELP ut_exp_headers my help text"), std::string::npos);
    EXPECT_NE(out.find("# TYPE ut_exp_headers counter"), std::string::npos);
}

// -----------------------------------------------------------------------------
// §6 MetricHooks
// -----------------------------------------------------------------------------

// 成功调用也要 touch 错误计数（预注册为 0），否则零错误期间序列不存在，
// Prometheus 无法区分"没有错误"和"没有该指标"——本项目踩过的坑
TEST(MetricsHooks, ClientErrorCounterPreRegisteredAtZero) {
    // 成功调用预注册最常见的几种错误原因为 0
    MetricHooks::onClientSend("ut_hook_ok");
    MetricHooks::onClientRecv("ut_hook_ok", 12.3, /*error_code=*/"");
    std::string out = Registry::instance().exportText();
    EXPECT_NE(out.find(
        "rpc_client_errors_total{method=\"ut_hook_ok\",code=\"send_failed\"} 0"),
        std::string::npos) << out;
}

// 失败调用错误计数 +1
TEST(MetricsHooks, ClientErrorCounterIncrementsOnFailure) {
    // 失败调用用具体错误类型标记
    MetricHooks::onClientSend("ut_hook_fail");
    MetricHooks::onClientRecv("ut_hook_fail", 99.0, /*error_code=*/"send_failed");
    std::string out = Registry::instance().exportText();
    EXPECT_NE(out.find(
        "rpc_client_errors_total{method=\"ut_hook_fail\",code=\"send_failed\"} 1"),
        std::string::npos);
}

// 熔断器状态 gauge：0=CLOSED 1=OPEN 2=HALF_OPEN
TEST(MetricsHooks, CircuitStateGauge) {
    MetricHooks::onCircuitState("ut_hook_cb", "1.2.3.4:80", 1);
    std::string out = Registry::instance().exportText();
    EXPECT_NE(out.find(
        "circuit_breaker_state{method=\"ut_hook_cb\",host=\"1.2.3.4:80\"} 1"),
        std::string::npos);
}

// 限流拒绝计数
TEST(MetricsHooks, RateLimitedCounter) {
    MetricHooks::onRateLimited("ut_hook_rl");
    MetricHooks::onRateLimited("ut_hook_rl");
    std::string out = Registry::instance().exportText();
    EXPECT_NE(out.find("rpc_rate_limited_total{service=\"ut_hook_rl\"} 2"),
              std::string::npos);
}

// -----------------------------------------------------------------------------
// §7 并发无丢失
// -----------------------------------------------------------------------------

// 4 线程 × 10000 次 inc，总数精确等于 40000（atomic 无丢失更新）
TEST(MetricsConcurrency, ParallelIncNoLoss) {
    auto& c = Registry::instance().counter("ut_conc_inc", "h");
    constexpr int kThreads = 4, kIters = 10000;
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t)
        ts.emplace_back([&c] {
            for (int i = 0; i < kIters; ++i) c.inc();
        });
    for (auto& t : ts) t.join();
    EXPECT_EQ(c.value(), kThreads * kIters);
}

// Gauge 的 CAS inc/dec 并发对冲后归零
TEST(MetricsConcurrency, GaugeIncDecBalanced) {
    auto& g = Registry::instance().gauge("ut_conc_gauge", "h");
    constexpr int kThreads = 4, kIters = 10000;
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t)
        ts.emplace_back([&g, t] {
            for (int i = 0; i < kIters; ++i)
                (t % 2 == 0) ? g.inc() : g.dec();
        });
    for (auto& t : ts) t.join();
    EXPECT_DOUBLE_EQ(g.value(), 0.0);
}
