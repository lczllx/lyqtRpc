#pragma once
// =============================================================================
// metrics.hpp — Prometheus 可观测性模块
// =============================================================================
// Counter: 只增不减(请求总数)
// Gauge:   可增可减(熔断器状态、令牌桶余量)
// Histogram: 自动分桶(延迟分布)，bucket: 10/25/50/100/250/500/1k/5k/10k/50k μs
// 全部基于 std::atomic 无锁写入，Registry 持 mutex 管理指标注册表。
//
// 用法:
//   auto& c = METRICS_COUNTER("rpc_requests", "Total requests", {{"method","add"}});
//   c.inc();
//   auto& g = METRICS_GAUGE("circuit_state", "Circuit state", {{"host","x"}});
//   g.set(0);
//   auto& h = METRICS_HISTO("rpc_latency", "Latency us", {{"method","echo"}});
//   h.observe(15.3);
//
// curl http://localhost:9090/metrics → Prometheus text format 0.0.4
// =============================================================================
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <atomic>
#include <sstream>
#include <iomanip>
#include <utility>

namespace lcz_rpc
{
    namespace metrics
    {

        // 标签集：有序 kv 对，如 {{"method","echo"},{"host","1.2.3.4"}}
        // 用 vector 而非 map：埋点处标签只有 1~2 个，保持书写顺序即可
        using Labels = std::vector<std::pair<std::string, std::string>>;

        // 格式化 labels → {key1="val1",key2="val2"}，空 labels 返回空串
        inline std::string formatLabels(const Labels &ls)
        {
            if (ls.empty())
                return "";
            std::string s = "{";
            for (size_t i = 0; i < ls.size(); ++i)
            {
                if (i > 0)
                    s += ",";
                s += ls[i].first + "=\"" + ls[i].second + "\"";
            }
            return s + "}";
        }
        // 向已格式化的 labels 串追加一个标签（histogram 导出 le 桶时用）:
        // {a="1"} + le="2" → {a="1",le="2"}；原 labels 为空时 → {le="2"}
        // 若无此合并，输出会变成非法的 {a="1"}{le="2"} 两组大括号
        inline std::string mergeLabels(const std::string &existing, const std::string &extraKV)
        {
            if (existing.empty() || existing == "{}")
                return "{" + extraKV + "}";
            return existing.substr(0, existing.size() - 1) + "," + extraKV + "}";
        }

        // ====== Counter：只增计数器（请求总数/错误总数）======
        // 写入是 relaxed 原子加，多线程并发 inc() 无锁；
        // labels 在构造时就预格式化成 {k="v",...} 字符串，导出时零拼接开销
        class Counter
        {
            std::string _name, _help, _labels; // 元信息，构造后只读
            std::atomic<int64_t> _value{0};    // 唯一会变的状态

        public:
            Counter(const std::string &name, const std::string &help, const Labels &ls = {})
                : _name(name), _help(help), _labels(formatLabels(ls)) {}
            void inc() { _value.fetch_add(1, std::memory_order_relaxed); }
            void add(int64_t n) { _value.fetch_add(n, std::memory_order_relaxed); }
            int64_t value() const { return _value.load(std::memory_order_relaxed); }
            const std::string &name() const { return _name; }
            const std::string &help() const { return _help; }
            const std::string &labels() const { return _labels; }
        };

        // ====== Gauge：可增可减的瞬时值（并发数/连接数/熔断状态/令牌余量）======
        // 注意 std::atomic<double> 没有 fetch_add（C++20 前），
        // 所以 inc/dec 用 CAS 循环实现原子加减
        class Gauge
        {
            std::string _name, _help, _labels;
            std::atomic<double> _value{0.0}; // 用 double 以兼容令牌数等小数值

        public:
            Gauge(const std::string &name, const std::string &help, const Labels &ls = {})
                : _name(name), _help(help), _labels(formatLabels(ls)) {}
            void set(double v) { _value.store(v, std::memory_order_relaxed); }
            // CAS 失败说明有并发写，o 已被更新为最新值，重试即可
            void inc()
            {
                double o = _value.load(std::memory_order_relaxed);
                while (!_value.compare_exchange_weak(o, o + 1.0, std::memory_order_relaxed))
                    ;
            }
            void dec()
            {
                double o = _value.load(std::memory_order_relaxed);
                while (!_value.compare_exchange_weak(o, o - 1.0, std::memory_order_relaxed))
                    ;
            }
            double value() const { return _value.load(std::memory_order_relaxed); }
            const std::string &name() const { return _name; }
            const std::string &help() const { return _help; }
            const std::string &labels() const { return _labels; }
        };

        // ====== Histogram：延迟分布直方图 ======
        // 存储用"非累计"桶：observe(v) 只给 v 落入的那一个桶 +1（写侧最快），
        // 导出时再由 exportText 现算 Prometheus 要求的累计桶（le="x" 表示 ≤x 的总数）。
        // 桶边界固定 10μs~50ms 共 10 档，超过 50ms 的只进 +Inf（即只计入 _count）
        class Histogram
        {
            static constexpr size_t N = 10;
            // 桶上界（μs）：覆盖 SHM 十几μs 到 TCP 大包几十 ms 的量程
            static constexpr double B[N] = {10, 25, 50, 100, 250, 500, 1000, 5000, 10000, 50000};
            std::string _name, _help, _labels;
            std::atomic<int64_t> _count{0}; // 总观测次数（含超出最大桶的）
            std::atomic<double> _sum{0.0};  // 观测值总和，_sum/_count 即平均延迟
            std::atomic<int64_t> _b[N];     // 各桶命中数（非累计）

        public:
            Histogram(const std::string &name, const std::string &help, const Labels &ls = {})
                : _name(name), _help(help), _labels(formatLabels(ls))
            {
                for (size_t i = 0; i < N; ++i)
                    _b[i].store(0, std::memory_order_relaxed);
            }
            // 记录一次观测：count+1、sum+=v、v 落入的第一个桶 +1
            // 三个原子操作间不保证一致性快照——scrape 恰好穿插在中间时
            // 单次误差 ±1，对监控无影响，换来写侧完全无锁
            void observe(double v)
            {
                _count.fetch_add(1, std::memory_order_relaxed);
                double s = _sum.load(std::memory_order_relaxed);
                while (!_sum.compare_exchange_weak(s, s + v, std::memory_order_relaxed))
                    ; // atomic<double> 无 fetch_add，CAS 重试
                for (size_t i = 0; i < N; ++i)
                    if (v <= B[i])
                    {
                        _b[i].fetch_add(1, std::memory_order_relaxed);
                        break; // 只进最小的匹配桶，累计留给导出侧
                    }
            }
            const std::string &name() const { return _name; }
            const std::string &help() const { return _help; }
            const std::string &labels() const { return _labels; }
            int64_t count() const { return _count.load(std::memory_order_relaxed); }
            double sum() const { return _sum.load(std::memory_order_relaxed); }
            int64_t bucket(size_t i) const { return _b[i].load(std::memory_order_relaxed); }
            static size_t bucketCount() { return N; }
            static double bucketBound(size_t i) { return B[i]; }
        };

        // ====== Registry 单例：全进程唯一的指标注册表 ======
        // key = 指标名 + 格式化后的 labels（如 rpc_requests_total{method="echo"}），
        // 同名不同 label 是不同的时间序列，各占一个条目。
        // 首次访问自动创建（懒注册），之后返回同一个对象的引用——
        // 指标对象创建后永不销毁（进程生命周期），所以返回的引用可以长期持有，
        // 埋点处用 static 局部变量缓存引用即可跳过查表（热路径优化手段）。
        //
        // 锁的范围：只保护 map 的查找/插入，不保护指标值本身
        // （值是 atomic，写入无锁；这也是"注册慢、写入快"的常见设计）
        class Registry
        {
        public:
            static Registry &instance()
            {
                static Registry r; // C++11 起局部 static 初始化线程安全
                return r;
            }

            // 获取（或首次创建）一个 Counter；gauge/histogram 同理
            Counter &counter(const std::string &name, const std::string &help, const Labels &ls = {})
            {
                std::string key = name + formatLabels(ls);
                std::lock_guard<std::mutex> lk(_m);
                auto *&p = _counters[key]; // 指针的引用：不存在时 operator[] 插入 nullptr
                if (!p)
                    p = new Counter(name, help, ls); // 故意不 delete：进程级生命周期
                return *p;
            }
            Gauge &gauge(const std::string &name, const std::string &help, const Labels &ls = {})
            {
                std::string key = name + formatLabels(ls);
                std::lock_guard<std::mutex> lk(_m);
                auto *&p = _gauges[key];
                if (!p)
                    p = new Gauge(name, help, ls);
                return *p;
            }
            Histogram &histogram(const std::string &name, const std::string &help, const Labels &ls = {})
            {
                std::string key = name + formatLabels(ls);
                std::lock_guard<std::mutex> lk(_m);
                auto *&p = _hists[key];
                if (!p)
                    p = new Histogram(name, help, ls);
                return *p;
            }

            // 导出全部指标为 Prometheus 文本格式 0.0.4（/metrics 的响应体）
            // 每个指标输出 # HELP（说明）、# TYPE（类型）、值三部分。
            // 持锁遍历期间新指标注册会短暂阻塞，但 scrape 15s 一次、毫秒级完成，无影响
            std::string exportText()
            {
                std::lock_guard<std::mutex> lk(_m);
                std::ostringstream ss;
                // 15 位有效数字：默认 6 位会把大数截成科学计数法（8.8023e+06），
                // 既难读又丢精度（字节数丢尾数、uptime 丢小数）
                ss << std::setprecision(15);
                // Counter/Gauge：一行 name{labels} value
                for (auto &[k, c] : _counters)
                    ss << "# HELP " << c->name() << " " << c->help() << "\n"
                       << "# TYPE " << c->name() << " counter\n"
                       << c->name() << c->labels() << " " << c->value() << "\n\n";
                for (auto &[k, g] : _gauges)
                {
                    // OpenMetrics 约定 _total 后缀专属 counter。
                    // process_cpu_seconds_total 这类"单调递增但用 set() 覆写"的指标
                    // 内部只能存成 Gauge，导出时按名字后缀修正 TYPE，
                    // 否则出现 "gauge 却叫 _total" 的矛盾，promtool lint 会告警
                    const auto &n = g->name();
                    bool is_total = n.size() > 6 && n.compare(n.size() - 6, 6, "_total") == 0;
                    ss << "# HELP " << n << " " << g->help() << "\n"
                       << "# TYPE " << n << " " << (is_total ? "counter" : "gauge") << "\n"
                       << n << g->labels() << " " << g->value() << "\n\n";
                }
                // Histogram：Prometheus 要求桶是"累计"语义——
                // le="50" 的值 = 所有 ≤50 的观测数（含 le="10"/le="25" 的），
                // 存储是非累计的，这里导出时现场累加（桶只有 10 个，开销可忽略）
                for (auto &[k, h] : _hists)
                {
                    ss << "# HELP " << h->name() << " " << h->help() << "\n"
                       << "# TYPE " << h->name() << " histogram\n";
                    for (size_t i = 0; i < h->bucketCount(); ++i)
                    {
                        int64_t cum = 0;
                        for (size_t j = 0; j <= i; ++j)
                            cum += static_cast<int64_t>(h->bucket(j));
                        // mergeLabels 把 le 桶标签并进已有 labels 的大括号内:
                        // {method="echo"} + le="10" → {method="echo",le="10"}
                        ss << h->name() << "_bucket"
                           << mergeLabels(h->labels(),
                                          "le=\"" + std::to_string(static_cast<int64_t>(h->bucketBound(i))) + "\"")
                           << " " << cum << "\n";
                    }
                    // +Inf 桶 = 总观测数；_sum/_count 供算平均值和 rate
                    ss << h->name() << "_bucket"
                       << mergeLabels(h->labels(), "le=\"+Inf\"") << " " << h->count() << "\n"
                       << h->name() << "_sum" << h->labels() << " " << h->sum() << "\n"
                       << h->name() << "_count" << h->labels() << " " << h->count() << "\n\n";
                }
                return ss.str();
            }

            Registry(const Registry &) = delete;
            void operator=(const Registry &) = delete;

        private:
            Registry() = default;
            std::mutex _m;
            std::unordered_map<std::string, Counter *> _counters;
            std::unordered_map<std::string, Gauge *> _gauges;
            std::unordered_map<std::string, Histogram *> _hists;
        };

// 便捷宏：埋点处一行拿到指标引用，如 METRICS_COUNTER(...).inc()
// 注意每次调用都会走一遍 Registry 查表（mutex+map），
// 热路径可用 static auto& 缓存返回的引用，只查一次
#define METRICS_COUNTER(n, h, ls) lcz_rpc::metrics::Registry::instance().counter(n, h, ls)
#define METRICS_GAUGE(n, h, ls) lcz_rpc::metrics::Registry::instance().gauge(n, h, ls)
#define METRICS_HISTO(n, h, ls) lcz_rpc::metrics::Registry::instance().histogram(n, h, ls)

    } // namespace metrics
} // namespace lcz_rpc
