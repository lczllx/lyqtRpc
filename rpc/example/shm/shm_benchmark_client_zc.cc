// ==================================================================
// shm_benchmark_client_zc.cc — FlatBuffers 零拷贝 SHM 压测客户端
// 用法: shm_benchmark_client_zc [single|multi|throughput] [add|echo] [requests] [threads] [duration]
// ==================================================================
#include "src/client/shm_client_zc.hpp"
#include "src/general/message.hpp"
#include "src/general/detail.hpp"
#include "src/general/log_system/lcz_log.h"
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <sstream>
#include <atomic>
#include <unordered_map>

// ---------- 统计类（与 JSON benchmark 一致）----------
class ZcBenchmarkStats {
public:
    std::vector<double> latencies;
    int success_count = 0;
    int fail_count = 0;
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;

    void record(double latency_us, bool success) {
        if (success) { latencies.push_back(latency_us); success_count++; }
        else { fail_count++; }
    }

    void merge(const ZcBenchmarkStats& other) {
        success_count += other.success_count;
        fail_count += other.fail_count;
        latencies.insert(latencies.end(), other.latencies.begin(), other.latencies.end());
    }

    void print(const std::string& title) {
        if (latencies.empty()) { std::cout << "没有成功的请求" << std::endl; return; }
        std::sort(latencies.begin(), latencies.end());
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
        if (duration == 0) duration = 1;
        double qps = (success_count * 1000.0) / duration;

        size_t n = latencies.size();
        double p50  = latencies[n * 0.50];
        double p90  = latencies[n * 0.90];
        double p95  = latencies[n * 0.95];
        double p99  = latencies[n * 0.99];
        double p999 = latencies[n * 0.999];
        double min_lat = latencies[0];
        double max_lat = latencies[n - 1];

        std::cout << "\n========== " << title << " ==========" << std::endl;
        std::cout << "总请求: " << (success_count + fail_count)
                  << "  成功: " << success_count
                  << "  失败: " << fail_count
                  << "  成功率: " << std::fixed << std::setprecision(2)
                  << (success_count > 0 ? success_count * 100.0 / (success_count + fail_count) : 0)
                  << "%" << std::endl;
        std::cout << "耗时: " << duration << " ms  QPS: " << std::fixed
                  << std::setprecision(2) << qps << std::endl;
        std::cout << "\n延迟 (微秒):" << std::endl;
        std::cout << "  Min:  " << std::setprecision(2) << min_lat << " us" << std::endl;
        std::cout << "  P50:  " << std::setprecision(2) << p50   << " us" << std::endl;
        std::cout << "  P90:  " << std::setprecision(2) << p90   << " us" << std::endl;
        std::cout << "  P95:  " << std::setprecision(2) << p95   << " us" << std::endl;
        std::cout << "  P99:  " << std::setprecision(2) << p99   << " us" << std::endl;
        std::cout << "  P999: " << std::setprecision(2) << p999  << " us" << std::endl;
        std::cout << "  Max:  " << std::setprecision(2) << max_lat << " us" << std::endl;
        std::cout << "==========================================\n" << std::endl;
    }
};

// ---------- 零拷贝 SHM 客户端封装 ----------
class ShmZcBenchClient {
public:
    ShmZcBenchClient(const std::string& shm_name, const std::string& notify_path)
        : _shm_name(shm_name), _notify_path(notify_path) {}

    bool connect() {
        _client = std::make_unique<lcz_rpc::ShmClientZc>(_shm_name, _notify_path);
        _client->setMessageCallback([this](const lcz_rpc::BaseConnection::ptr&,
                                            lcz_rpc::BaseMessage::ptr& msg) {
            auto resp = std::dynamic_pointer_cast<lcz_rpc::RpcResponse>(msg);
            if (!resp) return;
            std::lock_guard<std::mutex> lk(_resp_mutex);
            _resp_map[resp->rid()] = resp;
            _resp_cv.notify_all();
        });
        _client->connect();
        return _client->connected();
    }

    void shutdown() { if (_client) _client->shutdown(); }

    double call(const std::string& method, const Json::Value& params, double timeout_sec = 5.0) {
        auto req = lcz_rpc::MessageFactory::create<lcz_rpc::RpcRequest>();
        std::string rid = uuid();
        req->setId(rid);
        req->setMsgType(lcz_rpc::MsgType::REQ_RPC);
        req->setMethod(method);
        req->setParams(params);

        auto start = std::chrono::steady_clock::now();

        {
            std::lock_guard<std::mutex> lk(_send_mutex);
            if (!_client->send(req)) return -1.0;
        }

        {
            std::unique_lock<std::mutex> lk(_resp_mutex);
            auto deadline = std::chrono::steady_clock::now() +
                std::chrono::milliseconds(static_cast<int>(timeout_sec * 1000));
            bool ok = _resp_cv.wait_until(lk, deadline, [&]{
                return _resp_map.find(rid) != _resp_map.end();
            });
            if (!ok) { _resp_map.erase(rid); return -1.0; }

            auto resp = _resp_map[rid];
            _resp_map.erase(rid);

            auto end = std::chrono::steady_clock::now();
            if (resp->rcode() == lcz_rpc::RespCode::SUCCESS)
                return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            else
                return -1.0;
        }
    }

private:
    std::string _shm_name, _notify_path;
    std::unique_ptr<lcz_rpc::ShmClientZc> _client;
    std::mutex _send_mutex;
    std::mutex _resp_mutex;
    std::condition_variable _resp_cv;
    std::unordered_map<std::string, std::shared_ptr<lcz_rpc::RpcResponse>> _resp_map;
};

// ---------- main ----------
int main(int argc, char* argv[]) {
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);

    std::string test_type = "single";
    std::string method    = "add";
    int requests          = 10000;
    int threads           = 4;
    int duration          = 10;

    if (argc > 1) test_type = argv[1];
    if (argc > 2) method    = argv[2];
    if (argc > 3) requests  = std::atoi(argv[3]);
    if (argc > 4) threads   = std::atoi(argv[4]);
    if (argc > 5) duration  = std::atoi(argv[5]);

    Json::Value params;
    if (method == "add") {
        params["num1"] = 10;
        params["num2"] = 20;
    } else if (method == "echo") {
        params["data"] = "benchmark_payload";
    }

    std::cout << "========== SHM FlatBuffers 零拷贝 RPC 性能测试 ==========" << std::endl;
    std::cout << "测试类型: " << test_type << "  方法: " << method << std::endl;

    ZcBenchmarkStats stats;

    if (test_type == "single") {
        ShmZcBenchClient client("lcz_shm_bench_zc", "lcz_shm_bench_zc_notify");
        if (!client.connect()) { std::cerr << "连接失败" << std::endl; return 1; }
        stats.start_time = std::chrono::steady_clock::now();
        for (int i = 0; i < requests; ++i) {
            double lat = client.call(method, params);
            stats.record(lat, lat >= 0);
        }
        stats.end_time = std::chrono::steady_clock::now();
        client.shutdown();
    }
    else if (test_type == "multi") {
        ShmZcBenchClient shared_client("lcz_shm_bench_zc", "lcz_shm_bench_zc_notify");
        if (!shared_client.connect()) { std::cerr << "连接失败" << std::endl; return 1; }

        int per_thread = requests / threads;
        std::vector<std::thread> ths;
        std::vector<ZcBenchmarkStats> thread_stats(threads);

        stats.start_time = std::chrono::steady_clock::now();
        for (int t = 0; t < threads; ++t) {
            ths.emplace_back([&, t]() {
                for (int i = 0; i < per_thread; ++i) {
                    double lat = shared_client.call(method, params);
                    thread_stats[t].record(lat, lat >= 0);
                }
            });
        }
        for (auto& th : ths) th.join();
        stats.end_time = std::chrono::steady_clock::now();

        shared_client.shutdown();
        for (auto& ts : thread_stats) stats.merge(ts);
    }
    else if (test_type == "throughput") {
        ShmZcBenchClient client("lcz_shm_bench_zc", "lcz_shm_bench_zc_notify");
        if (!client.connect()) { std::cerr << "连接失败" << std::endl; return 1; }
        auto end_time = std::chrono::steady_clock::now() + std::chrono::seconds(duration);
        stats.start_time = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() < end_time) {
            double lat = client.call(method, params);
            stats.record(lat, lat >= 0);
        }
        stats.end_time = std::chrono::steady_clock::now();
        client.shutdown();
    }
    else {
        std::cerr << "未知测试类型: " << test_type << " (支持: single/multi/throughput)" << std::endl;
        return 1;
    }

    stats.print("SHM FlatBuffers 零拷贝 RPC 性能测试结果");
    return 0;
}
