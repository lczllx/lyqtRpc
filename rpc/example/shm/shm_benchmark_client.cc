// ==================================================================
// shm_benchmark_client.cc — SHM 性能测试客户端（多客户端支持）
// 多线程模式: 每个线程独立的 ShmClient，真并发，无 send mutex
// ==================================================================
#include "src/client/shm_client.hpp"
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

class BenchmarkStats {
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
    void merge(const BenchmarkStats& other) {
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
        std::cout << "\n========== " << title << " ==========" << std::endl;
        std::cout << "总请求: " << (success_count + fail_count) << "  成功: " << success_count
                  << "  失败: " << fail_count << "  成功率: " << std::fixed << std::setprecision(2)
                  << (success_count > 0 ? success_count*100.0/(success_count+fail_count) : 0)
                  << "%" << std::endl;
        std::cout << "耗时: " << duration << " ms  QPS: " << std::fixed
                  << std::setprecision(2) << qps << std::endl;
        std::cout << "\n延迟 (微秒):" << std::endl;
        std::cout << "  Min:  " << latencies[0] << " us" << std::endl;
        std::cout << "  P50:  " << latencies[n*0.50] << " us" << std::endl;
        std::cout << "  P90:  " << latencies[n*0.90] << " us" << std::endl;
        std::cout << "  P95:  " << latencies[n*0.95] << " us" << std::endl;
        std::cout << "  P99:  " << latencies[n*0.99] << " us" << std::endl;
        std::cout << "  P999: " << latencies[n*0.999] << " us" << std::endl;
        std::cout << "  Max:  " << latencies[n-1] << " us" << std::endl;
        std::cout << "==========================================\n" << std::endl;
    }
};

// 每个线程独立 ShmClient，无锁，真并发
void thread_worker(const std::string& notify_path, const std::string& method,
                   const Json::Value& params, int num_requests, BenchmarkStats& stats,
                   int duration_sec = 0) {
    lcz_rpc::ShmClient client(notify_path);

    std::mutex mtx;
    std::condition_variable cv;
    bool got_resp = false;
    std::string rid;

    client.setMessageCallback([&](const lcz_rpc::BaseConnection::ptr&,
                                   lcz_rpc::BaseMessage::ptr& msg) {
        auto resp = std::dynamic_pointer_cast<lcz_rpc::RpcResponse>(msg);
        if (!resp) return;
        std::lock_guard<std::mutex> lk(mtx);
        if (resp->rid() == rid) { got_resp = true; cv.notify_one(); }
    });

    client.connect();
    if (!client.connected()) { stats.fail_count = 1; std::cerr << "[ERROR] 连接失败" << std::endl; return; }

    int total = (duration_sec > 0) ? 99999999 : num_requests;
    auto deadline = (duration_sec > 0)
        ? std::chrono::steady_clock::now() + std::chrono::seconds(duration_sec)
        : std::chrono::steady_clock::time_point::max();

    int batch_count = 0; double batch_lat = 0;
    auto report_next = std::chrono::steady_clock::now() + std::chrono::seconds(1);

    for (int i = 0; i < total; ++i) {
        auto req = lcz_rpc::MessageFactory::create<lcz_rpc::RpcRequest>();
        {
            std::lock_guard<std::mutex> lk(mtx);
            rid = uuid();
            req->setId(rid);
            got_resp = false;
        }
        req->setMsgType(lcz_rpc::MsgType::REQ_RPC);
        req->setMethod(method);
        req->setParams(params);

        auto t1 = std::chrono::steady_clock::now();
        if (!client.send(req)) { stats.record(-1, false); continue; }

        {
            std::unique_lock<std::mutex> lk(mtx);
            cv.wait(lk, [&]{ return got_resp; });
        }
        auto t2 = std::chrono::steady_clock::now();
        double lat = std::chrono::duration_cast<std::chrono::microseconds>(t2-t1).count();
        stats.record(lat, true);
        if (duration_sec > 0) {
            batch_count++; batch_lat += lat;
            if (t2 >= report_next) {
                std::cout << "Sending EchoRequest at qps=" << batch_count
                          << " latency=" << static_cast<int>(batch_lat/batch_count) << std::endl;
                batch_count = 0; batch_lat = 0;
                report_next = t2 + std::chrono::seconds(1);
            }
            if (t2 >= deadline) break;
        }
    }

    client.shutdown();
}

int main(int argc, char* argv[]) {
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::ERROR);

    std::string test_type = "single";
    std::string method    = "add";
    int requests          = 10000;
    int threads           = 4;
    int duration          = 10;

    if (argc > 1 && strlen(argv[1]) > 0) test_type = argv[1];
    if (argc > 2 && strlen(argv[2]) > 0) method    = argv[2];
    if (argc > 3 && strlen(argv[3]) > 0) requests  = std::atoi(argv[3]);
    if (argc > 4 && strlen(argv[4]) > 0) threads   = std::atoi(argv[4]);
    int    payload_size = 16;
    if (argc > 5 && strlen(argv[5]) > 0) duration     = std::atoi(argv[5]);
    if (argc > 6 && strlen(argv[6]) > 0) payload_size = std::atoi(argv[6]);

    Json::Value params;
    if (method == "add") { params["num1"] = 10; params["num2"] = 20; }
    else if (method == "echo") { params["data"] = std::string(payload_size, 'x'); }

    std::cout << "========== SHM (共享内存) RPC 性能测试 ==========" << std::endl;
    std::cout << "测试类型: " << test_type << "  方法: " << method;
    if (method == "echo") std::cout << "  载荷: " << payload_size << "B";
    std::cout << std::endl;

    BenchmarkStats stats;

    if (test_type == "single") {
        stats.start_time = std::chrono::steady_clock::now();
        thread_worker("lcz_shm_bench_notify", method, params, requests, stats, duration);
        stats.end_time = std::chrono::steady_clock::now();
    }
    else if (test_type == "multi") {
        int per_thread = requests / threads;
        std::vector<std::thread> ths;
        std::vector<BenchmarkStats> thread_stats(threads);

        stats.start_time = std::chrono::steady_clock::now();
        for (int t = 0; t < threads; ++t) {
            ths.emplace_back([&, t]() {
                thread_worker("lcz_shm_bench_notify", method, params, per_thread, thread_stats[t], duration);
            });
        }
        for (auto& th : ths) th.join();
        stats.end_time = std::chrono::steady_clock::now();

        for (auto& ts : thread_stats) stats.merge(ts);
    }
    else if (test_type == "throughput") {
        // throughput: 持续发请求直到 duration 秒到期
        lcz_rpc::ShmClient client("lcz_shm_bench_notify");
        std::mutex mtx; std::condition_variable cv;
        bool got_resp = false; std::string rid;
        client.setMessageCallback([&](const lcz_rpc::BaseConnection::ptr&,
                                       lcz_rpc::BaseMessage::ptr& msg) {
            auto resp = std::dynamic_pointer_cast<lcz_rpc::RpcResponse>(msg);
            if (!resp) return;
            std::lock_guard<std::mutex> lk(mtx);
            if (resp->rid() == rid) { got_resp = true; cv.notify_one(); }
        });
        client.connect();
        if (!client.connected()) { stats.fail_count = 1; stats.print("SHM RPC 性能测试结果"); return 1; }

        auto end = std::chrono::steady_clock::now() + std::chrono::seconds(duration);
        stats.start_time = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() < end) {
            auto req = lcz_rpc::MessageFactory::create<lcz_rpc::RpcRequest>();
            { std::lock_guard<std::mutex> lk(mtx); rid = uuid(); req->setId(rid); got_resp = false; }
            req->setMsgType(lcz_rpc::MsgType::REQ_RPC);
            req->setMethod(method);
            req->setParams(params);
            auto t1 = std::chrono::steady_clock::now();
            if (!client.send(req)) { stats.record(-1, false); continue; }
            { std::unique_lock<std::mutex> lk(mtx); cv.wait(lk, [&]{ return got_resp; }); }
            auto t2 = std::chrono::steady_clock::now();
            stats.record(std::chrono::duration_cast<std::chrono::microseconds>(t2-t1).count(), true);
        }
        stats.end_time = std::chrono::steady_clock::now();
        client.shutdown();
    }
    else {
        std::cerr << "未知测试类型: " << test_type << " (支持: single/multi/throughput)" << std::endl;
        return 1;
    }

    stats.print("SHM RPC 性能测试结果");
    return 0;
}
