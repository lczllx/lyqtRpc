#include "../../src/client/rpc_client.hpp"
#include "../../src/general/detail.hpp"
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>
#include <iomanip>
#include <cmath>
#include <iostream>
#include <string>

class BenchmarkStats {
public:
    std::vector<double> latencies;  // 延迟（微秒）
    int success_count = 0;
    int fail_count = 0;
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;

    void record(double latency_us, bool success) {
        if (success) {
            latencies.push_back(latency_us);
            success_count++;
        } else {
            fail_count++;
        }
    }

    void print() {
        if (latencies.empty()) {
            std::cout << "没有成功的请求" << std::endl;
            return;
        }

        std::sort(latencies.begin(), latencies.end());

        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();

        double total_requests = success_count + fail_count;
        double qps = (success_count * 1000.0) / duration;

        double sum = 0;
        for (double lat : latencies) {
            sum += lat;
        }
        double avg = sum / latencies.size();

        size_t p50_idx = latencies.size() * 0.5;
        size_t p90_idx = latencies.size() * 0.9;
        size_t p95_idx = latencies.size() * 0.95;
        size_t p99_idx = latencies.size() * 0.99;

        double p50 = latencies[p50_idx];
        double p90 = latencies[p90_idx];
        double p95 = latencies[p95_idx];
        double p99 = latencies[p99_idx];
        double min_lat = latencies[0];
        double max_lat = latencies[latencies.size() - 1];

        std::cout << "\n========== 性能测试结果（JSON 序列化，原版副本）==========" << std::endl;
        std::cout << "总请求数: " << total_requests << std::endl;
        std::cout << "成功请求: " << success_count << std::endl;
        std::cout << "失败请求: " << fail_count << std::endl;
        std::cout << "成功率: " << std::fixed << std::setprecision(2)
                  << (success_count / total_requests * 100) << "%" << std::endl;
        std::cout << "测试时长: " << duration << " ms" << std::endl;
        std::cout << "QPS: " << std::fixed << std::setprecision(2) << qps << std::endl;
        std::cout << "\n延迟统计 (微秒):" << std::endl;
        std::cout << "  最小值: " << std::fixed << std::setprecision(2) << min_lat << " us" << std::endl;
        std::cout << "  平均值: " << std::fixed << std::setprecision(2) << avg << " us" << std::endl;
        std::cout << "  P50:    " << std::fixed << std::setprecision(2) << p50 << " us" << std::endl;
        std::cout << "  P90:    " << std::fixed << std::setprecision(2) << p90 << " us" << std::endl;
        std::cout << "  P95:    " << std::fixed << std::setprecision(2) << p95 << " us" << std::endl;
        std::cout << "  P99:    " << std::fixed << std::setprecision(2) << p99 << " us" << std::endl;
        std::cout << "  最大值: " << std::fixed << std::setprecision(2) << max_lat << " us" << std::endl;
        std::cout << "==================================\n" << std::endl;
    }
};

// 单线程性能测试
void single_thread_test(lcz_rpc::client::RpcClient& client,
                       const std::string& method,
                       const Json::Value& params,
                       int requests,
                       BenchmarkStats& stats) {
    stats.start_time = std::chrono::steady_clock::now();

    for (int i = 0; i < requests; ++i) {
        auto start = std::chrono::steady_clock::now();
        Json::Value result;
        bool success = client.call(method, params, result);
        auto end = std::chrono::steady_clock::now();

        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        stats.record(latency, success);
    }

    stats.end_time = std::chrono::steady_clock::now();
}

// 多线程并发测试
void multi_thread_test(lcz_rpc::client::RpcClient& client,
                      const std::string& method,
                      const Json::Value& params,
                      int total_requests,
                      int thread_count,
                      BenchmarkStats& stats) {
    int requests_per_thread = total_requests / thread_count;
    std::vector<std::thread> threads;
    std::vector<BenchmarkStats> thread_stats(thread_count);

    stats.start_time = std::chrono::steady_clock::now();

    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < requests_per_thread; ++i) {
                auto start = std::chrono::steady_clock::now();
                Json::Value result;
                bool success = client.call(method, params, result);
                auto end = std::chrono::steady_clock::now();

                auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                thread_stats[t].record(latency, success);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    stats.end_time = std::chrono::steady_clock::now();

    // 合并统计结果
    for (auto& ts : thread_stats) {
        stats.success_count += ts.success_count;
        stats.fail_count += ts.fail_count;
        stats.latencies.insert(stats.latencies.end(),
                              ts.latencies.begin(), ts.latencies.end());
    }
}

// 吞吐量测试（固定时间）
void throughput_test(lcz_rpc::client::RpcClient& client,
                    const std::string& method,
                    const Json::Value& params,
                    int duration_seconds,
                    BenchmarkStats& stats) {
    auto end_time = std::chrono::steady_clock::now() +
                    std::chrono::seconds(duration_seconds);

    stats.start_time = std::chrono::steady_clock::now();

    int count = 0;
    while (std::chrono::steady_clock::now() < end_time) {
        auto start = std::chrono::steady_clock::now();
        Json::Value result;
        bool success = client.call(method, params, result);
        auto end = std::chrono::steady_clock::now();

        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        stats.record(latency, success);
        count++;
    }

    stats.end_time = std::chrono::steady_clock::now();
    std::cout << "在 " << duration_seconds << " 秒内完成了 " << count << " 个请求" << std::endl;
}

int main(int argc, char* argv[])
{
    //lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);

    std::string test_type = "single";  // single, multi, throughput
    std::string method = "add";
    int requests = 10000;
    int threads = 4;
    int duration = 10;  // 秒
    bool use_discover = false;
    std::string server_ip = "127.0.0.1";
    int server_port = 8889;
    int registry_port = 8080;
    int payload_size = 0;  // echo 时若 >0 则使用该字节数的大 payload，用于对比 JSON/Proto

    // 解析命令行参数
    if (argc > 1) test_type = argv[1];
    if (argc > 2) method = argv[2];
    if (argc > 3) requests = std::atoi(argv[3]);
    if (argc > 4) threads = std::atoi(argv[4]);
    if (argc > 5) duration = std::atoi(argv[5]);
    if (argc > 6) use_discover = std::atoi(argv[6]) != 0;
    if (argc > 7) server_ip = argv[7];
    if (argc > 8) server_port = std::atoi(argv[8]);
    if (argc > 9) registry_port = std::atoi(argv[9]);
    if (argc > 10) payload_size = std::atoi(argv[10]);

    std::cout << "========== RPC 性能测试（JSON 序列化，原版副本）==========" << std::endl;
    if (method == "echo" && payload_size > 0)
        std::cout << "方法名: " << method << "，payload 大小: " << payload_size << " 字节（大 payload 对比）" << std::endl;
    std::cout << "测试类型: " << test_type << std::endl;
    std::cout << "方法名: " << method << std::endl;
    std::cout << "服务器: " << server_ip << ":" << server_port << std::endl;
    std::cout << "使用服务发现: " << (use_discover ? "是" : "否") << std::endl;

    // 创建客户端
    lcz_rpc::client::RpcClient client(use_discover, server_ip, use_discover ? registry_port : server_port);

    // 等待连接建立
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 测试连接（通过一次简单的调用）
    Json::Value test_params;
    test_params["num1"] = 1;
    test_params["num2"] = 1;
    Json::Value test_result;
    if (!client.call("add", test_params, test_result)) {
        std::cerr << "无法连接到服务器或服务不可用" << std::endl;
        std::cerr << "请确保服务端已启动在 " << server_ip << ":" << server_port << std::endl;
        return -1;
    }
    std::cout << "连接成功，开始性能测试..." << std::endl;

    // 准备测试参数
    Json::Value params;
    if (method == "add") {
        params["num1"] = 10;
        params["num2"] = 20;
    } else if (method == "echo") {
        if (payload_size > 0)
            params["data"]["payload"] = std::string(static_cast<size_t>(payload_size), 'x');
        else
            params["data"]["test"] = "benchmark";
    } else if (method == "heavy_compute") {
        params["value"] = 1000;
    }

    BenchmarkStats stats;

    // 执行测试
    if (test_type == "single") {
        std::cout << "单线程测试，请求数: " << requests << std::endl;
        single_thread_test(client, method, params, requests, stats);
    } else if (test_type == "multi") {
        std::cout << "多线程测试，线程数: " << threads << ", 总请求数: " << requests << std::endl;
        multi_thread_test(client, method, params, requests, threads, stats);
    } else if (test_type == "throughput") {
        std::cout << "吞吐量测试，持续时间: " << duration << " 秒" << std::endl;
        throughput_test(client, method, params, duration, stats);
    } else {
        std::cerr << "未知的测试类型: " << test_type << std::endl;
        std::cerr << "支持的类型: single, multi, throughput" << std::endl;
        return -1;
    }

    // 打印结果
    stats.print();

    return 0;
}
