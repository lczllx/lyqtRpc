#include "../../src/client/rpc_client.hpp"
#include "../../src/general/message.hpp"
#include "rpc_envelope.pb.h"
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>
#include <iomanip>
#include <cmath>
#include <iostream>
#include <string>

using namespace lcz_rpc::proto;

class BenchmarkStats {
public:
    std::vector<double> latencies;
    int success_count = 0;
    int fail_count = 0;
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;
    std::string echo_payload = "benchmark";  // echo 的 payload，可设为大字符串以对比 JSON/Proto

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
        for (double lat : latencies) sum += lat;
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

        std::cout << "\n========== 性能测试结果（Protobuf 序列化）==========" << std::endl;
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

// 根据 method 执行一次 call_proto 并返回是否成功（用于统计延迟）
template<typename Req, typename Resp>
bool call_proto_once(lcz_rpc::client::RpcClient& client, const std::string& method,
                     const Req& req, Resp* resp) {
    return client.call_proto<Req, Resp>(method, req, resp);
}

// 单线程测试
void single_thread_test(lcz_rpc::client::RpcClient& client, const std::string& method,
                        int requests, BenchmarkStats& stats) {
    stats.start_time = std::chrono::steady_clock::now();
    if (method == "add") {
        AddRequest req;
        req.set_num1(10);
        req.set_num2(20);
        for (int i = 0; i < requests; ++i) {
            auto start = std::chrono::steady_clock::now();
            AddResponse resp;
            bool success = client.call_proto<AddRequest, AddResponse>("add", req, &resp);
            auto end = std::chrono::steady_clock::now();
            stats.record(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(), success);
        }
    } else if (method == "echo") {
        EchoRequest req;
        req.set_data(stats.echo_payload);  // 可为大字符串，见 main 中设置
        for (int i = 0; i < requests; ++i) {
            auto start = std::chrono::steady_clock::now();
            EchoResponse resp;
            bool success = client.call_proto<EchoRequest, EchoResponse>("echo", req, &resp);
            auto end = std::chrono::steady_clock::now();
            stats.record(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(), success);
        }
    } else if (method == "heavy_compute") {
        HeavyRequest req;
        req.set_value(1000);
        for (int i = 0; i < requests; ++i) {
            auto start = std::chrono::steady_clock::now();
            HeavyResponse resp;
            bool success = client.call_proto<HeavyRequest, HeavyResponse>("heavy_compute", req, &resp);
            auto end = std::chrono::steady_clock::now();
            stats.record(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(), success);
        }
    }
    stats.end_time = std::chrono::steady_clock::now();
}

// 多线程测试
void multi_thread_test(lcz_rpc::client::RpcClient& client, const std::string& method,
                      int total_requests, int thread_count, BenchmarkStats& stats) {
    int requests_per_thread = total_requests / thread_count;
    std::vector<std::thread> threads;
    std::vector<BenchmarkStats> thread_stats(thread_count);
    stats.start_time = std::chrono::steady_clock::now();

    auto run_add = [&](int t) {
        AddRequest req;
        req.set_num1(10);
        req.set_num2(20);
        for (int i = 0; i < requests_per_thread; ++i) {
            auto start = std::chrono::steady_clock::now();
            AddResponse resp;
            bool ok = client.call_proto<AddRequest, AddResponse>("add", req, &resp);
            thread_stats[t].record(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count(), ok);
        }
    };
    auto run_echo = [&](int t) {
        EchoRequest req;
        req.set_data(stats.echo_payload);
        for (int i = 0; i < requests_per_thread; ++i) {
            auto start = std::chrono::steady_clock::now();
            EchoResponse resp;
            bool ok = client.call_proto<EchoRequest, EchoResponse>("echo", req, &resp);
            thread_stats[t].record(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count(), ok);
        }
    };
    auto run_heavy = [&](int t) {
        HeavyRequest req;
        req.set_value(1000);
        for (int i = 0; i < requests_per_thread; ++i) {
            auto start = std::chrono::steady_clock::now();
            HeavyResponse resp;
            bool ok = client.call_proto<HeavyRequest, HeavyResponse>("heavy_compute", req, &resp);
            thread_stats[t].record(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count(), ok);
        }
    };

    for (int t = 0; t < thread_count; ++t) {
        if (method == "add") threads.emplace_back(run_add, t);
        else if (method == "echo") threads.emplace_back(run_echo, t);
        else threads.emplace_back(run_heavy, t);
    }
    for (auto& th : threads) th.join();
    stats.end_time = std::chrono::steady_clock::now();
    for (auto& ts : thread_stats) {
        stats.success_count += ts.success_count;
        stats.fail_count += ts.fail_count;
        stats.latencies.insert(stats.latencies.end(), ts.latencies.begin(), ts.latencies.end());
    }
}

// 吞吐量测试
void throughput_test(lcz_rpc::client::RpcClient& client, const std::string& method,
                     int duration_seconds, BenchmarkStats& stats) {
    auto end_time = std::chrono::steady_clock::now() + std::chrono::seconds(duration_seconds);
    stats.start_time = std::chrono::steady_clock::now();
    if (method == "add") {
        AddRequest req;
        req.set_num1(10);
        req.set_num2(20);
        while (std::chrono::steady_clock::now() < end_time) {
            auto start = std::chrono::steady_clock::now();
            AddResponse resp;
            bool ok = client.call_proto<AddRequest, AddResponse>("add", req, &resp);
            stats.record(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count(), ok);
        }
    } else if (method == "echo") {
        EchoRequest req;
        req.set_data(stats.echo_payload);
        while (std::chrono::steady_clock::now() < end_time) {
            auto start = std::chrono::steady_clock::now();
            EchoResponse resp;
            bool ok = client.call_proto<EchoRequest, EchoResponse>("echo", req, &resp);
            stats.record(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count(), ok);
        }
    } else if (method == "heavy_compute") {
        HeavyRequest req;
        req.set_value(1000);
        while (std::chrono::steady_clock::now() < end_time) {
            auto start = std::chrono::steady_clock::now();
            HeavyResponse resp;
            bool ok = client.call_proto<HeavyRequest, HeavyResponse>("heavy_compute", req, &resp);
            stats.record(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count(), ok);
        }
    }
    stats.end_time = std::chrono::steady_clock::now();
}

int main(int argc, char* argv[])
{
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);

    std::string test_type = "single";
    std::string method = "add";
    int requests = 10000;
    int threads = 4;
    int duration = 10;
    bool use_discover = false;
    std::string server_ip = "127.0.0.1";
    int server_port = 8889;
    int registry_port = 8080;
    int payload_size = 0;  // echo 时若 >0 则使用该字节数的大 payload，用于对比 JSON/Proto

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

    BenchmarkStats stats;
    if (method == "echo" && payload_size > 0)
        stats.echo_payload.assign(static_cast<size_t>(payload_size), 'x');

    std::cout << "========== RPC 性能测试（默认 Protobuf）==========" << std::endl;
    if (method == "echo" && payload_size > 0)
        std::cout << "方法名: " << method << "，payload 大小: " << payload_size << " 字节（大 payload 对比）" << std::endl;
    std::cout << "测试类型: " << test_type << std::endl;
    std::cout << "方法名: " << method << std::endl;
    std::cout << "服务器: " << server_ip << ":" << server_port << std::endl;
    std::cout << "使用服务发现: " << (use_discover ? "是" : "否") << std::endl;

    lcz_rpc::client::RpcClient client(use_discover, server_ip, use_discover ? registry_port : server_port);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 测试连接
    AddRequest test_req;
    test_req.set_num1(1);
    test_req.set_num2(1);
    AddResponse test_resp;
    if (!client.call_proto<AddRequest, AddResponse>("add", test_req, &test_resp)) {
        std::cerr << "无法连接到服务器或服务不可用" << std::endl;
        std::cerr << "请确保服务端已启动在 " << server_ip << ":" << server_port << std::endl;
        return -1;
    }
    std::cout << "连接成功，开始性能测试..." << std::endl;

    if (test_type == "single") {
        std::cout << "单线程测试，请求数: " << requests << std::endl;
        single_thread_test(client, method, requests, stats);
    } else if (test_type == "multi") {
        std::cout << "多线程测试，线程数: " << threads << ", 总请求数: " << requests << std::endl;
        multi_thread_test(client, method, requests, threads, stats);
    } else if (test_type == "throughput") {
        std::cout << "吞吐量测试，持续时间: " << duration << " 秒" << std::endl;
        throughput_test(client, method, duration, stats);
    } else {
        std::cerr << "未知的测试类型: " << test_type << std::endl;
        std::cerr << "支持的类型: single, multi, throughput" << std::endl;
        return -1;
    }

    stats.print();
    return 0;
}
