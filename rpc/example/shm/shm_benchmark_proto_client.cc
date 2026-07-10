// SHM + Protobuf 零拷贝 压测 Client（真并发，每线程独立 client）
#include "src/client/shm_client_proto.hpp"
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
#include <atomic>

class ProtoStats {
public:
    std::vector<double> latencies;
    int success_count = 0, fail_count = 0;
    std::chrono::steady_clock::time_point start_time, end_time;
    void record(double l, bool ok) { if (ok) { latencies.push_back(l); success_count++; } else fail_count++; }
    void merge(const ProtoStats& o) {
        success_count += o.success_count; fail_count += o.fail_count;
        latencies.insert(latencies.end(), o.latencies.begin(), o.latencies.end());
    }
    void print(const std::string& title) {
        if (latencies.empty()) { std::cout << "没有成功的请求" << std::endl; return; }
        std::sort(latencies.begin(), latencies.end());
        auto d = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
        if (d == 0) d = 1;
        size_t n = latencies.size();
        std::cout << "\n========== " << title << " ==========" << std::endl;
        std::cout << "总请求: " << (success_count+fail_count) << "  成功: " << success_count
                  << "  失败: " << fail_count << "  成功率: " << std::fixed << std::setprecision(2)
                  << (success_count>0?success_count*100.0/(success_count+fail_count):0) << "%" << std::endl;
        std::cout << "耗时: " << d << " ms  QPS: " << std::fixed << std::setprecision(2)
                  << (success_count*1000.0/d) << std::endl;
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

void proto_worker(const std::string& notify_path, const std::string& method,
                  const std::string& payload, int num_requests, ProtoStats& stats) {
    lcz_rpc::ShmClientProto client(notify_path);
    std::mutex mtx; std::condition_variable cv;
    bool got_resp = false; std::string rid;

    client.setMessageCallback([&](const lcz_rpc::BaseConnection::ptr&,
                                   lcz_rpc::BaseMessage::ptr& msg) {
        auto resp = std::dynamic_pointer_cast<lcz_rpc::ProtoRpcResponse>(msg);
        if (!resp) return;
        std::lock_guard<std::mutex> lk(mtx);
        if (resp->rid() == rid) { got_resp = true; cv.notify_one(); }
    });
    client.connect();
    if (!client.connected()) { stats.fail_count += num_requests; return; }

    lcz_rpc::proto::AddRequest add_req;
    add_req.set_num1(10); add_req.set_num2(20);
    std::string add_body = add_req.SerializeAsString();

    for (int i = 0; i < num_requests; ++i) {
        auto req = lcz_rpc::MessageFactory::create<lcz_rpc::ProtoRpcRequest>();
        { std::lock_guard<std::mutex> lk(mtx); rid = uuid(); req->setId(rid); got_resp = false; }
        req->setMsgType(lcz_rpc::MsgType::REQ_RPC_PROTO);
        req->setMethod(method);
        req->setBody(method == "add" ? add_body : payload);

        auto start = std::chrono::steady_clock::now();
        if (!client.send(req)) { stats.record(-1, false); continue; }
        { std::unique_lock<std::mutex> lk(mtx); cv.wait(lk, [&]{ return got_resp; }); }
        auto end = std::chrono::steady_clock::now();
        stats.record(std::chrono::duration_cast<std::chrono::microseconds>(end-start).count(), true);
    }
    client.shutdown();
}

int main(int argc, char* argv[]) {
    lcz::LoggerManager::getInstance().rootLogger()->setLevel(lcz::LogLevel::value::FATAL);
    std::string test_type = "single", method = "add";
    int requests = 10000, threads = 4, duration = 10, payload_size = 16;
    if (argc > 1) test_type = argv[1];
    if (argc > 2) method    = argv[2];
    if (argc > 3) requests  = std::atoi(argv[3]);
    if (argc > 4) threads   = std::atoi(argv[4]);
    if (argc > 5) duration  = std::atoi(argv[5]);
    if (argc > 6) payload_size = std::atoi(argv[6]);

    std::string payload = std::string(payload_size, 'x');

    std::cout << "========== SHM Proto 零拷贝 RPC 性能测试 ==========" << std::endl;
    std::cout << "测试类型: " << test_type << "  方法: " << method;
    if (method == "echo") std::cout << "  载荷: " << payload_size << "B";
    std::cout << std::endl;

    ProtoStats stats;

    if (test_type == "single") {
        stats.start_time = std::chrono::steady_clock::now();
        proto_worker("lcz_shm_proto_bench_notify", method, payload, requests, stats);
        stats.end_time = std::chrono::steady_clock::now();
    } else if (test_type == "multi") {
        int per = requests / threads;
        std::vector<std::thread> ths;
        std::vector<ProtoStats> tstats(threads);
        stats.start_time = std::chrono::steady_clock::now();
        for (int t=0; t<threads; ++t)
            ths.emplace_back([&,t](){ proto_worker("lcz_shm_proto_bench_notify",method,payload,per,tstats[t]); });
        for (auto& th: ths) th.join();
        stats.end_time = std::chrono::steady_clock::now();
        for (auto& ts: tstats) stats.merge(ts);
    } else if (test_type == "throughput") {
        lcz_rpc::ShmClientProto client("lcz_shm_proto_bench_notify");
        std::mutex mtx; std::condition_variable cv;
        bool got_resp = false; std::string rid;
        client.setMessageCallback([&](const lcz_rpc::BaseConnection::ptr&,
                                       lcz_rpc::BaseMessage::ptr& msg) {
            auto resp = std::dynamic_pointer_cast<lcz_rpc::ProtoRpcResponse>(msg);
            if (!resp) return;
            std::lock_guard<std::mutex> lk(mtx);
            if (resp->rid() == rid) { got_resp = true; cv.notify_one(); }
        });
        client.connect();
        if (!client.connected()) { stats.fail_count=1; stats.print("Proto perf"); return 1; }
        auto end_time = std::chrono::steady_clock::now() + std::chrono::seconds(duration);
        stats.start_time = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() < end_time) {
            auto req = lcz_rpc::MessageFactory::create<lcz_rpc::ProtoRpcRequest>();
            { std::lock_guard<std::mutex> lk(mtx); rid = uuid(); req->setId(rid); got_resp = false; }
            req->setMsgType(lcz_rpc::MsgType::REQ_RPC_PROTO);
            req->setMethod(method);
            req->setBody(method == "add" ? payload : payload);
            auto t1 = std::chrono::steady_clock::now();
            if (!client.send(req)) { stats.record(-1,false); continue; }
            { std::unique_lock<std::mutex> lk(mtx); cv.wait(lk,[&]{return got_resp;}); }
            auto t2 = std::chrono::steady_clock::now();
            stats.record(std::chrono::duration_cast<std::chrono::microseconds>(t2-t1).count(),true);
        }
        stats.end_time = std::chrono::steady_clock::now();
        client.shutdown();
    }
    stats.print("SHM Proto 零拷贝 RPC 性能测试结果");
    return 0;
}
