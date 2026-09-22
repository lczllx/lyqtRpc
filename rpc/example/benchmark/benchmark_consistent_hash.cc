// benchmark_consistent_hash.cc — 一致性哈希负载均衡微基准
// 不依赖网络，直接压 MethodHost，量化四项指标：
//   1) selectHost 单次延迟（CONSISTENT_HASH vs SOURCE_HASH vs ROUND_ROBIN）
//   2) 哈希环构建耗时（appendHost 触发 160 虚拟节点全量重建，观察 O(N) 特征）
//   3) key 分布均衡性（10 台主机 10w key 的命中标准差 / max:min）
//   4) 增删节点最小重映射（对比取模哈希 SOURCE_HASH，量化"~1/N vs ~(N-1)/N"）
#include "src/client/rpc_registry.hpp"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

using lcz_rpc::client::MethodHost;
using lcz_rpc::HostInfo;
using lcz_rpc::LoadBalanceStrategy;

namespace
{
    void fillHosts(MethodHost &mh, int n)
    {
        for (int i = 1; i <= n; ++i)
            mh.appendHost(HostInfo("10.0.0." + std::to_string(i), 8080), 0);
    }

    std::vector<std::string> makeKeys(int n)
    {
        std::vector<std::string> keys;
        keys.reserve(n);
        for (int i = 0; i < n; ++i)
            keys.emplace_back("user-" + std::to_string(i));
        return keys;
    }

    // 测 selectHost 单次延迟：复用 key 池循环 ITERS 次，返回 ns/op 并打印
    void benchSelect(MethodHost &mh, LoadBalanceStrategy strategy, const char *name,
                     const std::vector<std::string> &keys, int iters)
    {
        uint64_t sink = 0;
        auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < iters; ++i)
            sink += mh.selectHost(strategy, keys[i % keys.size()]).host.second;
        auto t1 = std::chrono::steady_clock::now();
        (void)sink;
        double ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count() / (double)iters;
        double ops = 1e9 / ns;
        std::cout << "  " << std::left << std::setw(18) << name << std::right
                  << std::setw(9) << std::fixed << std::setprecision(1) << ns << " ns/op   "
                  << std::setw(11) << std::setprecision(0) << ops << " ops/s\n";
    }

    void benchRebuild()
    {
        std::cout << "[2] 哈希环构建耗时（appendHost 触发 160 虚拟节点全量重建）\n";
        MethodHost mh;
        for (int n = 1; n <= 10; ++n)
        {
            auto t0 = std::chrono::steady_clock::now();
            mh.appendHost(HostInfo("10.0.0." + std::to_string(n), 8080), 0);
            auto t1 = std::chrono::steady_clock::now();
            double us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            std::cout << "    第 " << std::setw(2) << n << " 台 appendHost（重建 160×" << n
                      << " 虚拟节点）: " << std::setw(6) << std::fixed << std::setprecision(1)
                      << us << " us\n";
        }
    }

    void benchDistribution()
    {
        const int HOSTS = 10, KEYS = 100000;
        MethodHost mh;
        fillHosts(mh, HOSTS);
        auto keys = makeKeys(KEYS);

        std::unordered_map<std::string, int> cnt;
        for (auto &k : keys)
            cnt[mh.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, k).host.first]++;

        double mean = KEYS / (double)HOSTS;
        double var = 0;
        int mn = std::numeric_limits<int>::max(), mx = 0;
        for (auto &kv : cnt)
        {
            double d = kv.second - mean;
            var += d * d;
            mn = std::min(mn, kv.second);
            mx = std::max(mx, kv.second);
        }
        double sd = std::sqrt(var / cnt.size());
        std::cout << "  主机数=" << HOSTS << " key=" << KEYS
                  << " 每台均值=" << std::fixed << std::setprecision(0) << mean
                  << " 标准差=" << std::setprecision(1) << sd
                  << " max:min=" << std::setprecision(2) << (double)mx / mn << "\n";
    }

    // 统计加/删节点后 key 重映射数量，返回重映射个数
    int countRemap(MethodHost &mh, LoadBalanceStrategy strategy,
                   const std::vector<std::string> &keys, const std::vector<std::string> &before)
    {
        int remapped = 0;
        for (size_t i = 0; i < keys.size(); ++i)
            if (mh.selectHost(strategy, keys[i]).host.first != before[i])
                ++remapped;
        return remapped;
    }

    void benchRemap()
    {
        const int KEYS = 10000;
        auto keys = makeKeys(KEYS);

        std::cout << "[4] 增删节点最小重映射（对比取模哈希 SOURCE_HASH）\n";

        // 加 1 台：4 → 5
        MethodHost ch_add, sh_add;
        fillHosts(ch_add, 4);
        fillHosts(sh_add, 4);
        std::vector<std::string> ch_before, sh_before;
        for (auto &k : keys)
        {
            ch_before.push_back(ch_add.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, k).host.first);
            sh_before.push_back(sh_add.selectHost(LoadBalanceStrategy::SOURCE_HASH, k).host.first);
        }
        ch_add.appendHost(HostInfo("10.0.0.5", 8080), 0);
        sh_add.appendHost(HostInfo("10.0.0.5", 8080), 0);
        int ch_remap = countRemap(ch_add, LoadBalanceStrategy::CONSISTENT_HASH, keys, ch_before);
        int sh_remap = countRemap(sh_add, LoadBalanceStrategy::SOURCE_HASH, keys, sh_before);
        std::cout << "  加 1 台(4→5): 一致性哈希 " << ch_remap << "/" << KEYS << " ("
                  << std::fixed << std::setprecision(1) << 100.0 * ch_remap / KEYS
                  << "%)  vs  取模哈希 " << sh_remap << "/" << KEYS << " ("
                  << 100.0 * sh_remap / KEYS << "%)\n";

        // 删 1 台：4 → 3
        MethodHost ch_del, sh_del;
        fillHosts(ch_del, 4);
        fillHosts(sh_del, 4);
        ch_before.clear();
        sh_before.clear();
        for (auto &k : keys)
        {
            ch_before.push_back(ch_del.selectHost(LoadBalanceStrategy::CONSISTENT_HASH, k).host.first);
            sh_before.push_back(sh_del.selectHost(LoadBalanceStrategy::SOURCE_HASH, k).host.first);
        }
        ch_del.removeHost(HostInfo("10.0.0.1", 8080));
        sh_del.removeHost(HostInfo("10.0.0.1", 8080));
        ch_remap = countRemap(ch_del, LoadBalanceStrategy::CONSISTENT_HASH, keys, ch_before);
        sh_remap = countRemap(sh_del, LoadBalanceStrategy::SOURCE_HASH, keys, sh_before);
        std::cout << "  删 1 台(4→3): 一致性哈希 " << ch_remap << "/" << KEYS << " ("
                  << std::fixed << std::setprecision(1) << 100.0 * ch_remap / KEYS
                  << "%)  vs  取模哈希 " << sh_remap << "/" << KEYS << " ("
                  << 100.0 * sh_remap / KEYS << "%)\n";
    }
}

int main()
{
    std::cout << "========== 一致性哈希负载均衡微基准 ==========\n";

    const int HOSTS = 10;
    const int KEYS = 100000;   // key 池
    const int ITERS = 1000000; // selectHost 调用次数

    MethodHost mh;
    fillHosts(mh, HOSTS);
    auto keys = makeKeys(KEYS);

    std::cout << "[1] selectHost 单次延迟（" << HOSTS << " 台主机，" << ITERS << " 次调用）\n";
    benchSelect(mh, LoadBalanceStrategy::CONSISTENT_HASH, "CONSISTENT_HASH", keys, ITERS);
    benchSelect(mh, LoadBalanceStrategy::SOURCE_HASH, "SOURCE_HASH(取模)", keys, ITERS);
    benchSelect(mh, LoadBalanceStrategy::ROUND_ROBIN, "ROUND_ROBIN", keys, ITERS);

    std::cout << "\n";
    benchRebuild();

    std::cout << "\n[3] key 分布均衡性\n";
    benchDistribution();

    std::cout << "\n";
    benchRemap();

    std::cout << "\n==============================================\n";
    return 0;
}
