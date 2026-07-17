#pragma once
// =============================================================================
// process_metrics.hpp — 进程级指标（对标 brpc /vars 的 process_* / system_*）
// =============================================================================
// 全部从 /proc 采集，无第三方依赖，仅 Linux。
// 采集时机：每次 /metrics scrape 前调用 ProcessMetrics::collect()，
// 而不是后台定时器——scrape 间隔(15s)天然就是采样间隔，且无请求时零开销。
//
// 指标名遵循 Prometheus 官方命名约定（与 client_golang 默认进程指标一致），
// 便于直接复用 Grafana 社区面板：
//   process_cpu_seconds_total     — 进程累计 CPU 时间(user+sys)，counter
//   process_resident_memory_bytes — RSS，对标 brpc process_memory_resident
//   process_virtual_memory_bytes  — VSZ，对标 brpc process_memory_virtual
//   process_open_fds              — 打开 fd 数，对标 brpc process_fd_count
//   process_threads               — 线程数，对标 brpc process_thread_count
//   system_loadavg_1m             — 系统 1 分钟负载，对标 brpc system_loadavg_1m
// =============================================================================
#include "metrics.hpp"
#include <fstream>
#include <sstream>
#include <string>
#include <dirent.h>
#include <unistd.h>

namespace lcz_rpc
{
    namespace metrics
    {

        class ProcessMetrics
        {
        public:
            // 在每次 /metrics scrape 前调用，刷新全部进程级指标
            static void collect()
            {
                collectStat();    // CPU
                collectStatus();  // 内存 + 线程数
                collectFds();     // fd 数
                collectLoadavg(); // 系统负载
            }

        private:
            // /proc/self/stat 第 14/15 字段: utime/stime（单位 jiffies）
            static void collectStat()
            {
                std::ifstream ifs("/proc/self/stat");
                if (!ifs)
                    return;
                std::string line;
                std::getline(ifs, line);
                // 进程名 comm 可能含空格，以 ')' 之后为准切分
                auto pos = line.rfind(')');
                if (pos == std::string::npos)
                    return;
                std::istringstream ss(line.substr(pos + 2));
                std::string field;
                long utime = 0, stime = 0;
                // ')' 后第 12/13 个字段即 utime/stime（stat 全局第 14/15 字段）
                for (int i = 1; i <= 13 && ss >> field; ++i)
                {
                    if (i == 12)
                        utime = std::stol(field);
                    if (i == 13)
                        stime = std::stol(field);
                }
                static const double hz = static_cast<double>(sysconf(_SC_CLK_TCK));
                METRICS_GAUGE("process_cpu_seconds_total",
                              "Total user+system CPU time in seconds", {})
                    .set((utime + stime) / hz);
            }

            // /proc/self/status: VmRSS/VmSize（单位 kB）/ Threads
            static void collectStatus()
            {
                std::ifstream ifs("/proc/self/status");
                if (!ifs)
                    return;
                std::string key;
                long value = 0;
                std::string unit;
                while (ifs >> key)
                {
                    if (key == "VmRSS:")
                    {
                        ifs >> value >> unit;
                        METRICS_GAUGE("process_resident_memory_bytes",
                                      "Resident memory (RSS) in bytes", {})
                            .set(value * 1024.0);
                    }
                    else if (key == "VmSize:")
                    {
                        ifs >> value >> unit;
                        METRICS_GAUGE("process_virtual_memory_bytes",
                                      "Virtual memory (VSZ) in bytes", {})
                            .set(value * 1024.0);
                    }
                    else if (key == "Threads:")
                    {
                        ifs >> value;
                        METRICS_GAUGE("process_threads", "Thread count", {}).set(value);
                    }
                    else
                    {
                        ifs.ignore(256, '\n'); // 跳过不关心的行
                    }
                }
            }

            // 遍历 /proc/self/fd 计数
            static void collectFds()
            {
                DIR *d = opendir("/proc/self/fd");
                if (!d)
                    return;
                int count = 0;
                while (readdir(d))
                    ++count;
                closedir(d);
                // 减去 "." ".." 和 opendir 自身占用的 fd
                METRICS_GAUGE("process_open_fds", "Open file descriptors", {}).set(count - 3);
            }

            // /proc/loadavg 第 1 字段: 1 分钟负载
            static void collectLoadavg()
            {
                std::ifstream ifs("/proc/loadavg");
                if (!ifs)
                    return;
                double load1 = 0;
                ifs >> load1;
                METRICS_GAUGE("system_loadavg_1m", "System 1-minute load average", {}).set(load1);
            }
        };

    } // namespace metrics
} // namespace lcz_rpc
