#pragma once
// =============================================================================
// metrics_server.hpp — 基于 muduo EventLoop 的 /metrics HTTP 端点
// =============================================================================
// 职责：起一个独立后台线程，监听 :9090，应答 Prometheus 的 scrape 请求
// （即 curl http://localhost:9090/metrics），返回 Registry 里所有指标的文本快照。
//
// 为什么用 muduo EventLoop 而不是手写 while+accept：
//   手写阻塞 accept() 在 stop() 时无法被唤醒，join() 会永久卡死
//   （SHM server 的 CI 死锁就是这个问题，见 docs/shm-serialization-pitfalls.md）。
//   muduo 的 loop.quit() 线程安全且能随时唤醒 loop()，退出干净。
//
// 线程模型：
//   - start() 创建 1 个后台线程，线程内构造 EventLoop 并 loop()（muduo 要求
//     EventLoop 构造与 loop() 在同一线程，所以不能在主线程构造再传进去）
//   - 与 RPC 数据面完全隔离：scrape 只读 Registry 里的 atomic，不碰业务线程
//   - 进程内所有指标共享一个 Registry 单例，因此一个进程只需要一个 MetricsServer
//
// 用法:
//   MetricsServer::start(9090);  // 幂等，重复调用只生效一次
//   MetricsServer::stop();       // quit + join；进程退出前必须调用，
//                                // 否则静态 _thread 析构时仍 joinable → std::terminate
// =============================================================================
#include "metrics.hpp"
#include "metrics_hooks.hpp"
#include "process_metrics.hpp"
#include <muduo/net/EventLoop.h>
#include <muduo/net/Channel.h>
#include <muduo/net/InetAddress.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <sstream>
#include <cstring>

namespace lcz_rpc
{
    namespace metrics
    {

        class MetricsServer
        {
        public:
            // 启动 /metrics 端点（后台线程，立即返回）
            // exchange 保证幂等：已启动时再次调用直接返回，不会起第二个线程
            static void start(int port = 9090)
            {
                if (_running.exchange(true))
                    return;
                _thread = std::thread([port]()
                                      { serve(port); });
            }

            // 停止端点并回收线程
            // quit() 是 muduo 提供的线程安全接口，会唤醒阻塞中的 loop()
            static void stop()
            {
                _running.store(false);
                if (_loop)
                    _loop->quit(); // 线程安全，唤醒 loop()
                if (_thread.joinable())
                    _thread.join();
            }

        private:
            // 后台线程主体：建监听 socket → 注册进 EventLoop → 事件循环应答 scrape
            static void serve(int port)
            {
                // EventLoop 必须在本线程构造（muduo 线程亲和性要求）
                // 暴露给 stop() 用于跨线程 quit()
                muduo::net::EventLoop loop;
                _loop = &loop;

                // ---- 标准 socket 三件套：socket → bind → listen ----
                int fd = socket(AF_INET, SOCK_STREAM, 0);
                if (fd < 0)
                    return;
                int opt = 1;
                // SO_REUSEADDR：进程重启时跳过 TIME_WAIT，避免 "Address already in use"
                setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

                struct sockaddr_in addr = {};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(static_cast<uint16_t>(port));
                addr.sin_addr.s_addr = INADDR_ANY; // 监听所有网卡，容器内也能被 scrape
                if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
                {
                    // 必须出声：静默失败时使用者以为指标在跑，curl 打到的
                    // 可能是同端口的其它进程（常见于 9090 被占用）
                    fprintf(stderr, "[MetricsServer] bind :%d 失败(%s)，/metrics 端点未启动\n",
                            port, strerror(errno));
                    close(fd);
                    return;
                }
                // backlog=5 足够：Prometheus 15s 才来一次，不存在并发 scrape 洪峰
                if (listen(fd, 5) < 0)
                {
                    fprintf(stderr, "[MetricsServer] listen :%d 失败(%s)，/metrics 端点未启动\n",
                            port, strerror(errno));
                    close(fd);
                    return;
                }

                // 把监听 fd 包成 muduo Channel 挂到 loop 上：
                // 有新连接可读时（客户端 connect 完成）触发下面的回调，
                // 平时线程阻塞在 epoll_wait 里，零 CPU 占用
                muduo::net::Channel ch(&loop, fd);
                ch.setReadCallback([&](muduo::Timestamp)
                                   {
            // 一次 scrape 的完整应答流程（短连接：应答完立即 close）
            int cli = accept(fd, nullptr, nullptr);
            if (cli < 0) return;

            // 只读请求行足够判断路径，不需要完整解析 HTTP
            char buf[4096];
            ssize_t n = recv(cli, buf, sizeof(buf) - 1, 0);
            if (n > 0) {
                buf[n] = '\0';
                if (strncmp(buf, "GET /metrics", 12) == 0) {
                    // scrape 前先刷新"惰性"指标——它们没有事件驱动的写入时机：
                    // ① uptime：steady_clock 差值（不能在 exportText 持锁时算，故放这里）
                    METRICS_GAUGE("rpc_server_uptime_seconds",
                                  "Server uptime in seconds", {}).set(MetricHooks::getUptime());
                    // ② 进程级指标：读 /proc 采集 CPU/内存/fd/线程/负载
                    //    放在 scrape 时采而不是后台定时器：无人拉取时零开销
                    ProcessMetrics::collect();
                    // ③ 导出 Registry 里全部指标为 Prometheus 文本格式 0.0.4
                    std::string body = Registry::instance().exportText();
                    // 手拼最简 HTTP 响应；Content-Type 里的 version=0.0.4
                    // 是 Prometheus 文本协议版本号，Prometheus 靠它识别格式
                    std::ostringstream resp;
                    resp << "HTTP/1.1 200 OK\r\n"
                         << "Content-Type: text/plain; version=0.0.4\r\n"
                         << "Content-Length: " << body.size() << "\r\n"
                         << "\r\n" << body;
                    std::string r = resp.str();
                    send(cli, r.data(), r.size(), 0);
                } else {
                    // 非 /metrics 路径一律 404（本端点只服务 Prometheus）
                    const char* nf = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
                    send(cli, nf, strlen(nf), 0);
                }
            }
            close(cli); });
                ch.enableReading(); // 注册 EPOLLIN，开始接收连接

                loop.loop(); // 阻塞在此，直到 stop() 调用 quit()
                close(fd);   // loop 退出后清理监听 fd
            }

            inline static std::thread _thread;              // 后台服务线程
            inline static std::atomic<bool> _running{false}; // 防重复 start 的标志
            inline static muduo::net::EventLoop *_loop = nullptr; // 供 stop() 跨线程 quit
        };

    } // namespace metrics
} // namespace lcz_rpc
