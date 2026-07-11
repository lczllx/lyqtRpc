#pragma once
// =============================================================================
// shm_server.hpp — SHM Server，worker 线程池（每 worker 独立 epoll）
// =============================================================================
// start() 流程：
//   1. bind + listen Unix socket
//   2. 启动 N 个 worker 线程，每个独立 epoll 循环
//   3. 主线程 accept → handshake → round-robin 分配给 worker
//   4. worker 通过 pipe 收到新客户端 req_fd → 加入 epoll → read_request → 派发
// =============================================================================

#include "../general/abstract.hpp"
#include "../general/shm_channel.hpp"
#include "../general/shm_connection.hpp"
#include "../general/message.hpp"
#include "../general/log_system/lcz_log.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/time.h>
#include <unistd.h>
#include <atomic>
#include <thread>
#include <vector>
#include <unordered_map>
#include <mutex>

namespace lcz_rpc {

class ShmServer : public BaseServer {
public:
    ShmServer(const std::string& notify_path = "lcz_shm_notify",
              const std::string& shm_prefix  = "lcz_shm",
              size_t req_size  = 64 * 1024 * 1024,
              size_t resp_size = 64 * 1024 * 1024,
              int    max_clients = 64,
              int    worker_threads = 4)
        : _notify_path(notify_path), _shm_prefix(shm_prefix),
          _req_size(req_size), _resp_size(resp_size),
          _max_clients(max_clients), _worker_count(worker_threads) {}

    void start() override {
        // 1. bind + listen
        _listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (_listen_fd < 0) {
            LCZ_ERROR("[ShmServer] socket failed errno=%d", errno); return;
        }
        // 设置 accept 超时 500ms，stop() 后 accept 会在超时内返回而不会永远阻塞
        struct timeval tv = {0, 500000};
        setsockopt(_listen_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        struct sockaddr_un addr = {};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, _notify_path.c_str(), sizeof(addr.sun_path) - 1);
        unlink(_notify_path.c_str());
        if (bind(_listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            LCZ_ERROR("[ShmServer] bind failed errno=%d", errno);
            close(_listen_fd); return;
        }
        if (listen(_listen_fd, _max_clients) < 0) {
            LCZ_ERROR("[ShmServer] listen failed errno=%d", errno);
            close(_listen_fd); return;
        }

        // 2. 创建 worker（先加入 vector，再启动线程）
        _running = true;
        for (int i = 0; i < _worker_count; ++i) {
            auto w = std::make_unique<Worker>();
            w->epfd = epoll_create1(0);
            if (pipe(w->wake_pipe) < 0) {
                LCZ_ERROR("[ShmServer] pipe failed for worker %d", i);
                return;
            }
            // 读端非阻塞，防止 while(read)>0 死等
            fcntl(w->wake_pipe[0], F_SETFL, O_NONBLOCK);
            struct epoll_event ev;
            ev.events = EPOLLIN;
            ev.data.fd = w->wake_pipe[0];
            epoll_ctl(w->epfd, EPOLL_CTL_ADD, w->wake_pipe[0], &ev);
            _workers.push_back(std::move(w));
        }
        for (int i = 0; i < _worker_count; ++i) {
            _workers[i]->thread = std::thread(&ShmServer::workerLoop, this, i);
        }

        LCZ_INFO("[ShmServer] listening on %s, workers=%d", _notify_path.c_str(), _worker_count);

        // 3. 主线程 accept 循环
        int next_id = 0;
        int round_robin = 0;
        while (_running) {
            int conn_fd = accept(_listen_fd, nullptr, nullptr);
            if (conn_fd < 0) break;

            if (next_id >= _max_clients) {
                LCZ_ERROR("[ShmServer] max clients reached (%d)", _max_clients);
                close(conn_fd); continue;
            }

            std::string shm_name = _shm_prefix + "_" + std::to_string(next_id++);
            auto entry = std::make_shared<ClientEntry>();

            if (!entry->channel.create(shm_name, _req_size, _resp_size)) {
                LCZ_ERROR("[ShmServer] create %s failed", shm_name.c_str());
                close(conn_fd); continue;
            }

            int req_fd = -1, resp_fd = -1;
            if (!ShmChannel::handshake_server(conn_fd, req_fd, resp_fd, shm_name)) {
                LCZ_ERROR("[ShmServer] handshake %s failed", shm_name.c_str());
                entry->channel.destroy();
                close(conn_fd); continue;
            }
            close(conn_fd);

            entry->channel.set_req_notify_fd(req_fd);
            entry->channel.set_resp_notify_fd(resp_fd);

            auto conn = std::make_shared<ShmConnection>();
            conn->setName("shm_server_" + std::to_string(next_id - 1));
            conn->setSender([entry](const BaseMessage::ptr& msg) {
                std::string body = msg->serialize();
                entry->channel.write_response(body, msg->msgType());
                entry->channel.notify_resp();
            });
            entry->conn = conn;

            if (_cb_connection) _cb_connection(conn);

            // 分配给 worker（round-robin）
            int wid = round_robin++ % _worker_count;
            auto& w = _workers[wid];
            {
                std::lock_guard<std::mutex> lk(w->mtx);
                w->clients[req_fd] = entry;
            }
            ssize_t __attribute__((unused)) wr = ::write(w->wake_pipe[1], &req_fd, sizeof(req_fd));

            LCZ_INFO("[ShmServer] client %d -> worker %d, shm=%s req_fd=%d",
                     next_id - 1, wid, shm_name.c_str(), req_fd);
        }

        close(_listen_fd);
        unlink(_notify_path.c_str());

        // 等待 worker 退出
        for (auto& w : _workers) {
            if (w->thread.joinable()) w->thread.join();
        }
    }

    void stop() override {
        _running = false;
        if (_listen_fd >= 0) { close(_listen_fd); _listen_fd = -1; }
    }
    ~ShmServer() {
        stop();
        for (auto& w : _workers) {
            close(w->wake_pipe[1]);
        }
        for (auto& w : _workers) {
            for (auto& [fd, entry] : w->clients) {
                entry->channel.destroy();
            }
        }
    }

private:
    struct ClientEntry {
        ShmChannel         channel;
        ShmConnection::ptr conn;
    };

    struct Worker {
        int         epfd = -1;
        int         wake_pipe[2] = {-1, -1};
        std::thread thread;
        std::mutex  mtx;
        std::unordered_map<int, std::shared_ptr<ClientEntry>> clients;
    };

    void workerLoop(int worker_id) {
        auto& w = _workers[worker_id];
        const int MAX_EVENTS = 64;
        struct epoll_event events[MAX_EVENTS];
        std::string body; MsgType type;

        LCZ_INFO("[ShmServer] worker %d started, epfd=%d", worker_id, w->epfd);

        while (_running) {
            int n = epoll_wait(w->epfd, events, MAX_EVENTS, 500);
            if (n < 0) break;

            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;

                if (fd == w->wake_pipe[0]) {
                    int new_fd;
                    while (::read(w->wake_pipe[0], &new_fd, sizeof(new_fd)) > 0) {
                        struct epoll_event ev;
                        ev.events = EPOLLIN;
                        ev.data.fd = new_fd;
                        epoll_ctl(w->epfd, EPOLL_CTL_ADD, new_fd, &ev);
                        LCZ_DEBUG("[ShmServer] worker %d added fd=%d", worker_id, new_fd);
                    }
                } else {
                    auto entry = [&]() -> std::shared_ptr<ClientEntry> {
                        std::lock_guard<std::mutex> lk(w->mtx);
                        auto it = w->clients.find(fd);
                        return it != w->clients.end() ? it->second : nullptr;
                    }();
                    if (!entry) continue;

                    uint64_t val;
                    ssize_t __attribute__((unused)) _rd = ::read(fd, &val, sizeof(val));

                    while (entry->channel.read_request(body, type)) {
                        auto msg = MessageFactory::create(type);
                        if (msg && msg->unserialize(body)) {
                            msg->setMsgType(type);
                            if (_cb_message) _cb_message(entry->conn, msg);
                        }
                    }
                }
            }
        }
        close(w->epfd);
        LCZ_INFO("[ShmServer] worker %d stopped", worker_id);
    }

    std::string       _notify_path;
    std::string       _shm_prefix;
    size_t            _req_size, _resp_size;
    int               _max_clients;
    int               _worker_count;
    int               _listen_fd = -1;
    std::atomic<bool> _running{false};

    std::vector<std::unique_ptr<Worker>> _workers;
};

} // namespace lcz_rpc
