#pragma once
// =============================================================================
// shm_server_zc.hpp — FlatBuffers 零拷贝 SHM Server，worker 线程池
// =============================================================================

#include "../general/abstract.hpp"
#include "../general/shm_channel.hpp"
#include "../general/shm_connection.hpp"
#include "../general/shm_zc_adaptor.hpp"
#include "../general/message.hpp"
#include "../general/log_system/lcz_log.h"
#include "rpc_message_generated.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <atomic>
#include <thread>
#include <vector>
#include <unordered_map>
#include <mutex>

namespace lcz_rpc {

class ShmServerZc : public BaseServer {
public:
    ShmServerZc(const std::string& notify_path = "lcz_shm_zc_notify",
                const std::string& shm_prefix  = "lcz_shm_zc",
                size_t req_size  = 64 * 1024 * 1024,
                size_t resp_size = 64 * 1024 * 1024,
                int    max_clients = 64,
                int    worker_threads = 4)
        : _notify_path(notify_path), _shm_prefix(shm_prefix),
          _req_size(req_size), _resp_size(resp_size),
          _max_clients(max_clients), _worker_count(worker_threads) {}

    void start() override {
        int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (listen_fd < 0) { LCZ_ERROR("[ShmServerZc] socket failed"); return; }
        struct sockaddr_un addr = {};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, _notify_path.c_str(), sizeof(addr.sun_path) - 1);
        unlink(_notify_path.c_str());
        if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0
            || listen(listen_fd, _max_clients) < 0) {
            LCZ_ERROR("[ShmServerZc] bind/listen failed"); close(listen_fd); return;
        }

        _running = true;
        for (int i = 0; i < _worker_count; ++i) {
            auto w = std::make_unique<Worker>();
            w->epfd = epoll_create1(0);
            pipe(w->wake_pipe);
            fcntl(w->wake_pipe[0], F_SETFL, O_NONBLOCK);
            struct epoll_event ev;
            ev.events = EPOLLIN; ev.data.fd = w->wake_pipe[0];
            epoll_ctl(w->epfd, EPOLL_CTL_ADD, w->wake_pipe[0], &ev);
            _workers.push_back(std::move(w));
        }
        for (int i = 0; i < _worker_count; ++i) {
            _workers[i]->thread = std::thread(&ShmServerZc::workerLoop, this, i);
        }

        LCZ_INFO("[ShmServerZc] listening on %s, workers=%d", _notify_path.c_str(), _worker_count);

        int next_id = 0, round_robin = 0;
        while (_running) {
            int conn_fd = accept(listen_fd, nullptr, nullptr);
            if (conn_fd < 0) break;
            if (next_id >= _max_clients) { close(conn_fd); continue; }

            std::string shm_name = _shm_prefix + "_" + std::to_string(next_id++);
            auto entry = std::make_shared<ClientEntry>();
            if (!entry->channel.create(shm_name, _req_size, _resp_size)) {
                close(conn_fd); continue;
            }
            int req_fd = -1, resp_fd = -1;
            if (!ShmChannel::handshake_server(conn_fd, req_fd, resp_fd, shm_name)) {
                entry->channel.destroy(); close(conn_fd); continue;
            }
            close(conn_fd);
            entry->channel.set_req_notify_fd(req_fd);
            entry->channel.set_resp_notify_fd(resp_fd);

            auto conn = std::make_shared<ShmConnection>();
            conn->setName("shm_server_zc_" + std::to_string(next_id - 1));
            conn->setSender([entry](const BaseMessage::ptr& msg) {
                auto resp = std::dynamic_pointer_cast<RpcResponse>(msg);
                if (!resp) return;
                flatbuffers::FlatBufferBuilder builder(256);
                auto id = builder.CreateString(resp->rid());
                int rcode = static_cast<int>(resp->rcode());
                std::string result_json;
                JSON::serialize(resp->result(), result_json);
                auto rv = builder.CreateVector(
                    reinterpret_cast<const uint8_t*>(result_json.data()), result_json.size());
                builder.Finish(fb::CreateRpcResponse(builder, id, rcode, rv));
                std::string body(reinterpret_cast<const char*>(builder.GetBufferPointer()),
                                 builder.GetSize());
                entry->channel.write_response(body, MsgType::RSP_RPC_FLAT);
                entry->channel.notify_resp();
            });
            entry->conn = conn;
            if (_cb_connection) _cb_connection(conn);

            int wid = round_robin++ % _worker_count;
            auto& w = _workers[wid];
            { std::lock_guard<std::mutex> lk(w->mtx); w->clients[req_fd] = entry; }
            ssize_t __attribute__((unused)) wr = ::write(w->wake_pipe[1], &req_fd, sizeof(req_fd));
            LCZ_INFO("[ShmServerZc] client %d -> worker %d, shm=%s", next_id-1, wid, shm_name.c_str());
        }
        close(listen_fd); unlink(_notify_path.c_str());
        for (auto& w : _workers) { if (w->thread.joinable()) w->thread.join(); }
    }

    void stop() override { _running = false; }
    ~ShmServerZc() {
        _running = false;
        for (auto& w : _workers) close(w->wake_pipe[1]);
        for (auto& w : _workers)
            for (auto& [fd, e] : w->clients) e->channel.destroy();
    }

private:
    struct ClientEntry {
        ShmChannel         channel;
        ShmConnection::ptr conn;
    };
    struct Worker {
        int epfd = -1, wake_pipe[2] = {-1, -1};
        std::thread thread;
        std::mutex  mtx;
        std::unordered_map<int, std::shared_ptr<ClientEntry>> clients;
    };

    void workerLoop(int id) {
        auto& w = _workers[id];
        const int MAX = 64;
        struct epoll_event events[MAX];
        std::string body; MsgType type;
        while (_running) {
            int n = epoll_wait(w->epfd, events, MAX, 500);
            if (n < 0) break;
            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;
                if (fd == w->wake_pipe[0]) {
                    int new_fd;
                    while (::read(w->wake_pipe[0], &new_fd, sizeof(new_fd)) > 0) {
                        struct epoll_event ev;
                        ev.events = EPOLLIN; ev.data.fd = new_fd;
                        epoll_ctl(w->epfd, EPOLL_CTL_ADD, new_fd, &ev);
                    }
                } else {
                    auto entry = [&]() -> std::shared_ptr<ClientEntry> {
                        std::lock_guard<std::mutex> lk(w->mtx);
                        auto it = w->clients.find(fd); return it != w->clients.end() ? it->second : nullptr;
                    }();
                    if (!entry) continue;
                    uint64_t val; (void)::read(fd, &val, sizeof(val));
                    while (entry->channel.read_request(body, type)) {
                        if (type != MsgType::REQ_RPC_FLAT) continue;
                        ShmZcReader reader(body);
                        auto* req = reader.as<fb::RpcRequest>();
                        if (!req) continue;
                        auto json_req = MessageFactory::create<RpcRequest>();
                        json_req->setId(ShmZcReader::strval(req->id()));
                        json_req->setMethod(ShmZcReader::strval(req->method()));
                        json_req->setTraceId(ShmZcReader::strval(req->trace_id()));
                        json_req->setMsgType(MsgType::REQ_RPC);
                        if (req->params() && req->params()->size() > 0) {
                            std::string ps(reinterpret_cast<const char*>(req->params()->data()),
                                           req->params()->size());
                            Json::Value pv;
                            if (JSON::deserialize(ps, pv)) json_req->setParams(pv);
                        }
                        if (_cb_message) { BaseMessage::ptr bm = json_req; _cb_message(entry->conn, bm); }
                    }
                }
            }
        }
        close(w->epfd);
    }

    std::string _notify_path, _shm_prefix;
    size_t _req_size, _resp_size;
    int _max_clients, _worker_count;
    std::atomic<bool> _running{false};
    std::vector<std::unique_ptr<Worker>> _workers;
};

} // namespace lcz_rpc
