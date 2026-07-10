#pragma once
// =============================================================================
// shm_client_zc.hpp — 零拷贝 SHM Client，多客户端
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
#include <thread>
#include <atomic>

namespace lcz_rpc {

class ShmClientZc : public BaseClient {
public:
    ShmClientZc(const std::string& notify_path = "lcz_shm_zc_notify")
        : _notify_path(notify_path) {}

    void connect() override {
        int conn_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (conn_fd < 0) {
            LCZ_ERROR("[ShmClientZc] socket failed errno=%d", errno); return;
        }
        struct sockaddr_un addr = {};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, _notify_path.c_str(), sizeof(addr.sun_path) - 1);
        if (::connect(conn_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            LCZ_ERROR("[ShmClientZc] connect %s failed errno=%d", _notify_path.c_str(), errno);
            close(conn_fd); return;
        }

        int req_fd = -1, resp_fd = -1;
        if (!ShmChannel::handshake_client(conn_fd, req_fd, resp_fd, _shm_name)) {
            LCZ_ERROR("[ShmClientZc] handshake failed");
            close(conn_fd); return;
        }
        close(conn_fd);

        if (!_channel.open(_shm_name)) {
            LCZ_ERROR("[ShmClientZc] open %s failed", _shm_name.c_str()); return;
        }
        _channel.set_req_notify_fd(req_fd);
        _channel.set_resp_notify_fd(resp_fd);

        auto conn = std::make_shared<ShmConnection>();
        conn->setName("shm_client_zc_" + _shm_name);
        conn->setSender([this](const BaseMessage::ptr& msg) { send(msg); });
        _conn = conn;
        if (_cb_connection) _cb_connection(conn);

        _worker = std::thread([this]() { responseLoop(); });
        LCZ_INFO("[ShmClientZc] connected, shm=%s", _shm_name.c_str());
    }

    bool send(const BaseMessage::ptr& msg) override {
        auto req = std::dynamic_pointer_cast<RpcRequest>(msg);
        if (!req) return false;

        flatbuffers::FlatBufferBuilder builder(256);
        auto id       = builder.CreateString(req->rid());
        auto method   = builder.CreateString(req->method());
        auto trace_id = builder.CreateString(req->trace_id());
        std::string params_json;
        JSON::serialize(req->params(), params_json);
        auto params_vec = builder.CreateVector(
            reinterpret_cast<const uint8_t*>(params_json.data()), params_json.size());
        auto root = fb::CreateRpcRequest(builder, id, method, trace_id, params_vec);
        builder.Finish(root);

        std::string body(reinterpret_cast<const char*>(builder.GetBufferPointer()),
                         builder.GetSize());
        bool ok = _channel.write_request(body, MsgType::REQ_RPC_FLAT);
        if (ok) _channel.notify_req();
        return ok;
    }

    void shutdown() override {
        _running = false;
        if (_worker.joinable()) _worker.join();
        _channel.destroy();
    }

    BaseConnection::ptr connection() override { return _conn; }
    bool connected() override { return _channel.is_open(); }
    ~ShmClientZc() { shutdown(); }

private:
    void responseLoop() {
        int epfd = epoll_create1(0);
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = _channel.resp_notify_fd();
        epoll_ctl(epfd, EPOLL_CTL_ADD, _channel.resp_notify_fd(), &ev);

        _running = true;
        std::string body; MsgType type;
        const int resp_fd = _channel.resp_notify_fd();
        LCZ_INFO("[ShmClientZc] responseLoop started, resp_fd=%d", resp_fd);
        while (_running) {
            struct epoll_event events[1];
            int n = epoll_wait(epfd, events, 1, 500);
            if (n < 0) break;
            if (n > 0) { uint64_t val; (void)::read(resp_fd, &val, sizeof(val)); }

            while (_channel.read_response(body, type)) {
                if (type != MsgType::RSP_RPC_FLAT) continue;
                ShmZcReader reader(body);
                auto* fresp = reader.as<lcz_rpc::fb::RpcResponse>();
                if (!fresp) continue;

                auto json_resp = MessageFactory::create<RpcResponse>();
                json_resp->setId(ShmZcReader::strval(fresp->id()));
                json_resp->setRcode(static_cast<RespCode>(fresp->rcode()));
                json_resp->setMsgType(MsgType::RSP_RPC);
                if (fresp->result() && fresp->result()->size() > 0) {
                    std::string rs(reinterpret_cast<const char*>(fresp->result()->data()),
                                   fresp->result()->size());
                    Json::Value rv;
                    if (JSON::deserialize(rs, rv)) json_resp->setResult(rv);
                }
                if (_cb_message) {
                    BaseMessage::ptr base_msg = json_resp;
                    _cb_message(_conn, base_msg);
                }
            }
        }
        close(epfd);
    }

    ShmChannel         _channel;
    ShmConnection::ptr _conn;
    std::string        _notify_path;
    std::string        _shm_name;
    std::thread        _worker;
    std::atomic<bool>  _running{false};
};

} // namespace lcz_rpc
