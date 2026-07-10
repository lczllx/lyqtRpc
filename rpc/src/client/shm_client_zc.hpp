#pragma once
// =============================================================================
// shm_client_zc.hpp — 零拷贝 SHM Client（FlatBuffers，读端零拷贝）
// =============================================================================
// 写端: FlatBufferBuilder 构建 → write_request 写入 ring buffer
// 读端: ShmZcReader 零拷贝映射 FlatBuffers 对象，无 deserialize
// =============================================================================

#include "../general/abstract.hpp"
#include "../general/shm_channel.hpp"
#include "../general/shm_connection.hpp"
#include "../general/shm_zc_adaptor.hpp"
#include "../general/message.hpp"
#include "../general/log_system/lcz_log.h"
#include "rpc_message_generated.h"
#include <sys/epoll.h>
#include <unistd.h>
#include <thread>
#include <atomic>

namespace lcz_rpc {

class ShmClientZc : public BaseClient {
public:
    ShmClientZc(const std::string& shm_name   = "lcz_shm_zc",
                const std::string& notify_path = "lcz_shm_zc_notify")
        : _shm_name(shm_name), _notify_path(notify_path) {}

    void connect() override {
        if (!_channel.open(_shm_name)) {
            LCZ_ERROR("[ShmClientZc] connect failed"); return;
        }
        if (!_channel.setup_notify_client(_notify_path)) {
            LCZ_ERROR("[ShmClientZc] notify setup failed");
            _channel.destroy(); return;
        }

        auto conn = std::make_shared<ShmConnection>();
        conn->setName("shm_client_zc");
        conn->setSender([this](const BaseMessage::ptr& msg) { send(msg); });
        _conn = conn;
        if (_cb_connection) _cb_connection(conn);

        _worker = std::thread([this]() { responseLoop(); });
        LCZ_INFO("[ShmClientZc] connected");
    }

    bool send(const BaseMessage::ptr& msg) override {
        auto req = std::dynamic_pointer_cast<RpcRequest>(msg);
        if (!req) { LCZ_ERROR("[ShmClientZc] not RpcRequest"); return false; }

        // FlatBufferBuilder 堆上构建（消息体小，避免 allocator 复杂度）
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

        // 写入 ring buffer（一次拷贝，FlatBuf 紧凑）
        std::string body(reinterpret_cast<const char*>(builder.GetBufferPointer()),
                         builder.GetSize());
        bool ok = _channel.write_request(body, MsgType::REQ_RPC_FLAT);
        if (ok) _channel.notify_req();
        LCZ_DEBUG("[ShmClientZc] sent request size=%u rid=%s", builder.GetSize(), req->rid().c_str());
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
            if (n > 0) { uint64_t val; ssize_t __attribute__((unused)) rd = ::read(resp_fd, &val, sizeof(val)); }

            while (_channel.read_response(body, type)) {
                if (type != MsgType::RSP_RPC_FLAT) continue;

                // 读端零拷贝：ShmZcReader 直接映射，不解析
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
    std::string        _shm_name, _notify_path;
    std::thread        _worker;
    std::atomic<bool>  _running{false};
};

} // namespace lcz_rpc
