#pragma once
// =============================================================================
// shm_server_zc.hpp — 零拷贝 SHM Server（FlatBuffers，读端零拷贝）
// =============================================================================
// 写端: FlatBufferBuilder 构建 → write_response 写入 ring buffer
//       消息体小（RPC 请求几百字节），一次拷贝可忽略
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
#include <atomic>

namespace lcz_rpc {

class ShmServerZc : public BaseServer {
public:
    ShmServerZc(const std::string& shm_name   = "lcz_shm_zc",
                const std::string& notify_path = "lcz_shm_zc_notify",
                size_t req_size  = 64 * 1024 * 1024,
                size_t resp_size = 64 * 1024 * 1024)
        : _shm_name(shm_name), _notify_path(notify_path),
          _req_size(req_size), _resp_size(resp_size) {}

    void start() override {
        if (!_channel.create(_shm_name, _req_size, _resp_size)) {
            LCZ_ERROR("[ShmServerZc] create failed"); return;
        }
        if (!_channel.setup_notify_server(_notify_path)) {
            LCZ_ERROR("[ShmServerZc] notify setup failed");
            _channel.destroy(); return;
        }

        auto conn = std::make_shared<ShmConnection>();
        conn->setName("shm_server_zc");
        conn->setSender([this](const BaseMessage::ptr& msg) {
            auto resp = std::dynamic_pointer_cast<RpcResponse>(msg);
            if (!resp) { LCZ_ERROR("[ShmServerZc] not RpcResponse"); return; }

            // FlatBufferBuilder 堆上构建，消息体小避免 allocator 复杂度
            flatbuffers::FlatBufferBuilder builder(256);
            auto id     = builder.CreateString(resp->rid());
            int  rcode  = static_cast<int>(resp->rcode());
            std::string result_json;
            JSON::serialize(resp->result(), result_json);
            auto result_vec = builder.CreateVector(
                reinterpret_cast<const uint8_t*>(result_json.data()), result_json.size());
            auto root = fb::CreateRpcResponse(builder, id, rcode, result_vec);
            builder.Finish(root);

            // 写入 ring buffer（一次拷贝，但 FlatBuf 紧凑，远小于 JSON）
            std::string body(reinterpret_cast<const char*>(builder.GetBufferPointer()),
                             builder.GetSize());
            _channel.write_response(body, MsgType::RSP_RPC_FLAT);
            _channel.notify_resp();
            LCZ_DEBUG("[ShmServerZc] sent zc response size=%u rid=%s",
                      builder.GetSize(), resp->rid().c_str());
        });

        if (_cb_connection) _cb_connection(conn);

        int epfd = epoll_create1(0);
        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = _channel.req_notify_fd();
        epoll_ctl(epfd, EPOLL_CTL_ADD, _channel.req_notify_fd(), &ev);

        LCZ_INFO("[ShmServerZc] started, req_fd=%d", _channel.req_notify_fd());
        _running = true;

        std::string body; MsgType type;
        const int req_fd = _channel.req_notify_fd();
        while (_running) {
            struct epoll_event events[1];
            int n = epoll_wait(epfd, events, 1, 500);
            if (n < 0) break;
            if (n > 0) { uint64_t val; ssize_t __attribute__((unused)) rd = ::read(req_fd, &val, sizeof(val)); }

            while (_channel.read_request(body, type)) {
                if (type != MsgType::REQ_RPC_FLAT) continue;

                // 读端零拷贝：ShmZcReader 直接映射，不解析
                ShmZcReader reader(body);
                auto* req = reader.as<lcz_rpc::fb::RpcRequest>();
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

                if (_cb_message) {
                    BaseMessage::ptr base_msg = json_req;
                    _cb_message(conn, base_msg);
                }
            }
        }
        close(epfd);
    }

    void stop() override { _running = false; }
    ~ShmServerZc() { _running = false; _channel.destroy(); }

private:
    ShmChannel        _channel;
    std::string       _shm_name, _notify_path;
    size_t            _req_size, _resp_size;
    std::atomic<bool> _running{false};
};

} // namespace lcz_rpc
