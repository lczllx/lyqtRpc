#pragma once
// =============================================================================
// shm_client_zc.hpp — 零拷贝 SHM Client，多客户端
// =============================================================================

#include "general/abstract.hpp"
#include "general/shm_channel.hpp"
#include "general/shm_connection.hpp"
#include "general/shm_zc_adaptor.hpp"
#include "general/message.hpp"
#include "general/log_system/lcz_log.h"
#include "rpc_message_generated.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <stop_token>

namespace lcz_rpc
{

    class ShmClientZc : public BaseClient
    {
    public:
        ShmClientZc(const std::string &notify_path = "lcz_shm_zc_notify")
            : _notify_path(notify_path)
        {
            if (const char *env = std::getenv("LCZ_SHM_WRITE_WAIT_MS"))
                _write_wait_ms = std::atoi(env);
        }

        void connect() override
        {
            int conn_fd = socket(AF_UNIX, SOCK_STREAM, 0);
            if (conn_fd < 0)
            {
                LCZ_ERROR("[ShmClientZc] socket failed errno=%d", errno);
                return;
            }
            struct sockaddr_un addr = {};
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, _notify_path.c_str(), sizeof(addr.sun_path) - 1);
            if (::connect(conn_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
            {
                LCZ_ERROR("[ShmClientZc] connect %s failed errno=%d", _notify_path.c_str(), errno);
                close(conn_fd);
                return;
            }

            int req_fd = -1, resp_fd = -1;
            if (!ShmChannel::handshake_client(conn_fd, req_fd, resp_fd, _shm_name))
            {
                LCZ_ERROR("[ShmClientZc] handshake failed");
                close(conn_fd);
                return;
            }
            close(conn_fd);

            if (!_channel.open(_shm_name))
            {
                LCZ_ERROR("[ShmClientZc] open %s failed", _shm_name.c_str());
                return;
            }
            _channel.set_req_notify_fd(req_fd);
            _channel.set_resp_notify_fd(resp_fd);

            auto conn = std::make_shared<ShmConnection>();
            conn->setName("shm_client_zc_" + _shm_name);
            conn->setSender([this](const BaseMessage::ptr &msg)
                            { return send(msg); });
            _conn = conn;
            if (_cb_connection)
                _cb_connection(conn);

            _worker = std::jthread([this](std::stop_token st)
                                   { responseLoop(st); });
            LCZ_INFO("[ShmClientZc] connected, shm=%s", _shm_name.c_str());
        }

        bool send(const BaseMessage::ptr &msg) override
        {
            auto req = std::dynamic_pointer_cast<RpcRequest>(msg);
            if (!req)
            {
                if (_conn)
                    _conn->setLastSendError(RpcError::SERVICE_ERROR);
                return false;
            }
            if (!_channel.is_open())
            {
                if (_conn)
                {
                    _conn->setLastSendError(RpcError::CONN_CLOSED);
                    _conn->markDead();
                }
                return false;
            }

            flatbuffers::FlatBufferBuilder builder(256);
            auto id = builder.CreateString(req->rid());
            auto method = builder.CreateString(req->method());
            auto trace_id = builder.CreateString(req->trace_id());
            auto span_id = builder.CreateString(req->span_id());
            std::string params_json;
            JSON::serialize(req->params(), params_json);
            auto params_vec = builder.CreateVector(
                reinterpret_cast<const uint8_t *>(params_json.data()), params_json.size());
            auto root = fb::CreateRpcRequest(builder, id, method, trace_id, span_id, params_vec);
            builder.Finish(root);

            std::string body(reinterpret_cast<const char *>(builder.GetBufferPointer()),
                             builder.GetSize());
            // 单帧超过 ring 容量：重试不会变好，直接按配置/载荷错误上报
            if (body.size() + 8 > _channel.req_capacity())
            {
                LCZ_ERROR("[ShmClientZc] message too large: %zu bytes > ring capacity %zu",
                          body.size() + 8, _channel.req_capacity());
                if (_conn)
                    _conn->setLastSendError(RpcError::SERVICE_ERROR);
                return false;
            }

            bool ok = _channel.write_request(body, MsgType::REQ_RPC_FLAT, _write_wait_ms);
            if (!ok)
            {
                if (_conn)
                {
                    if (!_channel.is_open())
                    {
                        _conn->setLastSendError(RpcError::CONN_CLOSED);
                        _conn->markDead();
                    }
                    else
                    {
                        // ring 满/等待超时：本地过载，可退避，不计熔断
                        _conn->setLastSendError(RpcError::BACKOFF);
                    }
                }
                return false;
            }
            _channel.notify_req();
            return true;
        }

        void shutdown() override
        {
            _worker.request_stop();
            if (_worker.joinable())
                _worker.join();
            if (_conn) _conn->markDead();
            _channel.destroy();
        }

        BaseConnection::ptr connection() override { return _conn; }
        bool connected() override { return _channel.is_open() && _conn && _conn->connected(); }
        ~ShmClientZc() { shutdown(); }

    private:
        void responseLoop(std::stop_token st)
        {
            int epfd = epoll_create1(0);
            struct epoll_event ev;
            ev.events = EPOLLIN;
            ev.data.fd = _channel.resp_notify_fd();
            epoll_ctl(epfd, EPOLL_CTL_ADD, _channel.resp_notify_fd(), &ev);

            const int resp_fd = _channel.resp_notify_fd();
            LCZ_INFO("[ShmClientZc] responseLoop started, resp_fd=%d", resp_fd);
            while (!st.stop_requested())
            {
                struct epoll_event events[1];
                int n = epoll_wait(epfd, events, 1, 500);
                if (n < 0)
                    break;

                // 每 500ms 超时醒一次：Server 崩溃或已 shutdown 时把连接标死，
                // 否则 connected() 永远为 true，发现/心跳路径会一直往死通道里写。
                if (n == 0 && !_channel.peerAlive())
                {
                    LCZ_ERROR("[ShmClientZc] server is gone (shm=%s), marking connection dead", _shm_name.c_str());
                    if (_conn) _conn->markDead();
                    break;
                }
                if (n > 0)
                {
                    uint64_t val;
                    (void)::read(resp_fd, &val, sizeof(val));
                }

                ShmChannel::FrameView view;
                while (_channel.read_response_view(view))
                {
                    BaseMessage::ptr base_msg;
                    if (view.type == MsgType::RSP_RPC_FLAT)
                    {
                        // FlatBuffers 直接映射 ring，不拷贝 body
                        ShmZcReader reader(view.body);
                        auto *fresp = reader.as<lcz_rpc::fb::RpcResponse>();
                        if (fresp)
                        {
                            auto json_resp = MessageFactory::create<RpcResponse>();
                            json_resp->setId(ShmZcReader::strval(fresp->id()));
                            json_resp->setRcode(static_cast<RespCode>(fresp->rcode()));
                            json_resp->setMsgType(MsgType::RSP_RPC);
                            if (fresp->result() && fresp->result()->size() > 0)
                            {
                                std::string rs(reinterpret_cast<const char *>(fresp->result()->data()),
                                               fresp->result()->size());
                                Json::Value rv;
                                if (JSON::deserialize(rs, rv))
                                    json_resp->setResult(rv);
                            }
                            base_msg = json_resp;
                        }
                    }
                    // 视图里的字段已拷进消息对象，派发前解 pin，不跨回调占用 ring
                    _channel.release_response_view();
                    if (base_msg && _cb_message)
                        _cb_message(_conn, base_msg);
                }
            }
            if (!st.stop_requested() && _conn)
                _conn->markDead(); // epoll 出错等非主动退出
            close(epfd);
        }

        ShmChannel _channel;
        ShmConnection::ptr _conn;
        std::string _notify_path;
        std::string _shm_name;
        std::jthread _worker;
        int _write_wait_ms = SHM_REQUEST_WAIT_MS; // ring 满时的有界等待预算（LCZ_SHM_WRITE_WAIT_MS 可覆盖）
    };

} // namespace lcz_rpc
