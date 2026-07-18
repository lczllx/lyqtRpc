#pragma once
#include "requestor.hpp"
#include "../general/message.hpp"
#include "../general/log_system/lcz_log.h"
#include "circuit_breaker.hpp"
#include <future>
#include <functional>

namespace lcz_rpc
{
    namespace client
    {
        // requestor里面的send是对basemessage进行处理
        // 这里的caller是对rpcresponse里面的result进行处理
        //  路径二：call_proto 纯 Proto API，热路径零 JSON
        //  RPC 调用封装类：封装 Requestor，提供同步/future/回调三种调用方式（JSON）+ call_proto（Proto）
        class RpcCaller
        {
        public:
            using ptr = std::shared_ptr<RpcCaller>;
            using RpcAsyncRespose = std::future<Json::Value>;
            using ResponseCallback = std::function<void(const Json::Value &)>;
            RpcCaller(const Requestor::ptr &reqtor, CircuitBreaker::ptr breaker = nullptr)
                : _requestor(reqtor), _breaker(std::move(breaker)) {}

            // 同步 RPC：阻塞等待响应，结果写入 result
            bool call(const BaseConnection::ptr &conn, const std::string &method_name, const Json::Value &params, Json::Value &result)
            {
                LCZ_DEBUG("RpcCaller sync call method=%s", method_name.c_str());
                std::string host = conn->peerAddress();
                if (_breaker && !_breaker->allowRequest(method_name, host))
                {
                    LCZ_WARN("熔断拒绝 method=%s host=%s", method_name.c_str(), host.c_str());
                    return false;
                }
                // 构造reqmsg
                RpcRequest::ptr req_msg = MessageFactory::create<RpcRequest>();
                req_msg->setId(uuid());
                req_msg->setMsgType(MsgType::REQ_RPC);
                req_msg->setMethod(method_name);
                req_msg->setParams(params);
                BaseMessage::ptr resp_msg;
                bool ret = _requestor->send(conn, std::dynamic_pointer_cast<BaseMessage>(req_msg), resp_msg);
                if (!ret)
                {
                    LCZ_ERROR("rpc同步请求失败");
                    if (_breaker)
                        _breaker->onFailure(method_name, host);
                    return false;
                }
                RpcResponse::ptr rpc_respmsg = std::dynamic_pointer_cast<RpcResponse>(resp_msg);
                if (rpc_respmsg.get() == nullptr)
                {
                    LCZ_ERROR("类型向下转换失败失败");
                    if (_breaker)
                        _breaker->onFailure(method_name, host);
                    return false;
                }
                if (rpc_respmsg->rcode() != RespCode::SUCCESS)
                {
                    LCZ_ERROR("rpc请求出错：%s", errReason(rpc_respmsg->rcode()).c_str());
                    if (_breaker)
                        _breaker->onFailure(method_name, host);
                    return false;
                }
                result = rpc_respmsg->result();
                LCZ_DEBUG("RpcCaller sync call finish method=%s", method_name.c_str());
                if (_breaker)
                    _breaker->onSuccess(method_name, host);
                return true;
            }
            // 异步 RPC：通过 result future 获取结果
            bool call(const BaseConnection::ptr &conn, const std::string &method_name, Json::Value &params, RpcAsyncRespose &result)
            {
                LCZ_DEBUG("RpcCaller future call method=%s", method_name.c_str());

                std::string host = conn->peerAddress();
                if (_breaker && !_breaker->allowRequest(method_name, host))
                {
                    LCZ_WARN("熔断拒绝 method=%s host=%s", method_name.c_str(), host.c_str());
                    return false;
                }

                // 向服务端发送异步回调请求，设置回调函数，在回调 函数中对pomise设置数据
                auto req_msg = MessageFactory::create<RpcRequest>();
                req_msg->setId(uuid());
                req_msg->setMsgType(MsgType::REQ_RPC);
                req_msg->setMethod(method_name);
                req_msg->setParams(params);
                BaseMessage::ptr resp_msg;

                auto json_pomise = std::make_shared<std::promise<Json::Value>>(); // 防止作用域结束销毁
                result = json_pomise->get_future();                               /// 创建 Promise-Future 对，通过 get_future() 连接

                Requestor::ReqCallback cb = std::bind(&RpcCaller::callBack, this, json_pomise, method_name,host,std::placeholders::_1);
                bool ret = _requestor->send(conn, std::dynamic_pointer_cast<BaseMessage>(req_msg), cb);
                if (!ret)
                {
                    LCZ_ERROR("rpc异步请求失败");
                    if (_breaker)
                        _breaker->onFailure(method_name, host);
                    return false;
                }

                return true;
            }
            // 回调式 RPC：响应到达时调用 cb
            bool call(const BaseConnection::ptr &conn, const std::string &method_name, Json::Value &params, const ResponseCallback &cb)
            {
                LCZ_DEBUG("RpcCaller callback call method=%s", method_name.c_str());

                 std::string host = conn->peerAddress();
                if (_breaker && !_breaker->allowRequest(method_name, host))
                {
                    LCZ_WARN("熔断拒绝 method=%s host=%s", method_name.c_str(), host.c_str());
                    return false;
                }

                auto req_msg = MessageFactory::create<RpcRequest>();
                req_msg->setId(uuid());
                req_msg->setMsgType(MsgType::REQ_RPC);
                req_msg->setMethod(method_name);
                req_msg->setParams(params);
                BaseMessage::ptr resp_msg;

                Requestor::ReqCallback reqcb = std::bind(&RpcCaller::callBackself, this, cb,method_name,host, std::placeholders::_1);
                bool ret = _requestor->send(conn, std::dynamic_pointer_cast<BaseMessage>(req_msg), reqcb);
                if (!ret)
                {
                    LCZ_ERROR("rpc回调请求失败");
                    if (_breaker)
                        _breaker->onFailure(method_name, host);
                    return false;
                }

                return true;
            }

        private:
            // 异步模式回调：校验 rcode 后设置 promise
            void callBack(std::shared_ptr<std::promise<Json::Value>> result,
                const std::string& method_name,const std::string& host, const BaseMessage::ptr &msg)
            {
                
                RpcResponse::ptr rpc_respmsg = std::dynamic_pointer_cast<RpcResponse>(msg);
                if (rpc_respmsg.get() == nullptr)
                {
                    LCZ_ERROR("类型向下转换失败失败");
                      if (_breaker)
                        _breaker->onFailure(method_name, host);
                    return;
                }
                if (rpc_respmsg->rcode() != RespCode::SUCCESS)
                {
                    LCZ_ERROR("rpc异步出错：%s", errReason(rpc_respmsg->rcode()).c_str());
                      if (_breaker)
                        _breaker->onFailure(method_name, host);
                    return;
                }
                result->set_value(rpc_respmsg->result()); // 被触发时设置结果
                if (_breaker)
                        _breaker->onSuccess(method_name, host);
            }
            // 回调模式：校验 rcode 后执行用户 cb
            void callBackself(const ResponseCallback &cb, const std::string& method_name,
                const std::string& host, const BaseMessage::ptr &msg)
            {
                RpcResponse::ptr rpc_respmsg = std::dynamic_pointer_cast<RpcResponse>(msg);
                if (rpc_respmsg.get() == nullptr)
                {
                    LCZ_ERROR("类型向下转换失败失败");
                    if (_breaker)
                        _breaker->onFailure(method_name, host);
                    return;
                }
                if (rpc_respmsg->rcode() != RespCode::SUCCESS)
                {
                    LCZ_ERROR("rpc回调出错：%s", errReason(rpc_respmsg->rcode()).c_str());
                    if (_breaker)
                        _breaker->onFailure(method_name, host);
                    return;
                }
                cb(rpc_respmsg->result()); // 使用回调处理结果
                if (_breaker)
                        _breaker->onSuccess(method_name, host);
            }

        public:
            // ---------- 路径二：纯 Proto API ----------
            // 同步 call_proto：Req/Resp 为 protobuf 类型，线缆为二进制，零 JSON
            // error_code 传出参数：成功时写空串；失败时写可读错误分类名(send_failed / backoff / remote_TIMEOUT ...)
            template <typename Req, typename Resp>
            bool call_proto(const BaseConnection::ptr &conn, const std::string &method_name,
                            const Req &req, Resp *resp,
                            std::chrono::milliseconds timeout = std::chrono::seconds(5),
                            std::string *error_code = nullptr) // 默认5s超时，平衡慢请求与快速失败
            {
                LCZ_DEBUG("RpcCaller call_proto sync method=%s", method_name.c_str());
                std::string host = conn->peerAddress();
                if (_breaker && !_breaker->allowRequest(method_name, host))
                {
                    LCZ_WARN("熔断拒绝 method=%s host=%s", method_name.c_str(), host.c_str());
                    if (error_code) *error_code = "circuit_open";
                    return false;
                }
                auto req_msg = MessageFactory::create<ProtoRpcRequest>();
                req_msg->setId(uuid());
                req_msg->setMsgType(MsgType::REQ_RPC_PROTO);
                req_msg->setMethod(method_name);
                req_msg->setTraceId(uuid()); // 分布式追踪：生成 trace_id
                req_msg->setSpanId("0");
                std::string body;
                if (!req.SerializeToString(&body))
                {
                    LCZ_ERROR("call_proto: Req::SerializeToString failed");
                    if (error_code) *error_code = "serialize_failed";
                    if (_breaker) _breaker->onFailure(method_name, host);
                    return false;
                }
                req_msg->setBody(body);

                BaseMessage::ptr resp_msg;
                if (!_requestor->send(conn, std::dynamic_pointer_cast<BaseMessage>(req_msg), resp_msg, timeout))
                {
                    LCZ_ERROR("call_proto sync send failed");
                    if (error_code) *error_code = "send_failed";
                    if (_breaker) _breaker->onFailure(method_name, host);
                    return false;
                }
                auto proto_resp = std::dynamic_pointer_cast<ProtoRpcResponse>(resp_msg);
                if (!proto_resp)
                {
                    LCZ_ERROR("call_proto: response type not ProtoRpcResponse");
                    if (error_code) *error_code = "parse_failed";
                    if (_breaker) _breaker->onFailure(method_name, host);
                    return false;
                }
                if (proto_resp->rcode() == RespCode::BACKOFF)
                {
                    int64_t wait_ms = proto_resp->retryAfterMs();
                    if (wait_ms <= 0) wait_ms = 10; // 兜底 10ms
                    // BACKOFF 不是最终失败，下面还有重试；只有重试也失败才设 error_code = "backoff"
                    LCZ_WARN("call_proto BACKOFF method=%s, 等待 %ldms 后重试一次", method_name.c_str(), wait_ms);
                    std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));

                    // 重试一次：重新构造请求再发，复用 trace_id 保持调用链完整
                    auto retry_req = MessageFactory::create<ProtoRpcRequest>();
                    retry_req->setId(uuid());
                    retry_req->setMsgType(MsgType::REQ_RPC_PROTO);
                    retry_req->setMethod(method_name);
                    retry_req->setBody(body); // 复用已序列化的 body
                    retry_req->setTraceId(req_msg->trace_id()); // 复用原始 trace_id
                    retry_req->setSpanId("1"); // 重试标记
                    BaseMessage::ptr retry_resp;
                    if (!_requestor->send(conn, std::dynamic_pointer_cast<BaseMessage>(retry_req), retry_resp, timeout))
                    {
                        LCZ_ERROR("call_proto BACKOFF 重试 send 失败");
                        if (error_code) *error_code = "send_failed";
                        if (_breaker) _breaker->onFailure(method_name, host);
                        return false;
                    }
                    proto_resp = std::dynamic_pointer_cast<ProtoRpcResponse>(retry_resp);
                    if (!proto_resp)
                    {
                        LCZ_ERROR("call_proto BACKOFF 重试: response type not ProtoRpcResponse");
                        if (error_code) *error_code = "parse_failed";
                        if (_breaker) _breaker->onFailure(method_name, host);
                        return false;
                    }
                    if (proto_resp->rcode() == RespCode::BACKOFF)
                    {
                        LCZ_ERROR("call_proto BACKOFF 重试再次被拒绝 method=%s", method_name.c_str());
                        if (error_code) *error_code = "backoff";
                        if (_breaker) _breaker->onFailure(method_name, host);
                        return false;
                    }
                    // 重试后继续走 SUCCESS 校验路径
                }
                if (proto_resp->rcode() != RespCode::SUCCESS)
                {
                    LCZ_ERROR("call_proto error: %s", errReason(proto_resp->rcode()).c_str());
                    if (error_code) *error_code = std::string("remote_") + errReason(proto_resp->rcode());
                    if (_breaker) _breaker->onFailure(method_name, host);
                    return false;
                }
                if (!resp->ParseFromString(proto_resp->body()))
                {
                    LCZ_ERROR("call_proto: Resp::ParseFromString failed");
                    if (error_code) *error_code = "parse_failed";
                    if (_breaker) _breaker->onFailure(method_name, host);
                    return false;
                }
                LCZ_DEBUG("RpcCaller call_proto sync finish method=%s", method_name.c_str());
                if (_breaker) _breaker->onSuccess(method_name, host);
                return true;
            }

            // 异步 call_proto：通过 future<Resp> 获取结果（Resp 需默认构造）
            template <typename Req, typename Resp>
            bool call_proto(const BaseConnection::ptr &conn, const std::string &method_name,
                            const Req &req, std::future<Resp> *out_future,
                            std::chrono::milliseconds timeout = std::chrono::seconds(5)) // 默认5s超时，平衡慢请求与快速失败
            {
                LCZ_DEBUG("RpcCaller call_proto async method=%s", method_name.c_str());
                std::string host = conn->peerAddress();
                if (_breaker && !_breaker->allowRequest(method_name, host))
                {
                    LCZ_WARN("熔断拒绝 method=%s host=%s", method_name.c_str(), host.c_str());
                    return false;
                }
                auto req_msg = MessageFactory::create<ProtoRpcRequest>();
                req_msg->setId(uuid());
                req_msg->setMsgType(MsgType::REQ_RPC_PROTO);
                req_msg->setMethod(method_name);
                std::string body;
                if (!req.SerializeToString(&body))
                {
                    LCZ_ERROR("call_proto async: Req::SerializeToString failed");
                    if (_breaker) _breaker->onFailure(method_name, host);
                    return false;
                }
                req_msg->setBody(body);

                auto prom = std::make_shared<std::promise<Resp>>();
                *out_future = prom->get_future();
                Requestor::ReqCallback cb = [this, prom, method_name, host](const BaseMessage::ptr &msg)
                {
                    auto pr = std::dynamic_pointer_cast<ProtoRpcResponse>(msg);
                    if (!pr)
                    {
                        LCZ_ERROR("call_proto async: response type not ProtoRpcResponse");
                        if (_breaker) _breaker->onFailure(method_name, host);
                        prom->set_exception(std::make_exception_ptr(std::runtime_error("invalid response type")));
                        return;
                    }
                    if (pr->rcode() != RespCode::SUCCESS)
                    {
                        if (_breaker) _breaker->onFailure(method_name, host);
                        prom->set_exception(std::make_exception_ptr(std::runtime_error(errReason(pr->rcode()))));
                        return;
                    }
                    Resp r;
                    if (!r.ParseFromString(pr->body()))
                    {
                        if (_breaker) _breaker->onFailure(method_name, host);
                        prom->set_exception(std::make_exception_ptr(std::runtime_error("ParseFromString failed")));
                        return;
                    }
                    if (_breaker) _breaker->onSuccess(method_name, host);
                    prom->set_value(std::move(r));
                };
                if (!_requestor->send(conn, std::dynamic_pointer_cast<BaseMessage>(req_msg), cb))
                {
                    LCZ_ERROR("call_proto async send failed");
                    if (_breaker) _breaker->onFailure(method_name, host);
                    return false;
                }
                return true;
            }

            // 回调式 call_proto
            template <typename Req, typename Resp>
            bool call_proto(const BaseConnection::ptr &conn, const std::string &method_name,
                            const Req &req, std::function<void(const Resp &)> on_success,
                            std::function<void(RespCode)> on_error = nullptr)
            {
                LCZ_DEBUG("RpcCaller call_proto callback method=%s", method_name.c_str());
                std::string host = conn->peerAddress();
                if (_breaker && !_breaker->allowRequest(method_name, host))
                {
                    LCZ_WARN("熔断拒绝 method=%s host=%s", method_name.c_str(), host.c_str());
                    return false;
                }
                auto req_msg = MessageFactory::create<ProtoRpcRequest>();
                req_msg->setId(uuid());
                req_msg->setMsgType(MsgType::REQ_RPC_PROTO);
                req_msg->setMethod(method_name);
                std::string body;
                if (!req.SerializeToString(&body))
                {
                    LCZ_ERROR("call_proto callback: Req::SerializeToString failed");
                    if (on_error) on_error(RespCode::INVALID_MSG);
                    if (_breaker) _breaker->onFailure(method_name, host);
                    return false;
                }
                req_msg->setBody(body);

                Requestor::ReqCallback cb = [this, on_success, on_error, method_name, host](const BaseMessage::ptr &msg)
                {
                    auto pr = std::dynamic_pointer_cast<ProtoRpcResponse>(msg);
                    if (!pr)
                    {
                        LCZ_ERROR("call_proto callback: response type not ProtoRpcResponse");
                        if (on_error) on_error(RespCode::INVALID_MSG);
                        if (_breaker) _breaker->onFailure(method_name, host);
                        return;
                    }
                    if (pr->rcode() != RespCode::SUCCESS)
                    {
                        if (on_error) on_error(pr->rcode());
                        if (_breaker) _breaker->onFailure(method_name, host);
                        return;
                    }
                    Resp r;
                    if (!r.ParseFromString(pr->body()))
                    {
                        if (on_error) on_error(RespCode::PARSE_FAILED);
                        if (_breaker) _breaker->onFailure(method_name, host);
                        return;
                    }
                    if (_breaker) _breaker->onSuccess(method_name, host);
                    if (on_success) on_success(r);
                };
                if (!_requestor->send(conn, std::dynamic_pointer_cast<BaseMessage>(req_msg), cb))
                {
                    LCZ_ERROR("call_proto callback send failed");
                    if (_breaker) _breaker->onFailure(method_name, host);
                    return false;
                }
                return true;
            }

        private:
            Requestor::ptr _requestor;
            CircuitBreaker::ptr _breaker;
        };
    }
}