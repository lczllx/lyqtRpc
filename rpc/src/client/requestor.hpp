#pragma once

/*对客户端的每一个请求进行管理
发送请求时：创建ReqDescribe并存入映射表
等待响应时：根据请求类型使用不同机制等待结果
收到响应时：通过onResponse查找对应的请求描述，执行相应处理
清理资源：处理完成后删除请求描述，防止内存泄漏
*/
#include "general/net.hpp"
#include "general/message.hpp"
#include "general/publicconfig.hpp"
#include "general/log_system/lcz_log.h"
#include "general/dispacher.hpp"
#include<future>
#include<functional>
#include<chrono>
#include "general/publicconfig.hpp"
#include "muduo/net/EventLoop.h"
#include "muduo/net/TcpConnection.h"

namespace lcz_rpc
{
    namespace client
    {
        // 请求管理器类：管理 RPC 请求的发送、响应匹配、超时处理
        class Requestor
        {
            public:
            using ptr=std::shared_ptr<Requestor>;
            using ReqCallback=std::function<void(const BaseMessage::ptr&)>;
            using AsyncResponse=std::future<BaseMessage::ptr>;
            // 请求描述结构体：记录请求类型、回调、promise、超时定时器等
            struct ReqDescribe
            {
                using ptr=std::shared_ptr<ReqDescribe>;
                ReqType reqtype;
                ReqCallback callback;
                std::promise<BaseMessage::ptr> response;
                BaseMessage::ptr request;
                muduo::net::TimerId timer_id;  // 超时定时器 ID（用于取消定时器）
                bool timeout_triggered = false;  // 是否已触发超时
            };
            // 处理服务端响应：根据消息 id 匹配请求，触发对应的 promise 或回调
            void onResponse(const BaseConnection::ptr& conn,BaseMessage::ptr& msg)
            {
                std::string id=msg->rid();
                ReqDescribe::ptr req_desc=getDesc(id);
                if(req_desc.get()==nullptr)
                {
                    LCZ_ERROR("收到 %s 响应，但消息描述不存在",id.c_str());
                    return;
                }
                
                // 检查是否已超时
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    if(req_desc->timeout_triggered)
                    {
                        LCZ_WARN("收到 %s 响应，但请求已超时，忽略响应", id.c_str());
                        delDescUnlocked(id);
                        return;
                    }
                    
                    // 取消定时器（无效 TimerId 时 cancel 为 no-op）
                    auto* muduo_conn = dynamic_cast<MuduoConnection*>(conn.get());
                    if(muduo_conn && muduo_conn->getLoop())
                    {
                        muduo_conn->getLoop()->cancel(req_desc->timer_id);
                    }
                }
                
                if(req_desc->reqtype==ReqType::ASYNC)
                {
                    req_desc->response.set_value(msg);//设置结果
                }
                else if(req_desc->reqtype==ReqType::CALLBACK)
                {
                    if(req_desc->callback)req_desc->callback(msg);//回调处理
                }
                else{
                    LCZ_ERROR("未知请求类型");
                }
                delDesc(id);//处理完删除掉这个描述信息
            }
            
            // 超时处理：请求超时后取消定时器，根据类型设置超时结果或忽略回调
            void onTimeout(const std::string& req_id)
            {
                ReqDescribe::ptr req_desc = getDesc(req_id);
                if(req_desc.get() == nullptr)
                {
                    LCZ_DEBUG("超时回调：请求 %s 已不存在（可能已收到响应）", req_id.c_str());
                    return;
                }
                
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    if(req_desc->timeout_triggered)
                    {
                        return;  // 已经处理过超时
                    }
                    req_desc->timeout_triggered = true;
                }
                
                LCZ_ERROR("请求超时: id=%s", req_id.c_str());
                
                // 根据请求是 JSON 还是 Proto 构造对应的超时响应
                if(req_desc->reqtype == ReqType::ASYNC)
                {
                    // 创建超时响应：若请求为 Proto RPC 则返回 ProtoRpcResponse，否则 RpcResponse
                    BaseMessage::ptr timeout_msg;
                    if (dynamic_cast<ProtoRpcRequest*>(req_desc->request.get())) {
                        auto proto_resp = MessageFactory::create<ProtoRpcResponse>();
                        proto_resp->setId(req_id);
                        proto_resp->setRcode(RespCode::TIMEOUT);
                        timeout_msg = proto_resp;
                    } else {
                        auto json_resp = MessageFactory::create<RpcResponse>();
                        json_resp->setId(req_id);
                        json_resp->setRcode(RespCode::TIMEOUT);
                        timeout_msg = json_resp;
                    }
                    // 注意：这里不能直接 set_value，因为 promise 可能已经被设置
                    // 更好的方式是使用 shared_future 或者检查 promise 状态
                    // 简化处理：尝试设置，如果失败说明已经收到响应
                    try {
                        req_desc->response.set_value(timeout_msg);
                    } catch(...) {
                        // promise 已经被设置，说明响应已到达，忽略超时
                        LCZ_DEBUG("超时处理：请求 %s 已收到响应，忽略超时", req_id.c_str());
                    }
                }
                else if(req_desc->reqtype == ReqType::CALLBACK)
                {
                    // 回调模式：可以调用回调并传递超时错误
                    // 或者不调用，让调用方自己处理超时
                    LCZ_WARN("回调模式请求超时: id=%s，回调不会被调用", req_id.c_str());
                }
                
                delDesc(req_id);
            }
            // 异步发送：创建请求描述、设置超时定时器，通过 async_resp 返回 future 供调用方等待
            bool send(const BaseConnection::ptr& conn,const BaseMessage::ptr& req,AsyncResponse& async_resp,
                     std::chrono::milliseconds timeout = std::chrono::seconds(5))
            {
                ReqDescribe::ptr req_desc=newDesc(req,ReqType::ASYNC);
                if(req_desc.get()==nullptr)
                {
                    LCZ_ERROR("构造请求描述对象失败！");
                    return false;
                }

                // 设置超时定时器：muduo runAfter 到期后跨线程回调 onTimeout
                auto* muduo_conn = dynamic_cast<MuduoConnection*>(conn.get());
                if(muduo_conn && muduo_conn->getLoop())
                {
                    double timeout_sec = timeout.count() / 1000.0;
                    std::string req_id = req->rid();
                    muduo::net::TimerId tid = muduo_conn->getLoop()->runAfter(timeout_sec,
                        [this, req_id]() { this->onTimeout(req_id); });
                    {
                        std::unique_lock<std::mutex> lock(_mutex);
                        req_desc->timer_id = tid;
                    }
                    LCZ_DEBUG("设置超时定时器: id=%s, timeout=%.2fs", req_id.c_str(), timeout_sec);
                }
                else
                {
                    LCZ_WARN("无法获取 EventLoop，超时机制不可用: id=%s", req->rid().c_str());
                }

                conn->send(req);//异步请求发送
                async_resp= req_desc->response.get_future();//获取关联的future对象
                return true;
            }
            // 同步发送：内部调用异步 send，阻塞等待响应直到超时或收到结果
            bool send(const BaseConnection::ptr& conn,const BaseMessage::ptr& req,BaseMessage::ptr& resp,
                     std::chrono::milliseconds timeout = std::chrono::seconds(5))
            {
                LCZ_DEBUG("Requestor sync send id=%s, timeout=%ldms", req->rid().c_str(), timeout.count());
                AsyncResponse async_resp;
                if(send(conn,req,async_resp, timeout)==false)
                {
                    LCZ_ERROR("Requestor sync send failed id=%s", req->rid().c_str());
                    return false;
                }
                
                // 使用 wait_for 实现超时
                if(async_resp.wait_for(timeout) == std::future_status::timeout)
                {
                    LCZ_ERROR("Requestor sync recv timeout id=%s", req->rid().c_str());
                    // 先尝试取消 muduo 定时器，避免之后在 loop 里再触发一次 onTimeout
                    ReqDescribe::ptr desc = getDesc(req->rid());
                    if(desc)
                    {
                        auto* mc = dynamic_cast<MuduoConnection*>(conn.get());
                        if(mc && mc->getLoop())
                            mc->getLoop()->cancel(desc->timer_id);
                    }
                    onTimeout(req->rid());  // 触发超时处理
                    return false;
                }
                
                resp=async_resp.get();
                LCZ_DEBUG("Requestor sync recv id=%s", req->rid().c_str());
                return true;
            }
            // 回调方式发送：注册回调函数，响应到达时通过 onResponse 触发 cb
            bool send(const BaseConnection::ptr& conn,const BaseMessage::ptr& req,const ReqCallback& cb)
            {
                ReqDescribe::ptr req_desc=newDesc(req,ReqType::CALLBACK,cb);
                if(req_desc.get()==nullptr)
                {
                    LCZ_ERROR("构造请求描述对象失败！");
                    return false;
                }
                conn->send(req);
                return true;
            }
            private:
            // 创建并注册新的请求描述到映射表，供后续查找和响应匹配
            ReqDescribe::ptr newDesc(const BaseMessage::ptr& req,ReqType req_type,const ReqCallback& cb=ReqCallback())
            {
                std::unique_lock<std::mutex> lock(_mutex);
                ReqDescribe::ptr req_desc=std::make_shared<ReqDescribe>();
                req_desc->reqtype=req_type;
                req_desc->request=req;
                if(req_type==ReqType::CALLBACK&&cb)req_desc->callback=cb;
                _request_desc[req->rid()]=req_desc;
                LCZ_DEBUG("newDesc add id=%s", req->rid().c_str());
                return req_desc;
            }
            // 根据请求 id 从映射表查找对应的请求描述
            ReqDescribe::ptr getDesc(const std::string& rid)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_request_desc.find(rid);
                if(it!=_request_desc.end())
                {
                    return it->second;
                }
                return  ReqDescribe::ptr();               
            }
            // 删除请求描述，释放资源（加锁版本）
            void delDesc(const std::string& rid)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                delDescUnlocked(rid);
            }
            
            // 删除请求描述（内部使用，调用方需已持 _mutex）
            void delDescUnlocked(const std::string& rid)
            {
                _request_desc.erase(rid);
            }
            private:
            std::mutex _mutex;
            std::unordered_map<std::string,ReqDescribe::ptr> _request_desc;//rid desc
        };
    }
}