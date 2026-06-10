#pragma once
#include "../general/net.hpp"
#include "../general/message.hpp"
#include "../general/dispacher.hpp"
#include "../general/publicconfig.hpp"
#include "../general/log_system/lcz_log.h"
#include "../general/rate_limiter.hpp"
#include <google/protobuf/message_lite.h>

/*服务端对rpc请求的处理
1. 接收RPC请求 → 2. 根据method名查找服务 → 3. 参数校验
   ↓
4. 调用服务方法 → 5. 处理业务逻辑 → 6. 返回响应结果
*/
namespace lcz_rpc{
    namespace server{
        // RPC 参数/返回值类型枚举，与 Json::Value 的类型检查方法一一对应
        enum class ValType {
            BOOL = 0,       // Json::isBool()
            INTEGRAL,       // Json::isIntegral()
            NUMERIC,        // Json::isNumeric()
            STRING,         // Json::isString()
            ARRAY,          // Json::isArray()
            OBJECT,         // Json::isObject()
            NULL_TYPE       // Json::isNull()
        };
        // 服务描述类：描述单个 RPC 方法，负责参数校验、执行回调、返回值校验
        class ServiceDescribe
        {
            public:
            using ptr=std::shared_ptr<ServiceDescribe>;
            using ParamsDescribe=std::pair<std::string,ValType>;
            using ServiceCallback=std::function<void(const Json::Value& ,Json::Value& )>;
            ServiceDescribe(std::string&& method_name,ServiceCallback&& cb, std::vector<ParamsDescribe>&& params_desc,ValType return_type)
            :_method_name(std::move(method_name)),_service_cb(std::move(cb)),_params_desc(std::move(params_desc)),_return_type(return_type){}
            
            // 校验 params 中各字段类型是否符合 _params_desc
            bool checkParams(const Json::Value& params)
            {
                for(auto& desc:_params_desc)
                {
                    if(params.isMember(desc.first)==false)
                    {
                        LCZ_ERROR("字段 %s 校验失败",desc.first.c_str());
                        return false;
                    }
                    if(check(desc.second,params[desc.first])==false)
                    {
                        LCZ_ERROR("类型 %s 校验失败",desc.first.c_str());
                        return false;
                    }
                }
                return true;
            }
            // 调用服务回调并校验返回值类型
            bool call(const Json::Value& param,Json::Value& result)
            {
                _service_cb(param,result);
                if(check_return_ty(result)==false)
                {
                    LCZ_ERROR("回调 函数中的处理结果校验失败");
                    return false;
                }
                return true;
            }
            const std::string& getMethodname()const {return _method_name;}
            private:
            bool check_return_ty(const Json::Value& val)
            {
                return check(_return_type,val);
            }
            bool check(ValType valtype,const Json::Value& val)
            {
                switch(valtype)
                {
                    case ValType::BOOL: return val.isBool();
                    case ValType::INTEGRAL: return val.isIntegral();
                    case ValType::NUMERIC: return val.isNumeric();
                    case ValType::STRING: return val.isString();
                    case ValType::ARRAY: return val.isArray();
                    case ValType::OBJECT: return val.isObject();
                    case ValType::NULL_TYPE: return val.isNull();
                }
                return false;
            }
            private:    
            std::string _method_name; 
            ServiceCallback _service_cb;
            std::vector<ParamsDescribe> _params_desc;
            ValType _return_type;
        };
        // 服务工厂类：建造者模式，用于构造 ServiceDescribe
        class ServiceFactory
        {
            public:
            void setReturntype(ValType rtype){_return_type=rtype;}
            void setMethodName(const std::string& method_name){_method_name=method_name;}
            // 添加参数描述 (参数名, 类型)
            void setParamdescribe(const std::string& param_name,ValType vtype){_params_desc.emplace_back(param_name,vtype);}
            void setServiceCallback(const ServiceDescribe::ServiceCallback& cb){_service_cb=cb;}
            
            // 根据已设置的参数构造 ServiceDescribe
            ServiceDescribe::ptr build()
            {
                std::string method_name = _method_name;
                ServiceDescribe::ServiceCallback service_cb = _service_cb;
                std::vector<ServiceDescribe::ParamsDescribe> params_desc = _params_desc;
                
                return std::make_shared<ServiceDescribe>(
                    std::move(method_name),
                    std::move(service_cb), 
                    std::move(params_desc),
                    _return_type
                );
            }
            private:
            std::string _method_name; 
            ServiceDescribe::ServiceCallback _service_cb;
            std::vector<ServiceDescribe::ParamsDescribe> _params_desc;
            ValType _return_type;

        };
        // 服务管理类：线程安全的 method->ServiceDescribe 注册表
        class ServiceManager
        {
            public:
            using ptr=std::shared_ptr<ServiceManager>;
            // 注册服务描述
            void add(const ServiceDescribe::ptr& service){std::unique_lock<std::mutex> lock(_mutex);_services[service->getMethodname()]=service;}
            
            // 根据方法名查找服务
            ServiceDescribe::ptr select(const std::string& methodname)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_services.find(methodname);
                if(it!=_services.end())
                {
                    return it->second;
                }
                return nullptr;
            }
            // 移除已注册的服务
            bool remove(const std::string& methodname)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_services.find(methodname);
                if(it!=_services.end())
                {
                     _services.erase(it);
                     return true;
                }
                return false;
            }

            private:
            std::mutex _mutex;
            std::unordered_map<std::string,ServiceDescribe::ptr> _services;
        };
        // RPC 路由器类：接收 RPC 请求，派发到对应的 ServiceDescribe 处理
        class RpcRouter
        {
            public:
            using ptr=std::shared_ptr<RpcRouter>;
            RpcRouter() : _manager(std::make_shared<ServiceManager>()) {}

            // 设置令牌桶限流器，不设置则不限流
            void setRateLimiter(TokenBucket::ptr limiter) { _rate_limiter = std::move(limiter); }

            // 处理 RPC 请求：查找服务、校验参数、调用回调、返回响应
            void onrpcRequst(const BaseConnection::ptr& conn,RpcRequest::ptr& req)
            {
                // 令牌桶流控：桶空则拒绝并告知 client 多久后重试
                if (_rate_limiter && !_rate_limiter->allow())
                {
                    LCZ_WARN("[RateLimiter-json] 桶空，拒绝 method=%s", req->method().c_str());
                    auto resp = MessageFactory::create<RpcResponse>();
                    resp->setId(req->rid());
                    resp->setMsgType(lcz_rpc::MsgType::RSP_RPC);
                    resp->setRcode(RespCode::BACKOFF);
                    resp->setRetryAfterMs(_rate_limiter->retryAfterMs());
                    conn->send(resp);
                    return;
                }
                LCZ_DEBUG("RpcRouter recv method=%s", req->method().c_str());
                auto service=_manager->select(req->method());
                if(service.get()==nullptr)
                {
                    LCZ_ERROR("服务不存在,method:%s",req->method().c_str());
                    return response(conn,req,Json::Value(),RespCode::SERVICE_NOT_FOUND);
                }
                if(service->checkParams(req->params())==false)
                {
                     LCZ_ERROR("参数校验失败,method:%s",req->method().c_str());
                    return response(conn,req,Json::Value(),RespCode::INVALID_PARAMS);
                }
                Json::Value result;
                bool ret=service->call(req->params(),result);
                if(ret==false)
                {
                    LCZ_ERROR("这里应该是服务调用失败,method:%s",req->method().c_str());
                    return response(conn,req,Json::Value(),RespCode::INTERNAL_ERROR);
                }
                LCZ_DEBUG("RpcRouter respond method=%s", req->method().c_str());
                return response(conn,req,result,RespCode::SUCCESS);
            }
            // 向路由器注册 RPC 方法
            void registerMethod(const ServiceDescribe::ptr& service){_manager->add(service);}
            // 构造 RpcResponse 并发送
            void response(const BaseConnection::ptr& conn,const RpcRequest::ptr& req,const Json::Value& result,RespCode rcode)
            {
                auto resp=MessageFactory::create<RpcResponse>();
                resp->setId(req->rid());
                resp->setMsgType(lcz_rpc::MsgType::RSP_RPC);
                resp->setRcode(rcode);
                resp->setResult(result);
                conn->send(resp);
            }
            private:
            ServiceManager::ptr _manager;
            TokenBucket::ptr _rate_limiter;
        };

        // 路径二：纯 Proto RPC 路由器，按 method 派发到类型化 handler(conn, Req, Resp*)
        class ProtoRpcRouter
        {
        public:
            using ptr = std::shared_ptr<ProtoRpcRouter>;

            // 设置令牌桶限流器，不设置则不限流
            void setRateLimiter(TokenBucket::ptr limiter) { _rate_limiter = std::move(limiter); }

            // 收到 Proto RPC 请求时调用：根据 method 查找并执行已注册的 proto handler
            void onProtoRequest(const BaseConnection::ptr& conn, ProtoRpcRequest::ptr& req)
            {
                // 令牌桶流控：桶空则拒绝
                if (_rate_limiter && !_rate_limiter->allow())
                {
                    const std::string& method = req->method();
                    LCZ_WARN("[RateLimiter-proto] 桶空，拒绝 method=%s", method.c_str());
                    sendProtoResponse(conn, req->rid(), RespCode::BACKOFF, "",
                                      _rate_limiter->retryAfterMs());
                    return;
                }
                const std::string& method = req->method();
                const std::string& body = req->body();
                const std::string& req_id = req->rid();
                LCZ_DEBUG("ProtoRpcRouter recv method=%s", method.c_str());
                auto it = _handlers.find(method);
                if (it == _handlers.end()) {
                    LCZ_ERROR("Proto method not found: %s", method.c_str());
                    sendProtoResponse(conn, req_id, RespCode::SERVICE_NOT_FOUND, "");
                    return;
                }
                it->second(conn, body, req_id);
            }
            // 注册纯 Proto 方法：handler(conn, const Req&, Resp*)，热路径零 JSON
            // 将类型化 handler 包装为统一签名的 lambda：body 解包 → Req → handler → Resp → 序列化 → 发送
            template<typename Req, typename Resp>
            void registerProtoHandler(const std::string& method,
                std::function<void(const BaseConnection::ptr&, const Req&, Resp*)> handler)
            {
                std::string method_copy = method;  // 按值捕获，避免 lambda 持有悬空引用
                _handlers[method] = [handler, method_copy](const BaseConnection::ptr& conn,
                    const std::string& body, const std::string& req_id) {
                    Req req;
                    if (!req.ParseFromString(body)) {
                        LCZ_ERROR("ProtoRpcRouter: ParseFromString failed method=%s", method_copy.c_str());
                        sendProtoResponse(conn, req_id, RespCode::PARSE_FAILED, "");
                        return;
                    }
                    Resp resp;
                    try {
                        handler(conn, req, &resp);
                    } catch (const std::exception& e) {
                        LCZ_ERROR("ProtoRpcRouter handler exception method=%s: %s", method_copy.c_str(), e.what());
                        sendProtoResponse(conn, req_id, RespCode::INTERNAL_ERROR, "");
                        return;
                    } catch (...) {
                        LCZ_ERROR("ProtoRpcRouter handler unknown exception method=%s", method_copy.c_str());
                        sendProtoResponse(conn, req_id, RespCode::INTERNAL_ERROR, "");
                        return;
                    }
                    std::string resp_body;
                    if (!resp.SerializeToString(&resp_body)) {
                        LCZ_ERROR("ProtoRpcRouter: SerializeToString failed");
                        sendProtoResponse(conn, req_id, RespCode::INTERNAL_ERROR, "");
                        return;
                    }
                    sendProtoResponse(conn, req_id, RespCode::SUCCESS, resp_body);
                };
            }
            static void sendProtoResponse(const BaseConnection::ptr& conn, const std::string& req_id,
                RespCode rcode, const std::string& body, int64_t retry_after_ms = 0)
            {
                auto resp = MessageFactory::create<ProtoRpcResponse>();
                resp->setId(req_id);
                resp->setMsgType(MsgType::RSP_RPC_PROTO);
                resp->setRcode(rcode);
                resp->setBody(body);
                if (retry_after_ms > 0) resp->setRetryAfterMs(retry_after_ms);
                conn->send(resp);
            }
        private:
            using HandlerFn = std::function<void(const BaseConnection::ptr&, const std::string& body, const std::string& req_id)>;
            std::unordered_map<std::string, HandlerFn> _handlers;
            TokenBucket::ptr _rate_limiter;
        };
    }
}