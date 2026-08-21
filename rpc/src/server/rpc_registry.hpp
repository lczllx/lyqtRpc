#pragma once
#include "general/net.hpp"
#include "general/message.hpp"
#include "general/dispacher.hpp"
#include "general/publicconfig.hpp"
#include "general/log_system/lcz_log.h"
#include "registry_store.hpp"
#include <iostream>
#include <set>
#include <string_view>

namespace lcz_rpc
{
    namespace server
    {
        // 提供者管理类：管理 method->Provider 映射，支持注册/删除/查询/负载更新/超时扫描
        class ProviderManager
        {
            public:
            using ptr=std::shared_ptr<ProviderManager>;
            // 提供者结构体：表示一个服务提供者，含连接、地址、负载、心跳时间等
            struct Provider
            {
                using ptr=std::shared_ptr<Provider>;
                std::mutex mutex;
                int load;
                std::vector<std::string> methods;
                BaseConnection::ptr conn;
                HostInfo address;
                std::chrono::steady_clock::time_point lastheartbeat;//最后心跳时间
                Provider(const BaseConnection::ptr& connection,const HostInfo& host)
                    :conn(connection),address(host),load(0),
                     lastheartbeat(std::chrono::steady_clock::now()){}
                // 记录该提供者提供的服务方法
                void appendmethod(std::string_view method)
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    methods.emplace_back(std::string(method));
                }
            };
            // 添加或更新服务提供者
            void addProvider(const BaseConnection::ptr& conn,const HostInfo& host,std::string_view method,int load)
            {
                Provider::ptr provider;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it=_connwithp.find(conn);
                    if(it==_connwithp.end())
                    {
                        provider =std::make_shared<Provider>(conn,host);  
                        _connwithp[conn]=provider;           
                    }else{
                        provider=it->second;
                    }
                    _methodwithproviders[std::string(method)].insert(provider);
                    provider->load=load;
                    provider->lastheartbeat=std::chrono::steady_clock::now();
                }
                    provider->appendmethod(method);
            }
            // 根据连接查找对应的 Provider
            Provider::ptr getProvider(const BaseConnection::ptr& conn)
            {                
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_connwithp.find(conn);
                if(it==_connwithp.end())
                {
                    return Provider::ptr();
                }else{
                    return it->second;
                }               
            }
            void delProvider(const BaseConnection::ptr& conn)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_connwithp.find(conn);
                if(it==_connwithp.end()){return ;}
                for(auto& method:it->second->methods)
                {
                    _methodwithproviders[method].erase(it->second);
                }
                _connwithp.erase(it);               
            }
            // 获取提供指定方法的主机列表
            std::vector<HostInfo> methodHost(std::string_view method)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                std::vector<HostInfo>ret;
                auto it=_methodwithproviders.find(std::string(method));
                if(it==_methodwithproviders.end())return ret;
                for(auto& provider:it->second)
                {
                    ret.emplace_back(provider->address);
                }
                return ret;
            }
            // 获取提供指定方法的主机详情（含负载）
            std::vector<HostDetail> methodHostDetails(std::string_view method) {
                std::unique_lock<std::mutex> lock(_mutex);
                std::vector<HostDetail> ret;
                auto it = _methodwithproviders.find(std::string(method));
                if (it == _methodwithproviders.end()) return ret;
                for (auto &provider : it->second) {
                    HostDetail detail;
                    detail.host.first = provider->address.first;
                    detail.host.second = provider->address.second;
                    detail.load = provider->load;
                    ret.emplace_back(detail);
                }
                return ret;
            }
            // 更新指定 method+host 的 provider 负载
            bool updateProviderLoad(std::string_view method,
                const HostInfo &host,
                int load)
            {
                    std::unique_lock<std::mutex>lock(_mutex);
                    auto it=_methodwithproviders.find(std::string(method));
                    if(it==_methodwithproviders.end()){return false;}
                    for(auto&provider:it->second)
                    {
                        if(provider->address==host){
                            provider->load=load;
                            provider->lastheartbeat=std::chrono::steady_clock::now();
                            return true;
                        }
                    }
                    return false;
            }
            // 更新 provider 最后心跳时间
            bool updateProviderLastHeartbeat(std::string_view method, const HostInfo& host) {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _methodwithproviders.find(std::string(method));
                if (it == _methodwithproviders.end()) return false;
                for (auto &p : it->second) {
                    if (p->address == host) {
                        p->lastheartbeat = std::chrono::steady_clock::now();
                        LCZ_INFO("%s:%d 心跳检测 更新最后心跳时间", p->address.first.c_str(),p->address.second);
                        return true; 
                    }
                }
                return false;
            }
            // 扫描并移除超时未心跳的 provider，返回需通知下线的 (method, host)
            std::vector<std::pair<std::string, HostInfo>> sweepExpired(std::chrono::seconds idle_timeout)
            {
                std::vector<std::pair<std::string, HostInfo>> expired;
                auto now = std::chrono::steady_clock::now();
                std::unique_lock<std::mutex> lock(_mutex);

                for (auto &kv : _methodwithproviders)
                {
                    const std::string &method = kv.first;
                    auto &providers = kv.second;
                    std::vector<Provider::ptr> to_remove_providers;
                    for (auto &p : providers)
                    {
                        auto idle_sec = std::chrono::duration_cast<std::chrono::seconds>(now - p->lastheartbeat).count();
                        if (now - p->lastheartbeat > idle_timeout)
                        {
                            LCZ_INFO("[Provider扫描] 发现过期提供者 method=%s %s:%d 闲置时间=%ld秒", 
                                 method.c_str(), p->address.first.c_str(), p->address.second, idle_sec);
                            expired.emplace_back(method, p->address);
                            to_remove_providers.push_back(p);
                        }
                    }
                    for (auto &p : to_remove_providers) providers.erase(p);
                }
                return expired;
            }

            private:
            std::mutex _mutex;
            std::unordered_map<std::string,std::set<Provider::ptr>> _methodwithproviders;
            std::unordered_map<BaseConnection::ptr,Provider::ptr> _connwithp;
        };
        // 发现者管理类：管理 method->Discoverer 映射，支持上线/下线通知
        class DiscoverManager
        {
            public:
            using ptr=std::shared_ptr<DiscoverManager>;
            // 发现者结构体：表示一个服务发现者，记录其关注的 method
            struct Discoverer
            {
                using ptr=std::shared_ptr<Discoverer>;
                std::mutex mutex;
                std::vector<std::string> methods;//发现过的服务名称
                BaseConnection::ptr conn;
                Discoverer(const BaseConnection::ptr& connection):conn(connection){}
                // 记录该发现者关注的服务方法
                void appendmethod(std::string_view method)
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    methods.emplace_back(std::string(method));
                }
            };
            // 添加发现者并建立 method->discoverer 映射
            Discoverer::ptr addDiscoverer(const BaseConnection::ptr& conn,const HostInfo& host,std::string_view method)
            {
                Discoverer::ptr discoverer;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it=_connwithd.find(conn);
                    if(it==_connwithd.end())
                    {
                        discoverer =std::make_shared<Discoverer>(conn);  
                        _connwithd[conn]=discoverer;           
                    }else{
                        discoverer=it->second;
                    }
                    _methodwithdiscoverer[std::string(method)].insert(discoverer);
                }
                    discoverer->appendmethod(method);
                    return discoverer;
            }
            // 根据连接查找 Discoverer
            Discoverer::ptr getProvider(const BaseConnection::ptr& conn)
            {                
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_connwithd.find(conn);
                if(it==_connwithd.end())
                {
                    return Discoverer::ptr();
                }else{
                    return it->second;
                }               
            }
            // 删除发现者并更新 method->discoverer 映射
            void delProvider(const BaseConnection::ptr& conn)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it=_connwithd.find(conn);
                if(it==_connwithd.end()){return ;}
                for(auto& method:it->second->methods)
                {
                    _methodwithdiscoverer[method].erase(it->second);
                }
                _connwithd.erase(it);               
            }
            // 向关注该 method 的发现者广播上线通知
            void onlineNotify(std::string_view method,const HostInfo& host)
            {
                return notify(method,host,ServiceOpType::ONLINE);
            }
            // 向关注该 method 的发现者广播下线通知
            void offlineNotify(std::string_view method,const HostInfo& host)
            {
                return notify(method,host,ServiceOpType::OFFLINE);
            }
            private:
            // 向关注 method 的发现者广播上线/下线通知
            void notify(std::string_view method,const HostInfo& host,ServiceOpType service_type)
            {
                std::unique_lock<std::mutex>lock(_mutex);
                auto it=_methodwithdiscoverer.find(std::string(method));
                if(it==_methodwithdiscoverer.end()){return ;}
                auto rpc_msg=MessageFactory::create<ServiceRequest>();
                rpc_msg->setHost(host);
                rpc_msg->setId(uuid());
                rpc_msg->setMethod(method);
                rpc_msg->setMsgType(MsgType::REQ_SERVICE);
                rpc_msg->setOptype(service_type);
                
                for(auto& provider:it->second)
                {
                    provider->conn->send(rpc_msg);//通知派发
                }

            }
            std::mutex _mutex;
            std::unordered_map<std::string,std::set<Discoverer::ptr>> _methodwithdiscoverer;
            std::unordered_map<BaseConnection::ptr,Discoverer::ptr> _connwithd;
        };

        // Provider 与 Discoverer 联合管理类：处理服务注册/发现/负载上报/心跳，协调两者
        class PwithDManager
        {
            public:
            using ptr=std::shared_ptr<PwithDManager>;
            PwithDManager(std::shared_ptr<IRegistryStore> store):_rstore(std::move(store)),_discoverer(std::make_shared<DiscoverManager>()){}
            // 处理服务请求：注册/发现/负载上报/心跳
            void onserviceRequest(const BaseConnection::ptr& conn,const ServiceRequest::ptr& msg)
            {
                ServiceOpType optype=msg->optype();
                if(optype==ServiceOpType::REGISTER)
                {//服务注册通知
                    LCZ_INFO("[trace_id=%s][Registry] 收到注册 method=%s host=%s:%d load=%d",
                             msg->trace_id().c_str(), msg->method().c_str(),
                             msg->host().first.c_str(), msg->host().second, msg->load());
                    _rstore->registerInstance(conn,msg->host(),msg->method(),msg->load());//注册服务
                    _discoverer->onlineNotify(msg->method(),msg->host());
                    //后续在这里处理负载均衡
                    return registryResponse(conn,msg);
                }
                else if(optype==ServiceOpType::DISCOVER)
                {//服务发现通知
                    LCZ_INFO("客⼾端要进⾏ %s 服务发现！", msg->method().c_str());
                    _discoverer->addDiscoverer(conn,msg->host(),msg->method());
                    return discoverResponse(conn,msg);
                }
                else if(optype==ServiceOpType::LOAD_REPORT)
                {//服务负载上报
                    LCZ_INFO("%s:%d 上报负载 %d", msg->host().first.c_str(),msg->host().second, msg->load());
                    //更新负载
                    bool update_success=_rstore->reportLoad(msg->method(),msg->host(),msg->load());
                   
                    return updateloadResponse(conn,msg,update_success);
                }
                else if(optype==ServiceOpType::HEARTBEAT_PROVIDER)//提供者向注册中心周期报活，服务端刷新 Provider.lasttime
                {//心跳检测
                    LCZ_INFO("[RegistryServer-Provider心跳接收] method=%s %s:%d", 
                         msg->method().c_str(), msg->host().first.c_str(), msg->host().second);
                    bool update_success=_rstore->heartbeat(msg->method(),msg->host());
                    if (update_success) {
                        LCZ_INFO("[RegistryServer-Provider心跳处理] method=%s %s:%d 更新成功", 
                             msg->method().c_str(), msg->host().first.c_str(), msg->host().second);
                    } else {
                        LCZ_WARN("[RegistryServer-Provider心跳处理] method=%s %s:%d 更新失败，提供者不存在", 
                             msg->method().c_str(), msg->host().first.c_str(), msg->host().second);
                    }
                    return heartbeatResponse(conn,msg,update_success,ServiceOpType::HEARTBEAT_PROVIDER);
                }
                else{
                    LCZ_ERROR("收到服务操作请求，但是操作类型错误");
                    return errResponse(conn,msg);
                }
            }
            // 连接关闭：仅清理内存映射，不删持久化数据。给 provider 重连窗口，超时由 sweep 兜底
            void onconnShoutdown(const BaseConnection::ptr& conn)
            {
                _rstore->cleanConnKeys(conn);
                _discoverer->delProvider(conn);
            }
            // 扫描超时 provider、删除并通知发现者
            std::vector<std::pair<std::string, HostInfo>> sweepAndNotify(int idle_timeout_sec) {
                auto expired = _rstore->sweepExpired(std::chrono::seconds(idle_timeout_sec));
                for (auto &pr : expired) {
                    _discoverer->offlineNotify(pr.first, pr.second);
                }
                return expired;
            }
            private:
            // 发送操作类型错误响应
            void errResponse(const BaseConnection::ptr& conn,const ServiceRequest::ptr& msg)
            {
                auto msg_resp=MessageFactory::create<ServiceResponse>();
                msg_resp->setId(msg->rid());
                msg_resp->setRcode(RespCode::INVALID_OPTYPE);
                msg_resp->setMsgType(MsgType::RSP_SERVICE);
                msg_resp->setOptype(ServiceOpType::UNKNOWN);
                conn->send(msg_resp);

            }
            // 发送服务注册成功响应
            void registryResponse(const BaseConnection::ptr& conn,const ServiceRequest::ptr& msg)
            {
                auto msg_resp=MessageFactory::create<ServiceResponse>();
                //组织响应信息
                msg_resp->setId(msg->rid());
                msg_resp->setRcode(RespCode::SUCCESS);
                msg_resp->setMsgType(MsgType::RSP_SERVICE);
                msg_resp->setOptype(ServiceOpType::REGISTER);
                conn->send(msg_resp);
            }
            // 发送服务发现响应（含主机列表及负载）
            void discoverResponse(const BaseConnection::ptr& conn,const ServiceRequest::ptr& msg)
            {
                auto msg_resp=MessageFactory::create<ServiceResponse>();
                //组织响应信息
                msg_resp->setId(msg->rid());
                msg_resp->setRcode(RespCode::SUCCESS);
                msg_resp->setMsgType(MsgType::RSP_SERVICE);
                msg_resp->setOptype(ServiceOpType::DISCOVER);
                msg_resp->setHost(_rstore->methodHost(msg->method()));
                auto hosts = _rstore->methodHostDetails(msg->method());
                msg_resp->setHostDetails(hosts);
                conn->send(msg_resp);
            }
            // 发送负载上报响应
            void updateloadResponse(const BaseConnection::ptr& conn,const ServiceRequest::ptr& msg,bool update_success)
            {
               
                auto msg_resp=MessageFactory::create<ServiceResponse>();
                if(!update_success){
                    LCZ_ERROR("load report failed: %s %s:%d 未找到对应服务",
                        msg->method().c_str(),
                        msg->host().first.c_str(),
                        msg->host().second);
                   msg_resp->setRcode(RespCode::SERVICE_NOT_FOUND);
                }
                else{
                    msg_resp->setRcode(RespCode::SUCCESS);
                }
                msg_resp->setId(msg->rid());
                msg_resp->setMsgType(MsgType::RSP_SERVICE);
                msg_resp->setOptype(ServiceOpType::LOAD_REPORT);
                conn->send(msg_resp);
            }
            // 发送心跳响应
            void heartbeatResponse(const BaseConnection::ptr& conn,const ServiceRequest::ptr& msg,bool update_success,ServiceOpType optype)
            {
                auto msg_resp=MessageFactory::create<ServiceResponse>();
                if(!update_success){
                    LCZ_ERROR("心跳检测失败: %s %s:%d 未找到对应服务提供者",
                        msg->method().c_str(),
                        msg->host().first.c_str(),
                        msg->host().second);
                    msg_resp->setRcode(RespCode::SERVICE_NOT_FOUND);
                }
                else{
                    msg_resp->setRcode(RespCode::SUCCESS);
                }
                msg_resp->setId(msg->rid());
                msg_resp->setMsgType(MsgType::RSP_SERVICE);
                msg_resp->setOptype(optype);
                conn->send(msg_resp);
            }
            private:
            IRegistryStore::ptr _rstore;
            DiscoverManager::ptr _discoverer;

        };

    } // namespace server
} // namespace lcz_rpc
