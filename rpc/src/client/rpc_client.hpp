#pragma once
#include <iostream>
#include "requestor.hpp"
#include "caller.hpp"
#include "circuit_breaker.hpp"
#include "rpc_registry.hpp"
#include "rpc_topic.hpp"
#include <string>
#include <cstdlib>
#include "../general/publicconfig.hpp"
#include "../general/log_system/lcz_log.h"
#include "../general/metrics_hooks.hpp"
#include "../server/memory_circuit_store.hpp"
#include "../server/etcd_circuit_store.hpp"

namespace lcz_rpc
{
    
    namespace client
    {
        // 注册中心客户端类：服务提供者用，向注册中心注册/上报负载/发送心跳
        class ClientRegistry
        {
        public:
            using ptr = std::shared_ptr<ClientRegistry>;
            ClientRegistry(const std::string &ip, int port)
                : _requestor(std::make_shared<Requestor>()), _provider(std::make_shared<Provider>(_requestor)), _dispacher(std::make_shared<Dispacher>())
            {
                auto resp_cb = std::bind(&client::Requestor::onResponse, _requestor.get(), std::placeholders::_1, std::placeholders::_2);

                auto msg_cb = std::bind(&Dispacher::onMessage, _dispacher.get(), std::placeholders::_1, std::placeholders::_2);
                _dispacher->registerhandler<BaseMessage>(lcz_rpc::MsgType::RSP_RPC, resp_cb);
                _dispacher->registerhandler<BaseMessage>(lcz_rpc::MsgType::RSP_RPC_PROTO, resp_cb);
                _dispacher->registerhandler<BaseMessage>(lcz_rpc::MsgType::RSP_SERVICE, resp_cb);
                //注册REQ_SERVICE消息处理回调（注册中心可能发送上线/下线通知，虽然提供者通常不需要处理，但需要注册以避免报错）
                _dispacher->registerhandler<ServiceRequest>(lcz_rpc::MsgType::REQ_SERVICE,
                    [](const BaseConnection::ptr& conn, ServiceRequest::ptr& msg){
                        // 提供者不需要处理上线/下线通知，直接忽略
                        LCZ_DEBUG("ClientRegistry收到REQ_SERVICE消息，忽略");
                    });
                _client = lcz_rpc::ClientFactory::create(ip, port);
                _client->setMessageCallback(msg_cb);
                _client->connect();
            }
            // 向注册中心注册服务方法
            bool methodRegistry(const std::string &method, const HostInfo &host,int load)
            {
                if (!ensureConnected()) { LCZ_ERROR("连接获取失败,无法注册服务:%s", method.c_str()); return false; }
                return _provider->methodRegistry(_client->connection(), method, host, load);
            }
            // 向注册中心上报负载
            bool reportLoad(const std::string &method, const HostInfo &host,int load)
            {
                if (!ensureConnected()) { LCZ_ERROR("连接获取失败,无法上报负载:%s", method.c_str()); return false; }
                return _provider->reportLoad(_client->connection(), method, host, load);
            }
            // 向注册中心发送提供者心跳
            bool heartbeatProvider(const std::string &method, const HostInfo &host)
            {
                if (!ensureConnected()) { LCZ_ERROR("连接获取失败,无法发送心跳:%s", method.c_str()); return false; }
                return _provider->heartbeatProvider(_client->connection(), method, host);
            }

        private:
            // 确保连接有效，断开则重连
            bool ensureConnected()
            {
                if (_client->connected()) return true;
                LCZ_WARN("[ClientRegistry] 连接断开，尝试重连...");
                _client->connect();
                if (_client->connected())
                {
                    LCZ_INFO("[ClientRegistry] 重连成功");
                    return true;
                }
                LCZ_ERROR("[ClientRegistry] 重连失败");
                return false;
            }

            BaseClient::ptr _client;
            Requestor::ptr _requestor;
            client::Provider::ptr _provider;
            Dispacher::ptr _dispacher;

        };
        // 服务发现客户端类：向注册中心发现服务，维护主机缓存，支持健康检查
        class ClientDiscover
        {
        public:
            using ptr = std::shared_ptr<ClientDiscover>;
            ~ClientDiscover()
            {
                if (_health_loop_ptr) {
                    _health_loop_ptr->quit();
                }
                if (_client) {
                    _client->shutdown();
                }
            }
            ClientDiscover(const std::string &ip, int port,const Discover::OfflineCallback &cb)
                : _requestor(std::make_shared<Requestor>()), _discover(std::make_shared<Discover>(_requestor, cb)), _dispacher(std::make_shared<Dispacher>())
            {
                auto resp_cb = std::bind(&client::Requestor::onResponse, _requestor.get(), std::placeholders::_1, std::placeholders::_2);
                auto req_cb = std::bind(&client::Discover::onserviceRequest, _discover.get(), std::placeholders::_1, std::placeholders::_2);
                auto msg_cb = std::bind(&Dispacher::onMessage, _dispacher.get(), std::placeholders::_1, std::placeholders::_2);
                _dispacher->registerhandler<BaseMessage>(lcz_rpc::MsgType::RSP_RPC, resp_cb);
                _dispacher->registerhandler<BaseMessage>(lcz_rpc::MsgType::RSP_RPC_PROTO, resp_cb);
                _dispacher->registerhandler<BaseMessage>(lcz_rpc::MsgType::RSP_SERVICE, resp_cb);
                _dispacher->registerhandler<ServiceRequest>(lcz_rpc::MsgType::REQ_SERVICE, req_cb);
                _client = lcz_rpc::ClientFactory::create(ip, port);
                _client->setMessageCallback(msg_cb);
                _client->connect();
                // 启动健康检查线程：定期 force_remote=true 拉取最新主机列表，兜底缓存失效
                _health_loop_ptr = _health_loop.startLoop();
                _health_loop_ptr->runEvery(_hb_config.heartbeat_interval_sec, [this]{
                    std::vector<std::string> methods;
                    {
                        std::unique_lock<std::mutex> lock(_tracked_mutex);
                        methods.assign(_tracked_methods.begin(), _tracked_methods.end());
                    }
                    if (methods.empty()) return;

                    auto conn = _client->connection();
                    if (!conn || !conn->connected()) {
                        LCZ_WARN("[ClientDiscover-健康检查] 连接断开，暂不刷新");
                        return;
                    }

                    for (const auto& method : methods) {
                        HostDetail detail;
                        if (_discover->serviceDiscover(conn, method, detail, LoadBalanceStrategy::ROUND_ROBIN, true)) {
                            LCZ_DEBUG("[ClientDiscover-健康检查] method=%s 刷新成功 host=%s:%d load=%d",
                                 method.c_str(), detail.host.first.c_str(), detail.host.second, detail.load);
                        } else {
                            LCZ_WARN("[ClientDiscover-健康检查] method=%s 刷新失败，等待下次调用重新发现", method.c_str());
                        }
                    }
                });
            }

            // 发现服务，返回主机详情（支持负载均衡），结果写入 detail_bylast
            bool serviceDiscover(const std::string &method, HostDetail &detail_bylast,LoadBalanceStrategy strategy) {
                HostDetail detail;
                auto conn = _client->connection();
                if(conn.get() == nullptr || conn->connected() == false)
                {
                    LCZ_ERROR("连接获取失败,无法发现服务:%s", method.c_str());
                    return false;
                }
                if (_discover->serviceDiscover(conn, method, detail,strategy)) {
                    detail_bylast = detail;
                    {
                        std::unique_lock<std::mutex> lock(_tracked_mutex);
                        _tracked_methods.insert(method);
                    }
                    // 可以把 detail.load 缓存起来
                    return true;
                }
                return false;  // 别忘记有返回
            }
            // 注入序列化器到内部客户端
            void setSerializer(std::shared_ptr<ISerializer> s) { if (_client) _client->setSerializer(s); }
            // 发现服务，返回主机信息（支持负载均衡）
            bool serviceDiscover(const std::string &method, HostInfo &host,LoadBalanceStrategy strategy) {
                HostDetail detail;
                auto conn = _client->connection();
                if(conn.get() == nullptr || conn->connected() == false)
                {
                    LCZ_ERROR("连接获取失败,无法发现服务:%s", method.c_str());
                    return false;
                }
                if (_discover->serviceDiscover(conn, method, detail,strategy)) {
                    host = detail.host;
                    {
                        std::unique_lock<std::mutex> lock(_tracked_mutex);
                        _tracked_methods.insert(method);
                    }
                    return true;
                }
                return false;  // 别忘记有返回
            }
        private:
            BaseClient::ptr _client;
            Requestor::ptr _requestor;
            Discover::ptr _discover;
            Dispacher::ptr _dispacher;
            std::unordered_set<std::string> _tracked_methods;// 已经发现的 method 集合
            std::mutex _tracked_mutex;//方法集合互斥锁

            HeartbeatConfig _hb_config;//健康检查配置
            muduo::net::EventLoopThread _health_loop;//健康检查线程
            muduo::net::EventLoop* _health_loop_ptr = nullptr;//健康检查线程指针
        };

        // RPC 客户端类：支持直连或服务发现，提供同步/异步/回调三种 RPC 调用
        class RpcClient
        {
        public:
            using ptr = std::shared_ptr<RpcClient>;
            ~RpcClient()
            {
                if (_rpc_client) {
                    _rpc_client->shutdown();
                }
            }
            // enablediscover 是否开启服务发现
            RpcClient(bool enablediscover, const std::string &ip, int port)
                : _enablediscover(enablediscover),
                  _breaker(makeBreaker()),
                  _requestor(std::make_shared<Requestor>()),
                  _caller(std::make_shared<RpcCaller>(_requestor, _breaker)),
                  _dispacher(std::make_shared<Dispacher>()),
                  _loadbalance_strategy(LoadBalanceStrategy::ROUND_ROBIN)
            {
                auto resp_cb = std::bind(&client::Requestor::onResponse, _requestor.get(), std::placeholders::_1, std::placeholders::_2);
                _dispacher->registerhandler<BaseMessage>(lcz_rpc::MsgType::RSP_RPC, resp_cb);
                _dispacher->registerhandler<BaseMessage>(lcz_rpc::MsgType::RSP_RPC_PROTO, resp_cb);
                if (_enablediscover)
                {
                    auto offlinecb = std::bind(&RpcClient::delClient, this, std::placeholders::_1);
                    _discover_client = std::make_shared<ClientDiscover>(ip, port, offlinecb); // 设置服务下线删除长连接
                }
                else
                {
                    auto msg_cb = std::bind(&Dispacher::onMessage, _dispacher.get(), std::placeholders::_1, std::placeholders::_2);
                    _rpc_client = lcz_rpc::ClientFactory::create(ip, port);
                    _rpc_client->setMessageCallback(msg_cb);
                    _rpc_client->connect();
                }

            }
            // 注入序列化器，默认 ProtobufSerializer
            void setSerializer(std::shared_ptr<ISerializer> s)
            {
                if (_rpc_client) _rpc_client->setSerializer(s);
                if (_discover_client) _discover_client->setSerializer(s);
            }
            // 设置负载均衡策略
            void setloadbalanceStrategy(LoadBalanceStrategy strategy)
            {
                _loadbalance_strategy = strategy;
            }
            // 同步 RPC 调用
            bool call(const std::string &method_name, const Json::Value &params, Json::Value &result)
            {
                BaseClient::ptr client = getClient(method_name);
                if (client.get() == nullptr)
                {
                    LCZ_ERROR("服务获取失败：%s", method_name.c_str());
                    return false;
                }
                return _caller->call(client->connection(), method_name, params, result);
            }
            // 异步 RPC 调用，通过 future 获取结果
            bool call(const std::string &method_name, Json::Value &params, RpcCaller::RpcAsyncRespose &result)
            {
                BaseClient::ptr client = getClient(method_name);
                if (client.get() == nullptr)
                {
                    LCZ_ERROR("服务获取失败：%s", method_name.c_str());
                    return false;
                }
                return _caller->call(client->connection(), method_name, params, result);
            }
            // 回调式 RPC 调用
            bool call(const std::string &method_name, Json::Value &params, const RpcCaller::ResponseCallback &cb)
            {
                BaseClient::ptr client = getClient(method_name);
                if (client.get() == nullptr)
                {
                    LCZ_ERROR("服务获取失败：%s", method_name.c_str());
                    return false;
                }
                return _caller->call(client->connection(), method_name, params, cb);
            }
            // 纯 Proto RPC 调用（含 Prometheus 客户端指标）
            template<typename Req, typename Resp>
            bool call_proto(const std::string &method_name, const Req &req, Resp *resp,
                           std::chrono::milliseconds timeout = std::chrono::seconds(5))
            {
                BaseClient::ptr client = getClient(method_name);
                if (client.get() == nullptr)
                {
                    LCZ_ERROR("服务获取失败：%s", method_name.c_str());
                    return false;
                }
                // ---- Prometheus 客户端指标埋点 ----
                // onClientSend: rpc_client_requests_total +1、in-flight 并发 +1
                // onClientRecv: RTT 入直方图、并发 -1、失败时按具体错误类型计数
                // 这里测到的是"端到端 RTT"（含网络+服务端处理），
                // 与服务端 rpc_request_duration_us（仅 handler 耗时）互补
                auto t1 = std::chrono::steady_clock::now();
                lcz_rpc::metrics::MetricHooks::onClientSend(method_name);
                std::string error_code;
                bool ok = _caller->call_proto(client->connection(), method_name, req, resp, timeout, &error_code);
                auto t2 = std::chrono::steady_clock::now();
                double lat = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
                lcz_rpc::metrics::MetricHooks::onClientRecv(method_name, lat, error_code);
                return ok;
            }

        private:
            // 根据环境变量 LCZ_ETCD 创建熔断器存储后端
            static CircuitBreaker::ptr makeBreaker()
            {
                CircuitConfig cfg;
                const char *env;
                if ((env = std::getenv("LCZ_CB_FAILURE_THRESHOLD"))) cfg.failure_threshold = std::atoi(env);
                if ((env = std::getenv("LCZ_CB_OPEN_DURATION")))    cfg.open_duration_sec = std::atoi(env);
                if ((env = std::getenv("LCZ_CB_HALF_OPEN_MAX")))    cfg.half_open_max_req = std::atoi(env);

                const char *etcd_url = std::getenv("LCZ_ETCD");
                if (etcd_url && etcd_url[0] != '\0')
                {
                    LCZ_INFO("[RpcClient] 使用 Etcd 存储断路器状态，etcd=%s", etcd_url);
                    return std::make_shared<CircuitBreaker>(
                        cfg,
                        std::make_shared<lcz_rpc::server::EtcdCircuitStore>(etcd_url));
                }
                LCZ_INFO("[RpcClient] 使用内存存储断路器状态");
                return std::make_shared<CircuitBreaker>(
                    cfg,
                    std::make_shared<lcz_rpc::server::MemoryCircuitStore>());
            }

            // 收到 OFFLINE 通知时：连接池移除 → 熔断器清理
            void delClient(const HostInfo &host)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _rpc_clients.erase(host);
                _breaker->removeHost(hostKey(host));
            }
            // 创建新连接并加入连接池
            BaseClient::ptr newClient(const HostInfo &host)
            {
                BaseClient::ptr client;
                auto msg_cb = std::bind(&Dispacher::onMessage, _dispacher.get(), std::placeholders::_1, std::placeholders::_2);
                client = lcz_rpc::ClientFactory::create(host.first, host.second);
                client->setMessageCallback(msg_cb);
                // client->setConnectionCallback(onConnection);
                client->connect();
                putClient(host, client);
                return client;
            }
            // 根据 method 获取或创建对应的 RPC 客户端（支持服务发现）
            BaseClient::ptr getClient(const std::string &method)
            {
                BaseClient::ptr client;
                if (_enablediscover)
                {
                    HostDetail detail;
                    // 先通过服务发现获取提供者的地址信息
                    bool ret = _discover_client->serviceDiscover(method, detail,_loadbalance_strategy);
                    if (!ret)
                    {
                        LCZ_ERROR("服务发现失败");
                        return BaseClient::ptr();
                    }
                    HostInfo host = detail.host;
                    client = getClient(host);
                    // 如果没有实例化客户端就创建一个新的
                    if (client.get() == nullptr)
                    {
                        client = newClient(host);
                    }
                }
                else
                {
                    client = _rpc_client;
                }
                return client;
            }
            // 从连接池获取或创建该 host 的客户端
            BaseClient::ptr getClient(const HostInfo &host)
            {
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it = _rpc_clients.find(host);
                    if (it != _rpc_clients.end())
                    {
                        return it->second;
                    }
                }
                return newClient(host);
            }
            // 将客户端加入连接池
            void putClient(const HostInfo &host, BaseClient::ptr &client)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _rpc_clients[host] = client;
            }

        private:
            // HostInfo 的哈希仿函数：std::pair 没有 std::hash 特化，需要自定义
            struct HostHash
            {
                size_t operator()(const HostInfo &host)const
                {
                    std::string all = host.first + std::to_string(host.second);
                    return std::hash<std::string>{}(all);
                }
            };
            std::mutex _mutex;
            bool _enablediscover;
            BaseClient::ptr _rpc_client;
            std::unordered_map<HostInfo, BaseClient::ptr, HostHash> _rpc_clients; // 连接池（长连接），收到 OFFLINE 通知后通过 delClient 回调删除
            Requestor::ptr _requestor;
            ClientDiscover::ptr _discover_client; // 服务发现客户端
            CircuitBreaker::ptr _breaker;         // 熔断器（必须在 _caller 之前声明）
            RpcCaller::ptr _caller;
            Dispacher::ptr _dispacher;
            LoadBalanceStrategy _loadbalance_strategy;//负载均衡策略
        };
        // 主题客户端类：复用 Requestor 发送 TopicRequest，接收推送并分发到订阅回调
        class TopicClient
        {
            public:
            using ptr=std::shared_ptr<TopicClient>;
            ~TopicClient() = default;
            TopicClient(const std::string &ip, int port)
            :_requestor(std::make_shared<lcz_rpc::client::Requestor>()),_topicmanager(std::make_shared<TopicManager>(_requestor)),_dispacher(std::make_shared<Dispacher>()){
                //1.对发送请求后接收的响应的处理2.对消息推送请求进行处理3.将dispcher对应的messagecallback设置到rpc_client里面的msgcb
                auto topic_resp=std::bind(&Requestor::onResponse,_requestor.get(),std::placeholders::_1,std::placeholders::_2);
                _dispacher->registerhandler<BaseMessage>(MsgType::RSP_TOPIC,topic_resp);
                auto topicpub_cb=std::bind(&TopicManager::onTopicPublish,_topicmanager.get(),std::placeholders::_1,std::placeholders::_2);
                _dispacher->registerhandler<TopicRequest>(MsgType::REQ_TOPIC,topicpub_cb);
                  
                auto message_cb=std::bind(&Dispacher::onMessage,_dispacher.get(),std::placeholders::_1,std::placeholders::_2);                
                _topic_client=lcz_rpc::ClientFactory::create(ip,port);
                _topic_client->setMessageCallback(message_cb);
                _topic_client->connect();
            }
            // 下面几个封装函数都直接复用 TopicManager，同步等待服务端确认
            // 创建主题
            bool createTopic(const std::string &topic_name) {return _topicmanager->createTopic(_topic_client->connection(),topic_name);}
            // 删除主题
            bool removeTopic( const std::string &topic_name) {return _topicmanager->removeTopic(_topic_client->connection(),topic_name);}
            // 订阅主题，收到推送时调用 cb
            bool subscribeTopic(const std::string &topic_name,
                                const TopicManager::SubCallback &cb,
                                int priority = 0,
                                const std::vector<std::string> &tags = {})
            {
                return _topicmanager->subscribeTopic(_topic_client->connection(),
                                                     topic_name,
                                                     cb,
                                                     priority,
                                                     tags);
            }
            // 取消订阅
            bool cancelTopic( const std::string &topic_name){return _topicmanager->cancelTopic(_topic_client->connection(),topic_name);}
            // 向主题发布消息
            bool publishTopic(const std::string &topic_name,
                              const std::string &msg,
                              TopicForwardStrategy strategy = TopicForwardStrategy::BROADCAST,
                              int fanoutLimit = 0,
                              const std::string &shardKey = "",
                              int priority = 0,
                              const std::vector<std::string> &tags = {},
                              int redundantCount = 0)
            {
                return _topicmanager->publishTopic(_topic_client->connection(),
                                                   topic_name,
                                                   msg,
                                                   strategy,
                                                   fanoutLimit,
                                                   shardKey,
                                                   priority,
                                                   tags,
                                                   redundantCount);
            }
            // 关闭主题客户端连接
            void shutdown(){_topic_client->shutdown();}
            private:
            Requestor::ptr _requestor;
            client::TopicManager::ptr _topicmanager;
            Dispacher::ptr _dispacher;
            BaseClient::ptr _topic_client;

            //TopicClient 仅保留必要的 RPC 功能
        };
    }
} // namespace lcz_rpc
