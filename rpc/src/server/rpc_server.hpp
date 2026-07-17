#pragma once
#include "../general/dispacher.hpp"
#include "rpc_registry.hpp"
#include "rpc_router.hpp"
#include "../client/rpc_client.hpp"
#include "rpc_topic.hpp"
#include "../general/net.hpp"
#include "../general/publicconfig.hpp"
#include "../general/log_system/lcz_log.h"
#include "memory_registry_store.hpp"
#include "etcd_registry_store.hpp"
#include "leader_election.hpp"
#include "memory_leader_election.hpp"
#include "etcd_leader_election.hpp"
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <thread>
namespace lcz_rpc
{
    namespace server
    {
        // 注册中心服务端类：提供服务注册/发现/心跳扫描
        class RegistryServer
        {
        public:
            using ptr = std::shared_ptr<RegistryServer>;
            RegistryServer(int port)
                : _pdmanager(makePdmanager()), _leader_elector(makeLeaderElector()), _dispacher(std::make_shared<Dispacher>())
            {
                auto service_cb = std::bind(&PwithDManager::onserviceRequest, _pdmanager.get(), std::placeholders::_1, std::placeholders::_2);
                _dispacher->registerhandler<ServiceRequest>(MsgType::REQ_SERVICE, service_cb);
                _server = lcz_rpc::ServerFactory::create(port);
                auto msg_cb = std::bind(&lcz_rpc::Dispacher::onMessage, _dispacher.get(), std::placeholders::_1, std::placeholders::_2);
                _server->setMessageCallback(msg_cb);
                auto close_cb = std::bind(&RegistryServer::onconnShoutdown, this, std::placeholders::_1);
                _server->setCloseCallback(close_cb);
                // server->setConnectionCallback(onConnection);
                // 启动心跳扫描定时器
                _hb_loop_ptr = _hb_loop.startLoop(); // 启动心跳扫描线程的事件循环
                _leader_elector->start(_hb_loop_ptr);
                // 心跳扫描定时器回调：多实例 HA 下仅 leader 执行 sweep 剔除过期 provider
                // follower 不执行 sweep，其 discoverer 依赖客户端 health check（每 10s）兜底发现 offline provider
                _hb_loop_ptr->runEvery(_hb_config.check_interval_sec, [this]()
                                       {
                    if (!_leader_elector->isLeader()) return;
                    LCZ_INFO("[RegistryServer-服务扫描] 开始扫描过期提供者，idle_timeout=%d秒",
                         _hb_config.idle_timeout_sec);
                    auto expired = _pdmanager->sweepAndNotify(_hb_config.idle_timeout_sec);
                    if (!expired.empty()) {
                        LCZ_INFO("[RegistryServer-服务扫描] 发现 %zu 个过期提供者，已通知下线", expired.size());
                        for (const auto &pr : expired) {
                            LCZ_INFO("[Registry] 剔除过期提供者 method=%s host=%s:%d (idle>%ds)",
                                     pr.first.c_str(), pr.second.first.c_str(), pr.second.second,
                                     _hb_config.idle_timeout_sec);
                        }
                    } });
            }
            // 启动注册中心服务器
            void start()
            {
                _server->start();
            }
            // 优雅退出
            void stop()
            {
                _hb_loop_ptr->quit();    // 停止心跳扫描事件循环
                _leader_elector->stop(); // 释放选举租约
                _server->stop();         // 停止监听
            }

        private:
            // 根据环境变量 LCZ_ETCD 选择存储后端
            static PwithDManager::ptr makePdmanager()
            {
                const char *etcd_url = std::getenv("LCZ_ETCD"); // 获取LCZ_ETCD这个环境变量的值
                if (etcd_url && etcd_url[0] != '\0')
                {
                    LCZ_INFO("[Registry] 使用 Etcd 存储后端，etcd=%s", etcd_url);
                    return std::make_shared<PwithDManager>(
                        std::make_shared<EtcdRegistryStore>(etcd_url));
                }
                LCZ_INFO("[Registry] 使用内存存储后端");
                return std::make_shared<PwithDManager>(
                    std::make_shared<MemoryRegistryStore>());
            }
            // 根据环境变量 LCZ_ETCD 选择选举后端
            static ILeaderElector::ptr makeLeaderElector()
            {
                const char *etcd_url = std::getenv("LCZ_ETCD");
                if (etcd_url && etcd_url[0] != '\0')
                {
                    LCZ_INFO("[Registry] 使用 Etcd 选举后端，etcd=%s", etcd_url);
                    return std::make_shared<EtcdLeaderElector>(etcd_url);
                }
                LCZ_INFO("[Registry] 使用内存选举后端（永远 leader）");
                return std::make_shared<MemoryLeaderElector>();
            }

            // 连接关闭时清理 provider/discoverer
            void onconnShoutdown(const BaseConnection::ptr &conn)
            {
                // demo 友好输出：Provider 进程退出/断开时，注册中心会立即感知连接断开
                // 注意：这种"即时下线"不会走超时扫描剔除逻辑
                LCZ_INFO("[Registry] 连接断开，触发下线处理");
                _pdmanager->onconnShoutdown(conn);
            }

        private:
            Dispacher::ptr _dispacher;
            PwithDManager::ptr _pdmanager;
            ILeaderElector::ptr _leader_elector;
            BaseServer::ptr _server;

            // 心跳扫描定时器（Muduo库实现）
            HeartbeatConfig _hb_config;                    // 心跳扫描配置
            muduo::net::EventLoopThread _hb_loop;          // 心跳扫描线程
            muduo::net::EventLoop *_hb_loop_ptr = nullptr; // 心跳扫描线程指针
        };

        // RPC 服务端类：提供 RPC 方法注册与调用，可选向注册中心注册并心跳/负载上报
        class RpcServer
        {
        public:
            using ptr = std::shared_ptr<RpcServer>;
            // 两套地址信息：1.rpc服务提供的访问地址信息2.注册中心服务端地址信息
            RpcServer(const HostInfo &access_addr, bool enablediscover = false, const HostInfo &registry_server_addr = HostInfo("", 0))
                : _access_addr(access_addr), _enablediscover(enablediscover), _dispacher(std::make_shared<Dispacher>()), _rpc_router(std::make_shared<RpcRouter>()), _proto_rpc_router(std::make_shared<ProtoRpcRouter>())
            {
                if (_enablediscover) // 如果启用服务发现，创建注册中心客户端
                {
                    _client_registry = std::make_shared<client::ClientRegistry>(registry_server_addr.first, registry_server_addr.second);
                    _report_loop_ptr = _report_loop.startLoop(); // 启动上报负载的线程的事件循环
                }
                // 注册 RPC 请求处理回调（JSON）
                auto rpc_cb = std::bind(&lcz_rpc::server::RpcRouter::onrpcRequst, _rpc_router.get(), std::placeholders::_1, std::placeholders::_2);
                _dispacher->registerhandler<lcz_rpc::RpcRequest>(lcz_rpc::MsgType::REQ_RPC, rpc_cb);
                // 路径二：纯 Proto RPC 请求处理
                auto proto_rpc_cb = std::bind(&lcz_rpc::server::ProtoRpcRouter::onProtoRequest, _proto_rpc_router.get(), std::placeholders::_1, std::placeholders::_2);
                _dispacher->registerhandler<lcz_rpc::ProtoRpcRequest>(lcz_rpc::MsgType::REQ_RPC_PROTO, proto_rpc_cb);
                //// 创建网络服务器实例
                _server = lcz_rpc::ServerFactory::create(access_addr.second, 4);
                // 设置消息处理回调
                auto msg_cb = std::bind(&lcz_rpc::Dispacher::onMessage, _dispacher.get(), std::placeholders::_1, std::placeholders::_2);
                _server->setMessageCallback(msg_cb);
                // ---- Prometheus: rpc_connection_count 连接数指标 ----
                // MuduoServer::onConnection 在连接建立/断开时会调用这两个回调
                // （BaseServer 预留的槽位，RpcServer 此前未使用），
                // 回调体只做一次原子加减，跑在 muduo IO 线程里，开销纳秒级
                _server->setConnectionCallback([](const BaseConnection::ptr &)
                                               { metrics::MetricHooks::onConnectionOpen(); });
                _server->setCloseCallback([](const BaseConnection::ptr &)
                                          { metrics::MetricHooks::onConnectionClose(); });

                // RpcServer 仅维持 Provider 心跳与负载上报
            }
            // 注册 RPC 方法；若启用发现则同步向注册中心注册并启动心跳/负载上报
            void registerMethod(const ServiceDescribe::ptr &service)
            {
                if (_enablediscover) // 如果启用服务发现，向注册中心注册方法
                {
                    int currentLoad = this->currentLoad();
                    bool ok = false;
                    // 注册中心启动窗口期内可能暂不可用，最多重试 3 次（每次间隔 1s）
                    for (int attempt = 1; attempt <= 3; ++attempt)
                    {
                        ok = _client_registry->methodRegistry(service->getMethodname(), _access_addr, currentLoad);
                        if (ok)
                            break;
                        LCZ_WARN("[Provider] 注册到 Registry 失败，method=%s host=%s:%d attempt=%d/3",
                                 service->getMethodname().c_str(), _access_addr.first.c_str(),
                                 _access_addr.second, attempt);
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                    }
                    if (ok)
                    {
                        LCZ_INFO("[Provider] 注册成功 method=%s host=%s:%d load=%d",
                                 service->getMethodname().c_str(), _access_addr.first.c_str(),
                                 _access_addr.second, currentLoad);
                        {
                            std::unique_lock<std::mutex> lock(_methods_mutex);
                            _registered_methods.emplace_back(service->getMethodname());
                        }
                        if (!_report_started.exchange(true)) // 原子地将_report_started设置为true
                        {
                            // 每3秒上报一次负载,绑定_client_registry的reportLoad方法
                            _report_loop_ptr->runEvery(
                                3.0, // 周期按需配置
                                std::bind(&RpcServer::reportLoadTick, this));
                            // heartbeat_interval_sec秒发送一次心跳,绑定_client_registry的heartbeatTick方法
                            _report_loop_ptr->runEvery(
                                static_cast<double>(_hb_config.heartbeat_interval_sec) /*这是给runEvery方法的参数，表示心跳间隔时间*/,
                                std::bind(&RpcServer::heartbeatTick, this));
                        }
                    }
                    else
                    {
                        LCZ_ERROR("[Provider] 注册到 Registry 最终失败，method=%s host=%s:%d",
                                  service->getMethodname().c_str(), _access_addr.first.c_str(),
                                  _access_addr.second);
                    }
                }
                // 在路由器中注册方法（线程安全）
                _rpc_router->registerMethod(service);
            }
            // 路径二：注册纯 Proto RPC 方法，热路径零 JSON
            template <typename Req, typename Resp>
            void registerProtoHandler(const std::string &method,
                                      std::function<void(const BaseConnection::ptr &, const Req &, Resp *)> handler)
            {
                _proto_rpc_router->registerProtoHandler<Req, Resp>(method, std::move(handler));
            }
            // 启动服务器（阻塞）
            void start() { _server->start(); }

            // 设置令牌桶限流器（对所有 RPC 入口生效），不设置则不限流
            void setRateLimiter(int rate_per_sec, int burst)
            {
                auto limiter = std::make_shared<TokenBucket>(rate_per_sec, burst);
                _rpc_router->setRateLimiter(limiter);
                _proto_rpc_router->setRateLimiter(limiter);
            }
            // 注入序列化器，默认 ProtobufSerializer，可替换为 FlatBuffers 等
            void setSerializer(std::shared_ptr<ISerializer> s)
            {
                _server->setSerializer(s);
            }

            // 优雅退出：停止上报定时器 → 停止服务器 → 等待后台线程
            void stop()
            {
                if (_enablediscover && _report_loop_ptr)
                    _report_loop_ptr->quit(); // 停止心跳+负载上报的事件循环
                _server->stop();              // 唤醒 muduo 事件循环使其从 start() 返回
                if (_server_thread.joinable())
                    _server_thread.join(); // 等待后台线程退出
            }

            // 非阻塞启动：在后台线程运行 muduo 事件循环，主线程可继续调用 registerMethod/registerProtoHandler
            void startInThread()
            {
                if (_server_thread.joinable())
                {
                    LCZ_WARN("RpcServer 已经在运行中");
                    return;
                }
                _server_thread = std::thread([this]()
                                             {
                                                 _server->start(); // 在单独线程中阻塞运行
                                             });
                LCZ_INFO("RpcServer 已在后台线程启动，主线程可继续调用 registerMethod()");
            }

            // 等待 startInThread 启动的线程结束
            void wait()
            {
                if (_server_thread.joinable())
                {
                    _server_thread.join();
                }
            }

            ~RpcServer()
            {
                stop();
            }

        private:
            // 读取 /proc/loadavg 1 分钟负载均值，除以 CPU 核数后 ×100 归一化到 [0, 100]
            int currentLoad() const
            {
                std::ifstream ifs("/proc/loadavg");
                if (!ifs)
                    return 0;
                float load1 = 0;
                ifs >> load1;
                int nproc = static_cast<int>(std::thread::hardware_concurrency());
                if (nproc < 1)
                    nproc = 1;
                int load = static_cast<int>((load1 / nproc) * 100);
                return load > 100 ? 100 : load;
            }
            // 定时器回调：向注册中心上报当前负载
            void reportLoadTick()
            {
                if (!_enablediscover || !_client_registry)
                    return;
                const int load = currentLoad();

                std::vector<std::string> methods;
                {
                    std::unique_lock<std::mutex> lock(_methods_mutex);
                    methods = _registered_methods; // 获取已注册的方法
                }
                for (const auto &method : methods)
                {
                    if (!_client_registry->reportLoad(method, _access_addr, load))
                    {
                        LCZ_WARN("reportLoad 失败: method=%s", method.c_str());
                    }
                }
            }
            // 定时器回调：向注册中心发送心跳
            void heartbeatTick()
            {
                if (!_enablediscover || !_client_registry)
                    return; // 如果未启用服务发现或注册中心客户端为空，则返回
                std::vector<std::string> methods;
                {
                    std::unique_lock<std::mutex> lock(_methods_mutex);
                    methods = _registered_methods; // 获取已注册的方法
                }
                LCZ_INFO("[RpcServer-Provider心跳定时器] 开始发送心跳，method数量=%zu", methods.size());
                // 遍历已注册的方法，发送心跳给注册中心
                for (const auto &method : methods)
                {
                    // 发送心跳给注册中心，成功/失败都记入 Prometheus:
                    // registry_heartbeats_total 总数 +1，失败额外记 registry_heartbeat_errors_total
                    // （心跳持续失败通常意味着注册中心不可达或 etcd lease 失效）
                    bool ok = _client_registry->heartbeatProvider(method, _access_addr);
                    metrics::MetricHooks::onRegistryHeartbeat(method, ok);
                    if (!ok)
                    {
                        LCZ_WARN("[RpcServer-Provider心跳失败] method=%s", method.c_str());
                    }
                }
            }

        private:
            HostInfo _access_addr;                        // 本机RPC服务访问地址
            bool _enablediscover;                         // 是否启用服务发现
            client::ClientRegistry::ptr _client_registry; // 注册中心客户端
            Dispacher::ptr _dispacher;                    // 消息分发器
            RpcRouter::ptr _rpc_router;                   // RPC路由器
            ProtoRpcRouter::ptr _proto_rpc_router;        // 路径二：纯 Proto RPC 路由器
            BaseServer::ptr _server;                      // 网络服务器

            HeartbeatConfig _hb_config; // 心跳配置

            // 这是和负载上报相关的设置
            muduo::net::EventLoopThread _report_loop;          // 上报负载的线程
            muduo::net::EventLoop *_report_loop_ptr = nullptr; // 上报负载的线程指针
            muduo::net::TimerId _report_timer;                 // 上报负载的定时器
            std::mutex _methods_mutex;                         // 方法互斥锁
            std::vector<std::string> _registered_methods;      // 已注册的方法
            std::atomic<bool> _report_started{false};          // CAS 保证多个 registerMethod 调用只启动一次心跳+负载上报定时器
            std::thread _server_thread;                        // 服务器运行线程（用于非阻塞启动）
        };
        // 主题服务端类：提供主题的创建/删除/订阅/发布
        class TopicServer
        {
        public:
            using ptr = std::shared_ptr<TopicServer>;
            TopicServer(int port)
                : _topicmanager(std::make_shared<TopicManager>()), _dispacher(std::make_shared<Dispacher>())
            {
                auto service_cb = std::bind(&TopicManager::ontopicRequest, _topicmanager.get(), std::placeholders::_1, std::placeholders::_2);
                _dispacher->registerhandler<TopicRequest>(MsgType::REQ_TOPIC, service_cb);
                _server = lcz_rpc::ServerFactory::create(port);
                auto msg_cb = std::bind(&lcz_rpc::Dispacher::onMessage, _dispacher.get(), std::placeholders::_1, std::placeholders::_2);
                _server->setMessageCallback(msg_cb);
                auto close_cb = std::bind(&TopicServer::onconnShoutdown, this, std::placeholders::_1);
                _server->setCloseCallback(close_cb);
            }
            // 启动主题服务器
            void start()
            {
                _server->start();
            }
            // 优雅退出
            void stop()
            {
                _server->stop();
            }

        private:
            // 连接关闭时清理订阅者
            void onconnShoutdown(const BaseConnection::ptr &conn)
            {
                _topicmanager->onconnShoutdown(conn);
            }

        private:
            lcz_rpc::server::TopicManager::ptr _topicmanager;
            Dispacher::ptr _dispacher;
            BaseServer::ptr _server;
        };

    } // namespace server
}