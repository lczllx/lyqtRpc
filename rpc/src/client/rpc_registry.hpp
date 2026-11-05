#pragma once
#include "requestor.hpp"
#include <algorithm>
#include <random>
#include <chrono>
#include <map>
#include "general/publicconfig.hpp"
#include "general/log_system/lcz_log.h"

static constexpr int MAX_IDX = 1000000000; // 轮询索引上限（10亿），防止无界增长溢出
static constexpr int VIRTUAL_NODES = 160;  // 一致性哈希：每个物理节点的虚拟节点数（Dubbo 同款默认）

namespace lcz_rpc
{
    namespace client
    {
        // 方法主机类：维护某 method 的主机列表，支持多种负载均衡策略选择主机
        class MethodHost
        {
        public:
            using ptr = std::shared_ptr<MethodHost>;
            MethodHost(const std::vector<HostDetail>& host) : _idx(0),_host(host),_rng(std::random_device()()),_hash() {}
            MethodHost() : _idx(0),_rng(std::random_device()()/*使用随机数生成器生成种子*/),_hash() {}
            // 添加或更新主机（同 host 则更新负载）
            void appendHost(const HostInfo &host,int load)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                //服务发现/负载上报可能多次收到同一个 host，如果老值不覆盖，新策略会永远看到旧负载
                auto it =std::find_if(_host.begin(),_host.end(),[&](const HostDetail& detail){return detail.host == host;});
                if(it!=_host.end())//在host存在时，更新负载就返回
                {
                    it->load = load;
                    return;
                }
                _host.emplace_back(HostDetail{host,load});
                // 新主机追加在末尾，已有主机的下标不变 ⇒ 只需插入它自己的虚拟节点。
                // 逐台 append 的总开销因此是 O(H·VIRTUAL_NODES)，而不是每台都全量重建的 O(H²·VIRTUAL_NODES)。
                if (_ring_built)
                    insertNodes(_host.size() - 1);
                else
                    rebuildRing(); // 带 host 构造的实例环还没建：一次性补齐，与旧行为一致
            }
            // 按指定策略从主机列表中选一个（单次选择不构造候选列表，保持热路径无分配）
            HostDetail selectHost(LoadBalanceStrategy strategy, const std::string &key = {})
            {
                std::unique_lock<std::mutex> lock(_mutex);
                if (_host.empty()) return HostDetail();//如果主机列表为空，则返回空
                switch (strategy)
                {
                case LoadBalanceStrategy::ROUND_ROBIN:
                    return pickRoundRobin();
                case LoadBalanceStrategy::RANDOM:
                    return pickRandom();
                case LoadBalanceStrategy::SOURCE_HASH:
                    return pickSourceHash(key);
                case LoadBalanceStrategy::LOWEST_LOAD:
                    return pickLowestLoad();
                case LoadBalanceStrategy::CONSISTENT_HASH:
                    return pickConsistentHash(key);
                }
                return HostDetail();//如果策略不合法，则返回空
            }
            // 以下 pick* 均为热路径单次选择，调用方必须已持有 _mutex
            // 轮询选择
            HostDetail pickRoundRobin()
            {
                if (_host.empty()) return HostDetail();
                auto pos = _idx++ % _host.size();
                if (_idx >= MAX_IDX) _idx %= _host.size();//防止索引溢出
                return _host[pos];
            }
            // 随机选择
            HostDetail pickRandom()
            {
                if (_host.empty()) return HostDetail();
                std::uniform_int_distribution<size_t> dist(0, _host.size() - 1);
                auto pos = dist(_rng);
                return _host[pos];
            }
            // 按 key 哈希固定选同一主机
            HostDetail pickSourceHash(const std::string &key)
            {
                if (key.empty()) return pickRandom();//如果key为空，则随机选择主机
                if (_host.empty()) return HostDetail();
                size_t hash_val = _hash(key);
                size_t pos = hash_val % _host.size();
                return _host[pos];
            }
            // 选负载最低的主机（同负载则轮询打散，避免所有请求砸向同一台）
            HostDetail pickLowestLoad()
            {
                if (_host.empty()) return HostDetail();
                //负载最优+轮询分配
                int best_load = std::numeric_limits<int>::max();//初始化最佳负载为int最大值
                std::vector<size_t> candidates;//所有负载最优的主机索引

                //负载最优筛选 遍历主机列表，记录负载最小的主机索引
                for (size_t i = 0; i < _host.size(); ++i)
                {
                    const auto &detail = _host[i];
                    if (detail.load < best_load)//发现更低的负载
                    {
                        best_load = detail.load;
                        candidates.clear();//清空候选集
                        candidates.push_back(i);//添加当前主机索引
                    }
                    else if (detail.load == best_load)
                    {
                        candidates.push_back(i);
                    }
                }
                if (candidates.empty()) return HostDetail();
                //轮询分配 配合_idx防止溢出，确保轮询顺序
                size_t cur_pos = _idx++;
                if (_idx >= MAX_IDX) _idx %= _host.size();
                auto pos = candidates[cur_pos % candidates.size()];
                return _host[pos];
            }
            // 一致性哈希选择：key 哈希后顺时针在哈希环上找第一个虚拟节点
            HostDetail pickConsistentHash(const std::string &key)
            {
                if (key.empty()) return pickRandom(); // 无 key 退化成随机（与 SOURCE_HASH 一致）
                if (_ring.empty()) return pickRandom(); // 环未建（带 host 构造）时退化随机，与 selectHosts 一致
                size_t hash_val = _hash(key);
                auto it = _ring.lower_bound(hash_val); // 顺时针找第一个 >= hash 的虚拟节点
                if (it == _ring.end()) it = _ring.begin(); // 越过环尾回绕到起点
                return _host[it->second];
            }
            // 按策略返回有序候选列表（主机去重，首元素与 selectHost 一致）。
            // 调用方重试时按顺序取下一个未试过的 host：一致性哈希/最低负载这类
            // 确定性策略原本每次都会选回同一台，重试等于重发到坏节点。
            std::vector<HostDetail> selectHosts(LoadBalanceStrategy strategy, const std::string &key = {})
            {
                std::unique_lock<std::mutex> lock(_mutex);
                std::vector<HostDetail> out;
                if (_host.empty()) return out;
                const size_t n = _host.size();
                out.reserve(n);
                switch (strategy)
                {
                case LoadBalanceStrategy::ROUND_ROBIN:
                {
                    // 先取模再自增，避免 uint64 溢出后取模语义变化
                    size_t start = _idx++ % n;
                    if (_idx >= MAX_IDX) _idx %= n; //防止索引溢出
                    for (size_t i = 0; i < n; ++i)
                        out.push_back(_host[(start + i) % n]);
                    return out;
                }
                case LoadBalanceStrategy::RANDOM:
                {
                    std::vector<size_t> order(n);
                    for (size_t i = 0; i < n; ++i) order[i] = i;
                    std::shuffle(order.begin(), order.end(), _rng);
                    for (size_t idx : order) out.push_back(_host[idx]);
                    return out;
                }
                case LoadBalanceStrategy::SOURCE_HASH:
                {
                    if (key.empty()) //如果key为空，则随机选择主机
                    {
                        randomOrder(out);
                        return out;
                    }
                    size_t start = _hash(key) % n;
                    for (size_t i = 0; i < n; ++i)
                        out.push_back(_host[(start + i) % n]);
                    return out;
                }
                case LoadBalanceStrategy::LOWEST_LOAD:
                {
                    // 按负载升序排列，同负载组内保持原顺序；
                    // 组内轮询起点与旧 selectHost 一致（_idx++ % 组大小）
                    std::vector<size_t> order(n);
                    for (size_t i = 0; i < n; ++i) order[i] = i;
                    std::stable_sort(order.begin(), order.end(), [this](size_t a, size_t b) {
                        return _host[a].load < _host[b].load;
                    });
                    size_t best_cnt = 1;
                    while (best_cnt < n && _host[order[best_cnt]].load == _host[order[0]].load)
                        ++best_cnt;
                    size_t start = _idx++;
                    if (_idx >= MAX_IDX) _idx %= n;
                    for (size_t i = 0; i < best_cnt; ++i)
                        out.push_back(_host[order[(start + i) % best_cnt]]);
                    for (size_t i = best_cnt; i < n; ++i)
                        out.push_back(_host[order[i]]);
                    return out;
                }
                case LoadBalanceStrategy::CONSISTENT_HASH:
                {
                    if (key.empty() || _ring.empty()) // 无 key/环未建退化随机
                    {
                        randomOrder(out);
                        return out;
                    }
                    size_t hash_val = _hash(key);
                    auto it = _ring.lower_bound(hash_val); // 顺时针找第一个 >= hash 的虚拟节点
                    if (it == _ring.end()) it = _ring.begin(); // 越过环尾回绕到起点
                    std::vector<bool> seen(n, false);
                    size_t remaining = n;
                    for (size_t step = 0; step < _ring.size() && remaining > 0; ++step)
                    {
                        size_t idx = it->second;
                        if (!seen[idx])
                        {
                            seen[idx] = true;
                            --remaining;
                            out.push_back(_host[idx]);
                        }
                        ++it;
                        if (it == _ring.end()) it = _ring.begin();
                    }
                    return out;
                }
                }
                return out; // 策略不合法
            }
            // 生成随机顺序的候选列表（调用方必须已持有 _mutex）
            void randomOrder(std::vector<HostDetail> &out)
            {
                std::vector<size_t> order(_host.size());
                for (size_t i = 0; i < _host.size(); ++i) order[i] = i;
                std::shuffle(order.begin(), order.end(), _rng);
                for (size_t idx : order) out.push_back(_host[idx]);
            }
            // 把第 i 台主机的虚拟节点插入环（调用方必须已持有 _mutex）
            void insertNodes(size_t i)
            {
                std::string hk = hostKey(_host[i].host);
                for (int v = 0; v < VIRTUAL_NODES; ++v)
                {
                    _ring[_hash(hk + "#" + std::to_string(v))] = i;
                }
            }
            // 全量重建哈希环（调用方必须已持有 _mutex）
            void rebuildRing()
            {
                _ring.clear();
                for (size_t i = 0; i < _host.size(); ++i)
                    insertNodes(i);
                _ring_built = true;
            }
            // 移除指定主机
            void removeHost(const HostInfo &host)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = std::find_if(
                    _host.begin(),
                    _host.end(),
                    [&](const HostDetail &detail) {
                        return detail.host == host;
                    });
                if (it != _host.end())
                {
                    _host.erase(it);
                    // erase 会让后面主机的下标整体前移，环里所有大于被删下标的映射都要改写，
                    // 全量重建比逐个平移简单且不会漏；下线通知是低频事件，这点开销可接受。
                    rebuildRing();
                }
            }
            // 默认轮询获取一个主机
            HostInfo getHost()
            {
                return selectHost(LoadBalanceStrategy::ROUND_ROBIN).host;
            }
            // 默认轮询获取主机详情
            HostDetail getHostDetail()
            {
                return selectHost(LoadBalanceStrategy::ROUND_ROBIN);
            }
            // 判断主机列表是否为空
            bool empty()
            {
                std::unique_lock<std::mutex> lock(_mutex);
                return _host.empty();
            }

        private:
            std::mutex _mutex;
            uint64_t _idx;//轮询索引 类型uint64_t防止溢出
            std::vector<HostDetail> _host;
            std::map<size_t, size_t> _ring;//一致性哈希环: hash(虚拟节点) -> _host 下标
            bool _ring_built = false;//环是否已建（带 host 构造的实例首次 append 时才建）
            std::mt19937 _rng;//随机数生成器
            std::hash<std::string> _hash;//hash函数
            //std::uniform_int_distribution<size_t> _dist;//随机数分布
        };
        // 提供者代理类：封装向注册中心注册/上报负载/发送心跳的请求
        class Provider
        {
        public:
            using ptr = std::shared_ptr<Provider>;
            Provider(const Requestor::ptr &requestor) : _requestor(requestor) {}
            // 向注册中心注册服务
            bool methodRegistry(const BaseConnection::ptr &conn, const std::string &method, const HostInfo &host,int load)
            {
                auto msg_req = MessageFactory::create<ServiceRequest>();
                msg_req->setId(uuid());
                msg_req->setMethod(method);
                msg_req->setMsgType(MsgType::REQ_SERVICE);
                msg_req->setHost(host);
                msg_req->setOptype(ServiceOpType::REGISTER);
                msg_req->setLoad(load);
                BaseMessage::ptr msg_resp;
                LCZ_DEBUG("methodRegistry send begin:%s -> %s:%d", method.c_str(), host.first.c_str(), host.second);
                bool ret = _requestor->send(conn, msg_req, msg_resp);
                if (!ret)
                {
                    LCZ_ERROR("服务注册失败:%s", method.c_str());
                    return false;
                }
                LCZ_DEBUG("methodRegistry send finish:%s", method.c_str());
                auto service_resp = std::dynamic_pointer_cast<ServiceResponse>(msg_resp);
                if (service_resp.get() == nullptr)
                {
                    LCZ_ERROR("向下转换失败");
                    return false;
                }
                if (service_resp->rcode() != RespCode::SUCCESS)
                {
                    LCZ_ERROR("注册失败:%s", errReason(service_resp->rcode()).c_str());
                    return false;
                }
               
                return true;
            }
            // 向注册中心上报负载
            bool reportLoad(const BaseConnection::ptr &conn,const std::string &method,const HostInfo &host,int load)
            {
                auto msg_req = MessageFactory::create<ServiceRequest>();
                msg_req->setId(uuid());
                msg_req->setMethod(method);
                msg_req->setMsgType(MsgType::REQ_SERVICE);
                msg_req->setHost(host);
                msg_req->setOptype(ServiceOpType::LOAD_REPORT);
                msg_req->setLoad(load);
                BaseMessage::ptr msg_resp;
                bool ret = _requestor->send(conn, msg_req, msg_resp);//上报信息给服务端
                if (!ret)
                {
                    LCZ_ERROR("上报负载失败:%s", method.c_str());
                    return false;
                }
              
                auto service_resp = std::dynamic_pointer_cast<ServiceResponse>(msg_resp);
                if (service_resp.get() == nullptr)
                {
                    LCZ_ERROR("向下转换失败");
                    return false;
                }
                if (service_resp->rcode() != RespCode::SUCCESS)
                {
                    LCZ_ERROR("负载上报失败:%s", service_resp ? errReason(service_resp->rcode()).c_str(): "响应类型非法");
                    return false;
                }
               
                return true;

            }
            // 向注册中心发送提供者心跳
            bool heartbeatProvider(const BaseConnection::ptr &conn,
                const std::string &method, const HostInfo &host)
            {
                LCZ_INFO("[Provider心跳-发送] method=%s host=%s:%d", method.c_str(), host.first.c_str(), host.second);
                auto msg_req = MessageFactory::create<ServiceRequest>();
                msg_req->setId(uuid());
                msg_req->setMethod(method);
                msg_req->setMsgType(MsgType::REQ_SERVICE);
                msg_req->setHost(host);
                msg_req->setOptype(ServiceOpType::HEARTBEAT_PROVIDER);
                BaseMessage::ptr msg_resp;
                bool ret = _requestor->send(conn, msg_req, msg_resp);
                if (!ret) { LCZ_ERROR("[Provider心跳-发送失败] method=%s", method.c_str()); return false; }
                auto resp = std::dynamic_pointer_cast<ServiceResponse>(msg_resp);
                if (!resp || resp->rcode() != RespCode::SUCCESS)
                {
                    LCZ_ERROR("[Provider心跳-响应失败] method=%s reason=%s", method.c_str(), resp ? errReason(resp->rcode()).c_str() : "响应类型非法");
                    return false;
                }
                LCZ_INFO("[Provider心跳-成功] method=%s host=%s:%d", method.c_str(), host.first.c_str(), host.second);
                return true;
            }
        private:
            Requestor::ptr _requestor;
        };
        // 服务发现类：请求注册中心获取主机列表，缓存并按策略选主机，处理上线/下线
        class Discover
        {
        public:
            using ptr = std::shared_ptr<Discover>;
            using OfflineCallback=std::function<void(const HostInfo&)>;
            Discover(const Requestor::ptr &requestor,const OfflineCallback& cb) : _requestor(requestor),_offline_cb(cb) {}
            // 处理上线/下线通知，更新 _method_host 缓存
            void onserviceRequest(const BaseConnection::ptr &conn,const ServiceRequest::ptr &req)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                std::string method = req->method();
                auto host=req->host();
                auto type = req->optype();
                if (type == ServiceOpType::ONLINE)
                {
                    auto it = _method_host.find(req->method());
                    if (it != _method_host.end())
                    {
                        
                       // TODO: 这里后续上线通知要带负载，现在还没有就先传 0
                       it->second->appendHost(req->host(), 0);
                    }
                    else
                    {
                        auto method_host=std::make_shared<MethodHost>();
                        method_host->appendHost(req->host(), 0);
                        _method_host[method]=method_host;
                    }
                }
                else if (type == ServiceOpType::OFFLINE)
                {
                    auto it = _method_host.find(req->method());
                    if (it != _method_host.end())
                    {
                        it->second->removeHost(req->host());
                        _offline_cb(req->host());//删除连接池的连接
                    }
                    else
                    {
                       return;
                    }
                }
                else
                {
                    return;
                }
            }
            // 发现服务：先查缓存，无则请求注册中心，按 strategy 选主机
            bool serviceDiscover(const BaseConnection::ptr &conn,
                                 const std::string &method,
                                 HostDetail &detail,
                                 LoadBalanceStrategy strategy,
                                 bool force_remote = false,
                                 const std::string &key = {})
            {
                std::vector<HostDetail> details;
                if (!serviceDiscover(conn, method, details, strategy, force_remote, key) || details.empty())
                    return false;
                detail = details.front();
                return true;
            }
            // 发现服务（候选列表版本）：返回按策略排序的全部主机，重试时逐个尝试
            bool serviceDiscover(const BaseConnection::ptr &conn,
                                 const std::string &method,
                                 std::vector<HostDetail> &details,
                                 LoadBalanceStrategy strategy,
                                 bool force_remote = false,
                                 const std::string &key = {})
            {
                if (!force_remote)
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it = _method_host.find(method);
                    if (it != _method_host.end())
                    {
                        details = it->second->selectHosts(strategy, key);
                        if (details.empty()) return false;
                        LCZ_INFO("[discover-cache] method=%s strategy=%d host=%s:%d load=%d candidates=%zu",
                            method.c_str(),
                            static_cast<int>(strategy),
                            details.front().host.first.c_str(),
                            details.front().host.second,
                            details.front().load,
                            details.size());
                        return true;
                    }
                }

                //如果缓存中没有，则发送请求获取主机列表
                auto msg_req = MessageFactory::create<ServiceRequest>();
                msg_req->setId(uuid());
                msg_req->setMethod(method);
                msg_req->setMsgType(MsgType::REQ_SERVICE);
                msg_req->setOptype(ServiceOpType::DISCOVER);
                BaseMessage::ptr msg_resp;
                bool ret = _requestor->send(conn, msg_req, msg_resp);
                if (!ret)
                {
                    LCZ_ERROR("服务发现失败:%s", method.c_str());
                    return false;
                }
                auto service_resp = std::dynamic_pointer_cast<ServiceResponse>(msg_resp);
                if (service_resp.get() == nullptr)
                {
                    LCZ_ERROR("向下转换失败");
                    return false;
                }
                if (service_resp->rcode() != RespCode::SUCCESS)
                {
                    LCZ_ERROR("服务发现失败:%s", errReason(service_resp->rcode()).c_str());
                    return false;
                }
                auto host_details = service_resp->hostsDetail(); // 注意这里
                 if (host_details.empty()) { LCZ_ERROR("服务发现失败，没有提供 %s 服务的主机", method.c_str()); return false; }

                 auto method_host = std::make_shared<MethodHost>();
                 for (const auto &detail : host_details) {
                     method_host->appendHost(detail.host, detail.load);
                 }

                 // 并发下另一线程可能已先写入缓存，或在请求在途期间收到了 ONLINE 推送。
                 // 二次检查后复用已有列表：在途响应是旧快照，覆盖它会丢掉更新的主机状态。
                 MethodHost::ptr cached;
                 {
                     std::unique_lock<std::mutex> lock(_mutex);
                     auto it = _method_host.find(method);
                     if (it != _method_host.end())
                         cached = it->second;
                     else
                     {
                         cached = method_host;
                         _method_host[method] = cached;
                     }
                 }
                 details = cached->selectHosts(strategy, key);
                 if (details.empty()) return false;
                 LCZ_INFO("[discover-cache] method=%s host=%s:%d load=%d candidates=%zu",
                    method.c_str(),
                    details.front().host.first.c_str(),
                    details.front().host.second,
                    details.front().load,
                    details.size());
                 return true;
            }
        private:
            std::mutex _mutex;
            OfflineCallback _offline_cb;
            std::unordered_map<std::string, MethodHost::ptr> _method_host;
            Requestor::ptr _requestor;
        };
    }
} // namespace lcz_rpc
