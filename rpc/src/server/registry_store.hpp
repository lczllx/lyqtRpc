#pragma once
#include "../general/publicconfig.hpp"
#include "../general/net.hpp"
#include<chrono>
namespace lcz_rpc
{
    namespace server
    {
        class IRegistryStore
        {
        public:
            using ptr=std::shared_ptr<IRegistryStore>;

            // 服务注册：关联连接 + 方法 + 地址 + 负载
            virtual void registerInstance(const BaseConnection::ptr &conn, 
                const HostInfo &host, const std::string &method, int load) = 0;
            // 服务发现：返回指定方法的全部主机地址
            virtual std::vector<HostInfo> methodHost(const std::string &method) = 0;
            // 服务发现（带负载）：返回指定方法的全部主机 + 负载
            virtual std::vector<HostDetail> methodHostDetails(const std::string &method) = 0;
            // 负载上报：更新指定 method+host 的负载值
            virtual bool reportLoad(const std::string &method, const HostInfo &host, int load) = 0;
            // 心跳：刷新指定 method+host 的最后心跳时间
            virtual bool heartbeat(const std::string &method, const HostInfo &host) = 0;
            // 连接断开：删除该连接关联的全部 provider，返回需要广播下线的 (method, host) 列表
            virtual std::vector<std::pair<std::string, HostInfo>> disconnectProvider(
                const BaseConnection::ptr &conn) = 0;
            // 连接断开时仅清理连接→key 映射（不删持久化数据），给 provider 重连窗口，超时由 sweep 兜底
            virtual void cleanConnKeys(const BaseConnection::ptr &conn) = 0;
            // 超时扫描：返回心跳超时的 (method, host) 列表
            virtual std::vector<std::pair<std::string, HostInfo>> 
            sweepExpired(std::chrono::seconds idle_timeout) = 0;

            virtual ~IRegistryStore() = default;
            //IRegistryStore() = delete;//抽象类不能实例化对象

        };
    }
}