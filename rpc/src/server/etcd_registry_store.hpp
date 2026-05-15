#pragma once
#include "./registry_store.hpp"
#include "rpc_registry.hpp"
#include "curl/curl.h"
#include <chrono>

// 通过底层libcurl 通过http调用etcd客户端
namespace lcz_rpc
{
    namespace server
    {
        class EtcdRegistryStore : public IRegistryStore
        {
        public:
            using ptr = std::shared_ptr<EtcdRegistryStore>;
            EtcdRegistryStore(const std::string &endpoints);
            ~EtcdRegistryStore() override;

            EtcdRegistryStore(const EtcdRegistryStore &) = delete;
            EtcdRegistryStore &operator=(const EtcdRegistryStore &) = delete;

            // key 构造：/lcz-rpc/v1/providers/<method>/<ip>:<port>
            std::string key_for(const std::string &method, const HostInfo &host);
            // base64 编解码（etcd REST API 要求 key/value 用 base64 传）
            static std::string base64_encode(const std::string &s);
            static std::string base64_decode(const std::string &s);

            // curl 二次封装 —— 三个 HTTP 方法对应三个 etcd API
            bool http_put(const std::string &key, const std::string &value); // 调用 /v3/kv/put，key/value 先 base64 编码
            // 调用 /v3/kv/range，返回解码后的 key/value 列表
            std::vector<std::pair<std::string, std::string>> http_get_prefix(const std::string &prefix);
            bool http_delete(const std::string &key); // 调用 /v3/kv/deleterange

            // curl 底层调用：发 POST，返回响应体
            std::string curl_post(const std::string &path, const std::string &json_body);

            // etcd v3 lease / txn API（供 EtcdLeaderElector 调用 etcd 进行分布式选举）
            // 注意：EtcdLeaderElector 拥有独立的 CURL* 句柄，不使用这里的 curl_post()
            //       这些方法仅作为备用便利封装，当前选举实现直接构造 JSON 自行 curl
            int64_t http_lease_grant(int ttl_sec);                   // POST /v3/lease/grant → 返回 lease_id，失败 -1，创建一个新的租约
            bool http_lease_keepalive(int64_t lease_id);             // POST /v3/lease/keepalive → 续约一次
            bool http_lease_revoke(int64_t lease_id);                // POST /v3/kv/lease/revoke → 主动撤销 lease
            bool http_txn_create_if_absent(const std::string &key,   // POST /v3/kv/txn → CAS compare version==0
                                           const std::string &value, //    成功则 put 并绑定 lease
                                           int64_t lease_id);        //    返回 succeeded 字段

            void registerInstance(
                const BaseConnection::ptr &conn,
                const ::lcz_rpc::HostInfo &host,
                const std::string &method,
                int load) override; // 添加服务提供者

            std::vector<::lcz_rpc::HostInfo> methodHost(const std::string &method) override;
            std::vector<::lcz_rpc::HostDetail> methodHostDetails(const std::string &method) override;

            bool reportLoad(
                const std::string &method,
                const ::lcz_rpc::HostInfo &host,
                int load) override; // 负载上报

            bool heartbeat(
                const std::string &method,
                const ::lcz_rpc::HostInfo &host) override; // 心跳

            std::vector<std::pair<std::string, ::lcz_rpc::HostInfo>> disconnectProvider(
                const BaseConnection::ptr &conn) override; // 断开提供者

            void cleanConnKeys(const BaseConnection::ptr &conn) override;

            std::vector<std::pair<std::string, ::lcz_rpc::HostInfo>> sweepExpired(
                std::chrono::seconds idle) override; // 超时扫描

        private:
            static HostInfo host_from_key(const std::string &key); // 从 key "....../10.0.0.1:8080" 的末尾解析出 HostInfo

            static int load_from_value(const std::string &v); // 从 value JSON 解析 load

            static int64_t ts_from_value(const std::string &v); // 从 value JSON 解析 ts
        private:
            std::mutex _mutex;                     // 保护 _keys_by_conn_
            std::mutex _curl_mutex;                // 保护 _curl（libcurl 禁止同一句柄多线程并发）
            CURL *_curl;                           // libcurl句柄，构造时 init，析构时 cleanup，全程复用
            struct curl_slist *_headers = nullptr; // HTTP 请求头
            std::string _etcd_pos;                 // etcd地址，"http://127.0.0.1:2379"
            int64_t _start_time;                   // 构造时刻（毫秒时间戳），sweepExpired 启动后宽限期内不剔除
            // 记录每个连接写入过哪些 etcd key，断连时批量删除
            // 不需要随时通过连接查找provider，使用uintptr_t-不延长连接对象的生命周期
            std::unordered_map<uintptr_t, std::vector<std::string>> _keys_by_conn;
        };
    }
}