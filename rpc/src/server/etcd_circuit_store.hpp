#pragma once
#include <string>
#include <memory>
#include <chrono>
#include "circuit_store.hpp"
#include "curl/curl.h"
#include <mutex>
#include <vector>
namespace lcz_rpc
{
    namespace server
    {
        // 熔断器etcd存储
        class EtcdCircuitStore : public ICircuitStateStore
        {
        public:
            using ptr = std::shared_ptr<EtcdCircuitStore>;
            EtcdCircuitStore(const std::string &endpoints);
            ~EtcdCircuitStore() override;

            EtcdCircuitStore(const EtcdCircuitStore &) = delete;
            EtcdCircuitStore &operator=(const EtcdCircuitStore &) = delete;

            // 状态变更时保存
            virtual bool save(const std::string &method,
                              const std::string &host, const CircuitStatus &status) override;
            // 启动时读加载
            virtual CircuitStatus load(const std::string &method,
                                       const std::string &host) override;
            // provider下线清理
            virtual bool remove(const std::string &method,
                                const std::string &host) override;

            // key 构造：/lcz-rpc/v1/circuit-breakers/<method>/<host>
            std::string key_for(const std::string &method, const std::string &host);
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

        private:
            std::mutex _curl_mutex;                // 保护 _curl（libcurl 禁止同一句柄多线程并发）
            CURL *_curl;                           // libcurl句柄，构造时 init，析构时 cleanup，全程复用
            struct curl_slist *_headers = nullptr; // HTTP 请求头
            std::string _etcd_pos;                 // etcd地址，"http://127.0.0.1:2379"

        };

    }
}
