#include "etcd_registry_store.hpp"
#include "curl/curl.h"

#include <sstream>
#include <jsoncpp/json/json.h>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>

// cb里面的*ptr是etcd客户端返回的响应，然后将其转为char*放入resp
// size是单个字节数，nmemb是块数
// userdata是resp
static size_t write_cb(void *ptr, size_t size, size_t nmemb, void *userdata)
{
    auto *s = static_cast<std::string *>(userdata);
    s->append(static_cast<char *>(ptr), size * nmemb);
    return size * nmemb; // 返回实际消费的字节数，不返回这个数 curl 会认为出错
}

namespace lcz_rpc
{
    namespace server
    {

        EtcdRegistryStore::EtcdRegistryStore(const std::string &endpoints) : _etcd_pos(endpoints), _curl(curl_easy_init())
        {
            _headers = curl_slist_append(_headers, "Content-Type: application/json");
            curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, _headers);
            _start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
        }
        EtcdRegistryStore::~EtcdRegistryStore()
        {
            curl_slist_free_all(_headers);
            curl_easy_cleanup(_curl);
        }

        // key 构造：/lcz-rpc/v1/providers/<method>/<ip>:<port>
        std::string EtcdRegistryStore::key_for(const std::string &method, const HostInfo &host)
        {
            return "/lcz-rpc/v1/providers/" + method + "/" + host.first + ":" +
                   std::to_string(host.second);
        }
        // base64 编解码（etcd REST API 要求 key/value 用 base64 传）
        std::string EtcdRegistryStore::base64_encode(const std::string &s)
        {
            using namespace boost::archive::iterators;
            using It = base64_from_binary<transform_width<std::string::const_iterator, 6, 8>>;
            std::string tmp(It(s.begin()), It(s.end()));
            while (tmp.size() % 4)
                tmp += '=';
            return tmp;
        }
        std::string EtcdRegistryStore::base64_decode(const std::string &s)
        {
            using namespace boost::archive::iterators;
            using It = transform_width<binary_from_base64<std::string::const_iterator>, 8, 6>;
            std::string res = s;
            while (!res.empty() && res.back() == '=')
                res.pop_back(); // 去掉 padding
            return std::string(It(res.begin()), It(res.end()));
        }

        // curl 二次封装 —— 三个 HTTP 方法对应三个 etcd API
        // 调用 /v3/kv/put，key/value 先 base64 编码
        bool EtcdRegistryStore::http_put(const std::string &key, const std::string &value)
        {
            //{"key":key,"value":value}
            std::string body = R"({"key":")" + base64_encode(key) +
                               R"(","value":")" + base64_encode(value) + R"("})";
            std::string resp = curl_post("/v3/kv/put", body);
            if (resp.empty()) LCZ_DEBUG("http_put fail key=%s", key.c_str());
            return !resp.empty();
        }
        // 调用 /v3/kv/range，返回解码后的 key/value 列表
        std::vector<std::pair<std::string, std::string>>
        EtcdRegistryStore::http_get_prefix(const std::string &prefix)
        {
            std::vector<std::pair<std::string, std::string>> ret;
            std::string e_key = base64_encode(prefix);

            // etcd 标准前缀匹配：range_end = prefix 最后一个字节 +1
            std::string range_end = prefix;
            for (int i = static_cast<int>(range_end.size()) - 1; i >= 0; --i)
            {
                if (static_cast<unsigned char>(range_end[i]) < 0xff)
                {
                    range_end[i] = static_cast<char>(static_cast<unsigned char>(range_end[i]) + 1);
                    range_end = range_end.substr(0, i + 1);
                    break;
                }
            }
            std::string e_end = base64_encode(range_end);
            std::string body = R"({"key":")" + e_key + R"(","range_end":")" + e_end + R"("})";
            std::string resp = curl_post("/v3/kv/range", body);
            if (resp.empty())
                return ret;
            LCZ_DEBUG("range resp prefix=%s body=%.200s", prefix.c_str(), resp.c_str());

            // resp 是 JSON，结构：{"kvs":[{"key":"...","value":"..."},...]}
            // 用 jsoncpp 解析
            Json::Value root;
            Json::CharReaderBuilder b;
            std::string errs;
            std::istringstream iss(resp);
            if (!Json::parseFromStream(b, iss, &root, &errs))
                return ret;

            if (!root.isMember("kvs"))
                return ret;

            for (const auto &kv : root["kvs"])
            {
                std::string k = base64_decode(kv["key"].asString());
                std::string v = base64_decode(kv["value"].asString());
                ret.emplace_back(k, v);
            }
            return ret;
        }
        // 调用 /v3/kv/deleterange
        bool EtcdRegistryStore::http_delete(const std::string &key)
        {
            std::string body = R"({"key":")" + base64_encode(key) + R"("})";
            std::string resp = curl_post("/v3/kv/deleterange", body);
            return resp.size();
        }

        // curl 底层调用：发 POST，返回响应体
        std::string EtcdRegistryStore::curl_post(const std::string &path,
                                                 const std::string &json_body)
        {
            // 组织url
            std::string url = _etcd_pos + path;
            std::string resp;
            std::lock_guard<std::mutex> lock(_curl_mutex);
            curl_easy_setopt(_curl, CURLOPT_URL, url.c_str());                // 设置请求url
            curl_easy_setopt(_curl, CURLOPT_POSTFIELDS, json_body.data());    // 设置POST请求体内容指针
            curl_easy_setopt(_curl, CURLOPT_POSTFIELDSIZE, json_body.size()); // 设请求体长度（字节数），不设的话 curl 会把 data 当 C 字符串按 strlen 取
            curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, write_cb);         // 设回调函数——curl 收到服务器响应数据时调它
            curl_easy_setopt(_curl, CURLOPT_WRITEDATA, &resp);                // 设回调函数的第四个参数（userdata），curl 把它原样传给 write_cb
            curl_easy_setopt(_curl, CURLOPT_TIMEOUT_MS, 3000L);               // 超时时间 3000 毫秒
            CURLcode rc = curl_easy_perform(_curl);
            if (rc != CURLE_OK) LCZ_DEBUG("curl fail rc=%d %s url=%s", rc, curl_easy_strerror(rc), url.c_str());
            return (rc == CURLE_OK) ? resp : "";
        }

        // -------------------- etcd v3 lease / txn API --------------------
        // 供 EtcdLeaderElector 进行分布式选举使用。
        // 注意：当前 EtcdLeaderElector 拥有独立 CURL* 句柄，自行构造 JSON 调用 etcd，
        //       并未直接调用这些方法。此处保留作为 EtcdRegistryStore 的 etcd API 完整封装。

        // POST /v3/lease/grant → {"ID": "12345"} 创建一个新的租约
        int64_t EtcdRegistryStore::http_lease_grant(int ttl_sec)
        {
            std::string body = R"({"TTL":)" + std::to_string(ttl_sec) + "}";
            std::string resp = curl_post("/v3/lease/grant", body);
            if (resp.empty()) return -1;
            Json::Value root;
            Json::Reader r;
            if (!r.parse(resp, root)) return -1;
            std::string id_str = root.get("ID", "").asString();
            if (id_str.empty()) return -1;
            return std::stoll(id_str); // lease ID 是 int64
        }

        // POST /v3/lease/keepalive → 续约一次，返回 {"TTL": "5"} 表示剩余 TTL
        bool EtcdRegistryStore::http_lease_keepalive(int64_t lease_id)
        {
            std::string body = R"({"ID":")" + std::to_string(lease_id) + R"("})";
            std::string resp = curl_post("/v3/lease/keepalive", body);
            if (resp.empty()) return false;
            Json::Value root;
            Json::Reader r;
            if (!r.parse(resp, root)) return false;
            return root.get("TTL", "").asString().size() > 0; // TTL 存在 = 续约成功
        }

        // POST /v3/kv/lease/revoke → 主动撤销 lease，绑定该 lease 的所有 key 立即删除
        bool EtcdRegistryStore::http_lease_revoke(int64_t lease_id)
        {
            std::string body = R"({"ID":")" + std::to_string(lease_id) + R"("})";
            std::string resp = curl_post("/v3/kv/lease/revoke", body);
            return !resp.empty();
        }

        // POST /v3/kv/txn → CAS 原子操作：compare version==0，成功则 put 并绑定 lease
        // key/value 需 base64 编码（etcd REST API 要求）
        // 返回根对象的 "succeeded" 字段
        bool EtcdRegistryStore::http_txn_create_if_absent(const std::string &key,
                                                           const std::string &value,
                                                           int64_t lease_id)
        {
            std::string b64_key = base64_encode(key);
            std::string b64_val = base64_encode(value);
            std::string body =
                R"({"compare":[{"key":")" + b64_key +
                R"(","result":"EQUAL","target":"VERSION","version":"0"}],)"      // key 不存在
                R"("success":[{"request_put":{"key":")" + b64_key +
                R"(","value":")" + b64_val +
                R"(","lease":)" + std::to_string(lease_id) + "}],"              // 创建并绑定 lease
                R"("failure":[]})";                                                // key 已存在则不做任何操作
            std::string resp = curl_post("/v3/kv/txn", body);
            if (resp.empty()) return false;
            Json::Value root;
            Json::Reader r;
            if (!r.parse(resp, root)) return false;
            return root.get("succeeded", false).asBool();
        }

        void EtcdRegistryStore::registerInstance(
            const BaseConnection::ptr &conn,
            const ::lcz_rpc::HostInfo &host,
            const std::string &method,
            int load)
        {
            std::string key = key_for(method, host);

            // 拼 value JSON
            auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

            //{"ip":"10.0.0.1","port":8080,"load":10,"timestamp":1718000000123} 毫秒时间戳
            std::string value = R"({"ip":")" + host.first + R"(","port":)" +
                                std::to_string(host.second) + R"(,"load":)" + std::to_string(load)
                                + R"(,"ts":)" + std::to_string(ts) + "}";

            http_put(key, value);//调用httpput，持久化到etcd

            // 记录这条 key 属于这个 conn，断连时批量删
            uintptr_t conn_id = reinterpret_cast<uintptr_t>(conn.get());
            {
                std::lock_guard<std::mutex> lock(_mutex);
                _keys_by_conn[conn_id].push_back(key);
            }
        }

        std::vector<::lcz_rpc::HostInfo> EtcdRegistryStore::methodHost(const std::string &method)
        {
            std::string prefix = "/lcz-rpc/v1/providers/" + method + "/";
            auto kvs = http_get_prefix(prefix);
            std::vector<::lcz_rpc::HostInfo> hosts;
            for (const auto &kv : kvs)
            {
                HostInfo h = host_from_key(kv.first);
                if (!h.first.empty())
                    hosts.push_back(h);
            }
            return hosts;
        }
        std::vector<::lcz_rpc::HostDetail> EtcdRegistryStore::methodHostDetails(const std::string &method)
        {
            std::string prefix = "/lcz-rpc/v1/providers/" + method + "/";
            auto kvs = http_get_prefix(prefix);
            std::vector<::lcz_rpc::HostDetail> details;
            for (const auto &kv : kvs)
            {
                HostInfo h = host_from_key(kv.first);
                if (h.first.empty())
                    continue;
                details.emplace_back(HostDetail(h, load_from_value(kv.second)));
            }
            return details;
        }

        bool EtcdRegistryStore::reportLoad(
            const std::string &method,
            const ::lcz_rpc::HostInfo &host,
            int load)
        {
            std::string key = key_for(method, host);
            // 读当前 value，只更新 load 和 ts
            int64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
            std::string value = R"({"ip":")" + host.first + R"(","port":)" 
            + std::to_string(host.second) + R"(,"load":)" 
            + std::to_string(load) + R"(,"ts":)" + std::to_string(ts) + "}";
            return http_put(key, value);
        }

        bool EtcdRegistryStore::heartbeat(
            const std::string &method,
            const ::lcz_rpc::HostInfo &host)
        {
            std::string key = key_for(method, host);
            int64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
            // 先查 key 是否存在
            auto kvs = http_get_prefix("/lcz-rpc/v1/providers/" + method + "/");
            LCZ_DEBUG("heartbeat kvs.size=%zu method=%s host=%s:%d", kvs.size(), method.c_str(), host.first.c_str(), host.second);
            for (const auto &kv : kvs)
            {
                HostInfo h = host_from_key(kv.first);
                LCZ_DEBUG("heartbeat decoded_key=%s h=%s:%d with host=%s:%d eq=%d", kv.first.c_str(), h.first.c_str(), h.second, host.first.c_str(), host.second, (h == host));
                if (h == host)
                {
                    int load = load_from_value(kv.second);
                    std::string value = R"({"ip":")" + host.first + R"(","port":)" 
                    + std::to_string(host.second) + R"(,"load":)" 
                    + std::to_string(load) + R"(,"ts":)" + std::to_string(ts) + "}";
                    return http_put(key, value);
                }
            }
            return false;
        }

        std::vector<std::pair<std::string, ::lcz_rpc::HostInfo>>
        EtcdRegistryStore::disconnectProvider( const BaseConnection::ptr &conn)
        {
            std::vector<std::pair<std::string, ::lcz_rpc::HostInfo>> out;
            uintptr_t conn_id = reinterpret_cast<uintptr_t>(conn.get());

            std::vector<std::string> keys;
            {
                std::lock_guard<std::mutex> lock(_mutex);
                auto it = _keys_by_conn.find(conn_id);
                if (it == _keys_by_conn.end())
                    return out;
                keys = std::move(it->second);
                _keys_by_conn.erase(it);
            }

            for (const std::string &key : keys)
            {
                HostInfo h = host_from_key(key);
                auto start = key.find("/providers/");
                auto end = key.rfind('/');
                if (start != std::string::npos && end != std::string::npos && !h.first.empty())
                {
                    std::string method = key.substr(start + 11, end - start - 11);
                    out.emplace_back(method, h);
                }
                http_delete(key);
            }
            return out;
        }

        void EtcdRegistryStore::cleanConnKeys(const BaseConnection::ptr &conn)
        {
            uintptr_t conn_id = reinterpret_cast<uintptr_t>(conn.get());
            std::lock_guard<std::mutex> lock(_mutex);
            _keys_by_conn.erase(conn_id);
        }

        std::vector<std::pair<std::string, ::lcz_rpc::HostInfo>>
        EtcdRegistryStore::sweepExpired(std::chrono::seconds idle)
        {
            std::vector<std::pair<std::string, ::lcz_rpc::HostInfo>> expired;
            int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
            // 启动宽限期：idle 秒内不剔除，给提供者时间重连并刷新时间戳
            if (now - _start_time < static_cast<int64_t>(idle.count()) * 1000)
                return expired;
            auto kvs = http_get_prefix("/lcz-rpc/v1/providers/");
            int64_t limit = static_cast<int64_t>(idle.count()) * 1000;

            std::vector<std::string> expired_keys;
            for (const auto &kv : kvs)
            {
                int64_t ts = ts_from_value(kv.second);
                if (now - ts > limit)
                {
                    HostInfo h = host_from_key(kv.first);
                    auto start = kv.first.find("/providers/");
                    auto end = kv.first.rfind('/');

                    if (start != std::string::npos &&
                        end != std::string::npos && !h.first.empty())
                    {
                        std::string method = kv.first.substr(start + 11, end - start - 11);
                        expired.emplace_back(method, h);
                    }
                    expired_keys.push_back(kv.first);
                }
            }

            for (const auto &key : expired_keys)
            {
                // 二次校验：防止 get→delete 之间 provider 心跳刷新了时间戳
                auto latest = http_get_prefix(key);
                bool still_expired = true;
                for (const auto &kv : latest)
                {
                    if (kv.first == key)
                    {
                        int64_t ts = ts_from_value(kv.second);
                        if (now - ts <= limit)
                            still_expired = false;
                        break;
                    }
                }
                if (still_expired)
                    http_delete(key);
            }

            {
                std::lock_guard<std::mutex> lock(_mutex);
                for (const auto &key : expired_keys)
                {
                    for (auto &entry : _keys_by_conn)
                    {
                        auto &vec = entry.second;
                        vec.erase(std::remove(vec.begin(), vec.end(), key), vec.end());
                    }
                }
            }
            return expired;
        }

        // 从 key "....../10.0.0.1:8080" 的末尾解析出 HostInfo
        HostInfo EtcdRegistryStore::host_from_key(const std::string &key)
        {
            auto pos = key.rfind('/');//找最后一个'/'
            if (pos == std::string::npos)
                return {};
            std::string addr = key.substr(pos + 1); // "10.0.0.1:8080"
            auto mid = addr.rfind(':');
            if (mid == std::string::npos)
                return {};
            return {addr.substr(0, mid), std::stoi(addr.substr(mid + 1))};
        }
        // 从 value JSON 解析 load
        int EtcdRegistryStore::load_from_value(const std::string &v)
        {
            Json::Value j;
            Json::CharReaderBuilder b;
            std::string errs;
            std::istringstream iss(v);
            if (!Json::parseFromStream(b, iss, &j, &errs))
                return 0;
            return j.get("load", 0).asInt();
        }
        // 从 value JSON 解析 ts
        int64_t EtcdRegistryStore::ts_from_value(const std::string &v)
        {
            Json::Value j;
            Json::CharReaderBuilder b;
            std::string errs;
            std::istringstream iss(v);
            if (!Json::parseFromStream(b, iss, &j, &errs))
                return 0;
            return j.get("ts", 0).asInt64();
        }
    }
}