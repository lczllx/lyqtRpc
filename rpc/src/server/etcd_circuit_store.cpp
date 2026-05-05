#include <string>
#include <memory>
#include "etcd_circuit_store.hpp"
#include "../general/log_system/lcz_log.h"
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
        // 状态变更时保存
        bool EtcdCircuitStore::save(const std::string &method,
                                    const std::string &host, const CircuitStatus &status)
        {
            // 用json拼接
            Json::Value v;
            v["state"] = static_cast<int>(status.state); // uint8_t → int
            v["failures"] = status.failures;
            v["half_open"] = status.half_open;
            v["opened_at"] = static_cast<Json::Int64>(status.opened_at);

            std::string json = Json::FastWriter().write(v); // {"state":1,"failures":5,...}
            return http_put(key_for(method, host), json);
        }

        // 启动时读加载
        CircuitStatus EtcdCircuitStore::load(const std::string &method,
                                             const std::string &host)
        {
            auto kvs = http_get_prefix(key_for(method, host));
            if (kvs.empty())
                return CircuitStatus{};
            //将字符串解析成json
            Json::Value v;
            Json::CharReaderBuilder b;
            std::string errs;
            std::istringstream iss(kvs[0].second);
            Json::parseFromStream(b, iss, &v, &errs);
            // 解析出状态返回
            CircuitStatus status;
            status.state = static_cast<CircuitState>(v["state"].asInt());
            status.failures = v["failures"].asInt();
            status.half_open = v["half_open"].asInt();
            status.opened_at = v["opened_at"].asInt64();
            return status;
        }

        // provider下线清理
        bool EtcdCircuitStore::remove(const std::string &method,
                                      const std::string &host)
        {
            return http_delete(key_for(method, host));
        }

        EtcdCircuitStore::EtcdCircuitStore(const std::string &endpoints) : _etcd_pos(endpoints), _curl(curl_easy_init())
        {
            _headers = curl_slist_append(_headers, "Content-Type: application/json");
            curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, _headers);
        }
        EtcdCircuitStore::~EtcdCircuitStore()
        {
            curl_slist_free_all(_headers);
            curl_easy_cleanup(_curl);
        }

        // key 构造：/lcz-rpc/v1/circuit-breakers/<method>/<host>
        std::string EtcdCircuitStore::key_for(const std::string &method, const std::string &host)
        {
            return "/lcz-rpc/v1/circuit-breakers/" + method + "/" + host;
        }
        // base64 编解码（etcd REST API 要求 key/value 用 base64 传）
        std::string EtcdCircuitStore::base64_encode(const std::string &s)
        {
            using namespace boost::archive::iterators;
            using It = base64_from_binary<transform_width<std::string::const_iterator, 6, 8>>;
            std::string tmp(It(s.begin()), It(s.end()));
            while (tmp.size() % 4)
                tmp += '=';
            return tmp;
        }
        std::string EtcdCircuitStore::base64_decode(const std::string &s)
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
        bool EtcdCircuitStore::http_put(const std::string &key, const std::string &value)
        {
            //{"key":key,"value":value}
            std::string body = R"({"key":")" + base64_encode(key) +
                               R"(","value":")" + base64_encode(value) + R"("})";
            std::string resp = curl_post("/v3/kv/put", body);
            if (resp.empty())
                LCZ_DEBUG("http_put fail key=%s", key.c_str());
            return !resp.empty();
        }
        // 调用 /v3/kv/range，返回解码后的 key/value 列表
        std::vector<std::pair<std::string, std::string>>
        EtcdCircuitStore::http_get_prefix(const std::string &prefix)
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
        bool EtcdCircuitStore::http_delete(const std::string &key)
        {
            std::string body = R"({"key":")" + base64_encode(key) + R"("})";
            std::string resp = curl_post("/v3/kv/deleterange", body);
            return resp.size();
        }

        // curl 底层调用：发 POST，返回响应体
        std::string EtcdCircuitStore::curl_post(const std::string &path,
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
            if (rc != CURLE_OK)
                LCZ_DEBUG("curl fail rc=%d %s url=%s", rc, curl_easy_strerror(rc), url.c_str());
            return (rc == CURLE_OK) ? resp : "";
        }

    }

}
