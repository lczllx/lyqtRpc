#include "etcd_leader_election.hpp"
#include "etcd_registry_store.hpp"
#include "../general/log_system/lcz_log.h"
#include <muduo/net/EventLoop.h>
#include <jsoncpp/json/json.h>
#include <unistd.h>

// 一次 libcurl 写回调，将收到的数据追加到 userdata（std::string*）
static size_t curl_write_cb(void *ptr, size_t size, size_t nmemb, void *userdata)
{
    auto *s = static_cast<std::string *>(userdata);
    s->append(static_cast<char *>(ptr), size * nmemb);
    return size * nmemb;
}

namespace lcz_rpc
{
    namespace server
    {

        EtcdLeaderElector::EtcdLeaderElector(const std::string &etcd_endpoints)
            : _etcd_pos(etcd_endpoints), _curl(curl_easy_init()) // 创建独立 CURL 句柄
              ,
              _headers(nullptr), _loop(nullptr), _is_leader(false), _lease_id(-1) // 初始无 lease
        {
            _headers = curl_slist_append(_headers, "Content-Type: application/json");
            curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, _headers);

            char host[256] = {};
            gethostname(host, sizeof(host));
            _instance_id = std::string(host) + ":" + std::to_string(getpid());
        }

        EtcdLeaderElector::~EtcdLeaderElector()
        {
            stop();
            curl_slist_free_all(_headers);
            curl_easy_cleanup(_curl);
        }

        bool EtcdLeaderElector::isLeader() const { return _is_leader; }

        void EtcdLeaderElector::start(muduo::net::EventLoop *loop, LeadershipCallback cb)
        {
            _loop = loop;
            _callback = std::move(cb);
            _timer_id = _loop->runEvery(TICK_INTERVAL_SEC, [this]()
                                        { electionTick(); });
            LCZ_INFO("[EtcdLeaderElector] 选举已启动 instance=%s", _instance_id.c_str());
        }

        void EtcdLeaderElector::stop()
        {
            if (_lease_id >= 0)
            {
                std::string body = R"({"ID":")" + std::to_string(_lease_id) + R"("})";
                curl_post("/v3/kv/lease/revoke", body);
                _lease_id = -1;
            }
            _is_leader = false;
        }

        // 每 TICK_INTERVAL_SEC（1s）由 muduo 定时器触发
        void EtcdLeaderElector::electionTick()
        {
            if (_is_leader)
                refreshLease(); // leader 每次续约 lease
            else
                tryBecomeLeader(); // follower 每次尝试 CAS 竞选
        }

        // 两步完成竞选：
        //   1. POST /v3/lease/grant 获取一个 5s TTL 的 lease_id
        //   2. POST /v3/kv/txn CAS 事务：compare version==0 → request_put 附带 lease_id
        //   若 CAS 成功 → _is_leader=true，开始续约循环
        //   若 CAS 失败（已有其他实例持有）→ 撤销 lease，静默保持 follower
        void EtcdLeaderElector::tryBecomeLeader()
        {
            // 步骤 1：向 etcd 申请一个 5s TTL 的 lease
            std::string grant_body = R"({"TTL":)" + std::to_string(LEASE_TTL_SEC) + "}";
            std::string grant_resp = curl_post("/v3/lease/grant", grant_body);
            if (grant_resp.empty())
                return; // etcd 不可达，下个 tick 重试

            Json::Value root;
            Json::Reader r;
            if (!r.parse(grant_resp, root))
                return;
            int64_t lease_id = std::stoll(root.get("ID", "").asString());

            // 步骤 2：CAS 原子创建 /lcz-rpc/v1/leader
            // etce REST API 要求 key/value 使用 base64 编码
            std::string key = "/lcz-rpc/v1/leader";
            std::string b64_key = EtcdRegistryStore::base64_encode(key);
            std::string b64_val = EtcdRegistryStore::base64_encode(_instance_id);

            // CAS 事务 JSON：
            //   compare: version == 0（key 不存在才可创建）
            //   success: put key=value 并绑定 lease（leader 崩溃 lease 过期自动删除）
            //   failure: 空操作（key 已存在说明已有 leader）
            std::string txn_body =
                R"({"compare":[{"key":")" + b64_key +
                R"(","result":"EQUAL","target":"VERSION","version":"0"}],)"
                R"("success":[{"request_put":{"key":")" +
                b64_key +
                R"(","value":")" + b64_val +
                R"(","lease":)" + std::to_string(lease_id) + "}}],"
                                                             R"("failure":[]})";

            std::string txn_resp = curl_post("/v3/kv/txn", txn_body);
            if (txn_resp.empty())
            {
                // 网络失败 → 撤销 lease，下个 tick 重试
                std::string rev_body = R"({"ID":")" + std::to_string(lease_id) + R"("})";
                curl_post("/v3/kv/lease/revoke", rev_body);
                return;
            }

            // CAS 返回 {"succeeded": true/false}
            if (!r.parse(txn_resp, root) || !root.get("succeeded", false).asBool())
            {
                // 竞选失败（已有 leader）→ 撤销刚申请的 lease，保持 follower
                std::string rev_body = R"({"ID":")" + std::to_string(lease_id) + R"("})";
                curl_post("/v3/kv/lease/revoke", rev_body);
                return;
            }

            // 竞选成功 → 记录 lease_id，开始续约
            _is_leader = true;
            _lease_id = lease_id;
            LCZ_INFO("[EtcdLeaderElector] 成为 leader instance=%s lease=%ld", _instance_id.c_str(), lease_id);
            if (_callback)
                _callback(true);
        }

        // Leader 每次 tick 调用：向 etcd 发送 keepalive 续约一次
        // 续约失败（网络不通 / lease 过期）→ 退为 follower
        void EtcdLeaderElector::refreshLease()
        {
            if (_lease_id < 0)
            {
                becomeFollower();
                return;
            }

            std::string body = R"({"ID":")" + std::to_string(_lease_id) + R"("})";
            std::string resp = curl_post("/v3/lease/keepalive", body);
            if (resp.empty())
            {
                becomeFollower();
                return;
            }

            Json::Value root;
            Json::Reader r;
            // keepalive 成功返回含 TTL 的 JSON，TTL 字段存在即表示续约成功
            if (!r.parse(resp, root) || root["result"].get("TTL", "").asString().empty())
                becomeFollower();
        }

        // 主动或被动退为 follower：
        //   1. 撤销 etcd lease → leader key 立即失效（不需要等 5s TTL）
        //   2. 触发 LeadershipCallback(false) → 上层停止 sweep
        void EtcdLeaderElector::becomeFollower()
        {
            if (_lease_id >= 0)
            {
                std::string body = R"({"ID":")" + std::to_string(_lease_id) + R"("})";
                curl_post("/v3/kv/lease/revoke", body); // 主动撤销 lease
                _lease_id = -1;
            }
            if (_is_leader)
            {
                _is_leader = false;
                LCZ_INFO("[EtcdLeaderElector] 退为 follower instance=%s", _instance_id.c_str());
                if (_callback)
                    _callback(false); // 通知 RegistryServer 停止 sweep
            }
        }

        std::string EtcdLeaderElector::curl_post(const std::string &path, const std::string &json_body)
        {
            std::string url = _etcd_pos + path;
            std::string resp;
            std::lock_guard<std::mutex> lock(_curl_mutex); // libcurl 要求同一句柄禁止并发调用
            curl_easy_setopt(_curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(_curl, CURLOPT_POSTFIELDS, json_body.data());
            curl_easy_setopt(_curl, CURLOPT_POSTFIELDSIZE, json_body.size());
            curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
            curl_easy_setopt(_curl, CURLOPT_WRITEDATA, &resp);
            curl_easy_setopt(_curl, CURLOPT_TIMEOUT_MS, 3000L); // 3s 超时，防止选举/续约阻塞定时器
            CURLcode rc = curl_easy_perform(_curl);
            if (rc != CURLE_OK)
                LCZ_DEBUG("[EtcdLeaderElector] curl fail rc=%d url=%s", rc, url.c_str());
            return (rc == CURLE_OK) ? resp : "";
        }

    } // namespace server
} // namespace lcz_rpc
