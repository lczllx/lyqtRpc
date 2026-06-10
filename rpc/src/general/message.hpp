#pragma once
#include "abstract.hpp"
#include "detail.hpp"
#include "fields.hpp"
#include "publicconfig.hpp"
#include "log_system/lcz_log.h"
#include "rpc_envelope.pb.h"

namespace lcz_rpc
{
    // Json 消息基类：以 Json::Value 存储，提供序列化/反序列化
    class JsonMessage:public BaseMessage
    {
        public:
        using ptr = std::shared_ptr<JsonMessage>;  
        // 将 _data 序列化为 JSON 字符串
        virtual std::string serialize()override
        {
            std::string output;
            bool ret=JSON::serialize(_data,output);
            if(!ret)
            {
                LCZ_ERROR("Serialize failed!");
                return "";
            }
            return output;
        }
        // 将字符串反序列化到 _data
        virtual bool unserialize(const std::string &msg)override
        {
            return JSON::deserialize(msg,_data);
        }
        // JsonMessage 不额外校验，默认合法
        virtual bool check()override
        {
            return true;
        }
        
    protected:
        Json::Value _data;       // 消息数据（改为 protected，让子类可以访问）
    
    };
    // Json 请求消息类：含 method 字段，用于请求类消息基类
    class JsonRequest:public JsonMessage
    {
        public:
        using ptr = std::shared_ptr<JsonRequest>;  
        
        // 获取方法名
        std::string method() const
        {
            return _data[KEY_METHOD].asString();
        }
        // 设置方法名
        void setMethod(const std::string &method)
        {
            _data[KEY_METHOD] = method;
        }
    };
    // Json 响应消息类：含 rcode、result 字段，用于响应类消息基类
    class JsonResponse:public JsonMessage
    {
        public:
        using ptr = std::shared_ptr<JsonResponse>; 
        // 检查响应码等字段是否有效
        virtual bool check()override
        {
           if(!_data.isMember(KEY_RCODE))
           {
                LCZ_ERROR("Response code is not found!");
                return false;
           }
           if(!_data[KEY_RCODE].isIntegral())
           {
                LCZ_ERROR("Response code is not integral!");
                return false;
           }
           return true;
        }
        
        // 获取响应码
        RespCode rcode() const
        {
            return static_cast<RespCode>(_data[KEY_RCODE].asInt());
        }
        // 设置响应码
        void setRcode(RespCode rcode)
        {
            _data[KEY_RCODE] = static_cast<int>(rcode);
        }
        
        // 获取结果
        Json::Value result() const
        {
            return _data[KEY_RESULT];
        }
        // 设置结果
        void setResult(const Json::Value &result)
        {
            _data[KEY_RESULT] = result;
        }
        // 服务端过载时建议客户端等待的毫秒数，0 表示未设置
        int64_t retryAfterMs() const
        {
            return _data.get(KEY_RETRY_AFTER_MS, 0).asInt64();
        }
        void setRetryAfterMs(int64_t ms)
        {
            _data[KEY_RETRY_AFTER_MS] = static_cast<Json::Int64>(ms);
        }
    };
    // RPC 请求消息类：携带 method、params，用于 RPC 调用
    class RpcRequest:public JsonRequest
    {
        public:
        using ptr = std::shared_ptr<RpcRequest>;  
        // 校验 method 和 params 字段
        virtual bool check()override
        {
            //长度 消息类型 id长度 id data
            //     Rpcrequest
            if(_data[KEY_METHOD].isString()==false/*消息类型不为字符串*/||
            _data[KEY_METHOD].isNull()/*消息类型不能为空*/)
            {
               LCZ_ERROR("Method is not string or null!");
                return false;
            }
            if(_data[KEY_PARAMS].isObject()==false/*参数类型错误*/||
            _data[KEY_PARAMS].isNull()/*参数不能为空*/)
            {
                LCZ_ERROR("Params is not object or null!");
                return false;
            }
            return true;
        }
        // 获取 RPC 参数
        Json::Value params()const
        {
            return _data[KEY_PARAMS];//获取参数
        }
        // 设置 RPC 参数
        void setParams(const Json::Value &params)
        {
                _data[KEY_PARAMS] = params;
        }
    };
    // RPC 响应消息类：携带 rcode、result，用于 RPC 调用结果
    class RpcResponse:public JsonResponse
    {
        public:
        using ptr = std::shared_ptr<RpcResponse>; 
        // 校验 rcode 和 result 字段
        virtual bool check()override
        {
            //对于响应消息，响应码不能为空，结果不能为空
            if(_data[KEY_RCODE].isIntegral()==false||
            _data[KEY_RCODE].isNull())
            {
                LCZ_ERROR("Response code is not integral or null!");
                return false;
            }
            if(_data[KEY_RESULT].isNull())
            {
                LCZ_ERROR("Result is null!");
                return false;
            }
            return true;
        }
    };
    // 主题请求消息类：用于主题的创建/删除/订阅/发布等操作
    class TopicRequest:public JsonRequest
    {
        public:
        using ptr = std::shared_ptr<TopicRequest>;
        // 按转发策略校验对应字段：
        // FANOUT→fanoutLimit>0, SOURCE_HASH→shardKey非空, PRIORITY→priority>0||tags非空, REDUNDANT→redundantCount>1
        virtual bool check()override
        {           
            if(_data[KEY_TOPIC_KEY].isString()==false||
            _data[KEY_TOPIC_KEY].isNull())
            {
                LCZ_ERROR("主题键不是字符串或为空!");
                return false;
            }
            if(_data[KEY_OPTYPE].isIntegral()==false||
            _data[KEY_OPTYPE].isNull())
            {
                LCZ_ERROR("参数不是对象或为空!");
                return false;
            }
            if(_data[KEY_OPTYPE].asInt()==static_cast<int>(TopicOpType::PUBLISH)&&(_data[KEY_TOPIC_MSG].isString()==false||
            _data[KEY_TOPIC_MSG].isNull()))
            {
                LCZ_ERROR("主题消息不是字符串或为空!");
                return false;
            }
            switch (forwardStrategy()) {
                case TopicForwardStrategy::FANOUT:
                    if (fanoutLimit() <= 0) 
                    {
                        LCZ_ERROR("扇出数量限制不能小于等于0!");
                        return false;
                    }
                    break;
                case TopicForwardStrategy::SOURCE_HASH:
                    if (shardKey().empty()) 
                    {
                        LCZ_ERROR("源哈希键不能为空!");
                        return false;
                    }
                    break;
                case TopicForwardStrategy::PRIORITY:
                    if (priority() <= 0 && tags().empty()) 
                    {
                        LCZ_ERROR("优先级不能小于等于0且标签不能为空!");
                        return false;
                    }
                    break;
                case TopicForwardStrategy::REDUNDANT:
                    if (redundantCount() <= 1) 
                    {
                        LCZ_ERROR("冗余投递数量不能小于等于1!");
                        return false;
                    }
                    break;
                default:
                    break;
                }
            return true;
        } 
        //获取当前使用的转发策略
        TopicForwardStrategy forwardStrategy()const{return static_cast<TopicForwardStrategy>(_data.get(KEY_TOPIC_FORWARD, static_cast<int>(TopicForwardStrategy::BROADCAST)).asInt());}
        //设置当前使用的转发策略
        void setForwardStrategy(TopicForwardStrategy forwardStrategy){ _data[KEY_TOPIC_FORWARD] = static_cast<int>(forwardStrategy);}
        //获取扇出数量限制
        int fanoutLimit()const
        {
            return _data.get(KEY_TOPIC_FANOUT,0).asInt();//安全获取扇出数量限制
        }
        //设置扇出数量限制
        void setFanoutLimit(int fanoutLimit)
        {
           _data[KEY_TOPIC_FANOUT] = fanoutLimit;
        }
        //获取源哈希键
        const std::string shardKey()const
        {
            return _data.get(KEY_TOPIC_SHARD_KEY,"").asString();//安全获取源哈希键
        }
        //设置源哈希键
        void setShardKey(const std::string &shardKey)
        {
            _data[KEY_TOPIC_SHARD_KEY] = shardKey;
        }
        // 获取优先级
        int priority()const
        {
            return _data.get(KEY_TOPIC_PRIORITY,0).asInt();//安全获取优先级
        }
        // 设置优先级
        void setPriority(int priority)
        {
            _data[KEY_TOPIC_PRIORITY] = priority;
        }
        // 获取标签列表
        // std::string tags()const
        // {
        //     return _data.get(KEY_TOPIC_TAGS,"").asString();//安全获取标签
        // }
        std::vector<std::string> tags() const 
        {
            std::vector<std::string> result;
            const auto &arr = _data.get(KEY_TOPIC_TAGS,Json::arrayValue);
            if (!arr.isArray()) return result;
            result.reserve(arr.size());
            for (const auto &item : arr)
            if (item.isString()) result.push_back(item.asString());
            return result;
        }
        // 设置标签列表
        void setTags(const std::vector<std::string> &tags)
        {
            _data[KEY_TOPIC_TAGS] = Json::Value(Json::arrayValue);
            for(const auto &tag : tags)
            {
                _data[KEY_TOPIC_TAGS].append(tag);
            }
        }
        // 获取冗余投递数量
        int redundantCount()const
        {
            return _data.get(KEY_TOPIC_REDUNDANT,0).asInt();//安全获取冗余投递数量
        }
        // 设置冗余投递数量
        void setRedundantCount(int redundantCount)
        {
            _data[KEY_TOPIC_REDUNDANT] = redundantCount;
        }
        // 获取主题 key
        std::string topicKey()const
        {
            return _data[KEY_TOPIC_KEY].asString();
        }
        // 设置主题 key
        void setTopicKey(const std::string &topicKey)
        {
            _data[KEY_TOPIC_KEY] = topicKey;
        }
        // 获取操作类型
        TopicOpType optype()const
        {
            return static_cast<TopicOpType>(_data[KEY_OPTYPE].asInt());
        }
        // 设置操作类型
        void setOptype(TopicOpType optype)
        {
            _data[KEY_OPTYPE] = static_cast<int>(optype);
        }
        // 获取主题消息内容
        std::string topicMsg()const
        {
            return _data[KEY_TOPIC_MSG].asString();
        }
        // 设置主题消息内容
        void setTopicMsg(const std::string &topicMsg)
        {
            _data[KEY_TOPIC_MSG] = topicMsg;
        }
    };
    // 主题响应消息类：主题操作的结果响应
    class TopicResponse:public JsonResponse
    {
        public: 
        using ptr = std::shared_ptr<TopicResponse>;  
        //这里不重写check()方法，因为主题响应消息的响应码和结果都是可选的
        // rcode, setRcode, result, setResult 继承自 JsonResponse
    };
    // //在lcz_rpc命名空间定义的HostInfoDetail结构体，用于存储主机信息和负载信息
    // struct HostInfoDetail {
    //     std::string ip;
    //     int port;
    //     int load=0; // 新增
    //     HostInfoDetail() = default;
    //     HostInfoDetail(const std::string &ip_, int port_, int load_)
    //         : ip(ip_), port(port_), load(load_) {}
    // };
    // 服务请求消息类：用于服务注册/发现/负载上报/心跳等
    class ServiceRequest:public JsonRequest
    {
        public: 
        using ptr = std::shared_ptr<ServiceRequest>; 
        // 获取负载值
        int load()const{
            return _data.get(KEY_LOAD,0).asInt();//安全获取负载信息
        }
        // 设置负载值
        void setLoad(int load)
        {
            _data[KEY_LOAD] = load;
        }
        virtual bool check()override
        {
           
            //对于服务请求，方法名不能为空，操作类型不能为空，主机信息不能为空
            if(_data[KEY_METHOD].isString()==false||
            _data[KEY_METHOD].isNull())
            {
               LCZ_ERROR("Method is not string or null!");
                return false;
            }
            if(_data[KEY_OPTYPE].isIntegral()==false||
            _data[KEY_OPTYPE].isNull())
            {
               LCZ_ERROR("Op type is not integral or null!");
                return false;
            }
            //不是服务发现的话，就需要提供主机信息
            if(_data[KEY_OPTYPE].asInt()!=static_cast<int>(ServiceOpType::DISCOVER)&&
                (_data[KEY_HOST].isObject()==false||
                _data[KEY_HOST].isNull()||
                _data[KEY_HOST][KEY_HOST_IP].isString()==false||
                _data[KEY_HOST][KEY_HOST_IP].isNull()||
                _data[KEY_HOST][KEY_HOST_PORT].isIntegral()==false||
                _data[KEY_HOST][KEY_HOST_PORT].isNull()))
            {
                LCZ_ERROR("service discover host is not object or null or ip is not string or null or port is not integral or null!");
                return false;
            }
            if(_data[KEY_OPTYPE].asInt()==static_cast<int>(ServiceOpType::LOAD_REPORT)&&
            (_data[KEY_LOAD].isIntegral()==false||
            _data[KEY_LOAD].isNull()))
            {
                LCZ_ERROR("没有上报负载信息!");
                return false;
            }
            
            return true;
        } 
        // 获取操作类型
        ServiceOpType optype()const
        {
            return static_cast<ServiceOpType>(_data[KEY_OPTYPE].asInt());
        }
        // 设置操作类型
        void setOptype(ServiceOpType optype)
        {
            _data[KEY_OPTYPE] = static_cast<int>(optype);
        }
        // 获取主机信息 (ip, port)
        HostInfo host()const
        {
            return std::make_pair(_data[KEY_HOST][KEY_HOST_IP].asString(),_data[KEY_HOST][KEY_HOST_PORT].asInt());
        }
        // 设置主机信息
        void setHost(const HostInfo &host)
        {
            _data[KEY_HOST] = Json::Value(Json::objectValue);
            _data[KEY_HOST][KEY_HOST_IP] = host.first;
            _data[KEY_HOST][KEY_HOST_PORT] = host.second;
        }
    };
    // 服务响应消息类：服务注册/发现/负载上报等的响应
    class ServiceResponse:public JsonResponse
    {
        public: 
        using ptr = std::shared_ptr<ServiceResponse>;  
        // 校验 rcode、optype 等字段
        virtual bool check()override
        {
            if(_data[KEY_RCODE].isIntegral()==false||
            _data[KEY_RCODE].isNull())
            {
                LCZ_ERROR("Response code is not integral or null!");
                return false;
            }
            if(_data[KEY_OPTYPE].isIntegral()==false||
            _data[KEY_OPTYPE].isNull())
            {
                LCZ_ERROR("Op type is not integral or null!");
                return false;
            }
            if(_data[KEY_OPTYPE].asInt()==static_cast<int>(ServiceOpType::DISCOVER)&&
            (   _data[KEY_METHOD].isString()==false||
                _data[KEY_METHOD].isNull()||
                _data[KEY_HOST].isArray()==false||
                _data[KEY_HOST].isNull()))
            {
               LCZ_ERROR("service discover method is not string or null or host is not array or null!");
                return false;
            }
            return true;
        }
        // rcode, setRcode, result, setResult 继承自 JsonResponse
        // 但 ServiceResponse 还需要 method 和 optype
        
        // 获取方法名
        std::string method() const
        {
            return _data[KEY_METHOD].asString();
        }
        // 设置方法名
        void setMethod(const std::string &method)
        {
            _data[KEY_METHOD] = method;
        }
        // 设置操作类型
        void setOptype(ServiceOpType optype)
        {
            _data[KEY_OPTYPE] = static_cast<int>(optype);
        }
        // 获取操作类型
        ServiceOpType optype()const
        {
            return static_cast<ServiceOpType>(_data[KEY_OPTYPE].asInt());
        }
        // 获取主机列表（从 JSON 数组解析）
        std::vector<HostInfo> hosts() const
        {
            std::vector<HostInfo> addresses;
            
            for(int i = 0; i < _data[KEY_HOST].size(); i++)
            {
                addresses.emplace_back(
                    _data[KEY_HOST][i][KEY_HOST_IP].asString(),
                    _data[KEY_HOST][i][KEY_HOST_PORT].asInt()
                );
            }
            return addresses;
        }
         // 设置主机列表（保存为 JSON 数组）
         void setHost(const std::vector<HostInfo> &addresses)
         {
             _data[KEY_HOST] = Json::Value(Json::arrayValue);
             
             for(const auto &address : addresses)
             {
                 Json::Value hostObj(Json::objectValue);
                 hostObj[KEY_HOST_IP] = address.first;
                 hostObj[KEY_HOST_PORT] = address.second;
                 _data[KEY_HOST].append(hostObj);
             }
         }
         // 获取主机详情列表（含负载）
        std::vector<HostDetail> hostsDetail() const
        {
            std::vector<HostDetail> hostsdetails;
            
            for(int i = 0; i < _data[KEY_HOST].size(); i++)
            {
                HostInfo host(_data[KEY_HOST][i][KEY_HOST_IP].asString(),
                             _data[KEY_HOST][i][KEY_HOST_PORT].asInt());
                int load = _data[KEY_HOST][i].get(KEY_LOAD,0).asInt();
                hostsdetails.emplace_back(host, load);
            }
            return hostsdetails;
        }
        // 设置主机详情列表（含负载）
        void setHostDetails(const std::vector<HostDetail> &addresses) {
            _data[KEY_HOST] = Json::Value(Json::arrayValue);
            for (const auto &detail : addresses) {
                Json::Value hostObj(Json::objectValue);
                hostObj[KEY_HOST_IP] = detail.host.first;
                hostObj[KEY_HOST_PORT] = detail.host.second;
                hostObj[KEY_LOAD] = detail.load;
                _data[KEY_HOST].append(hostObj);
            }
        }

       

    };

    // 纯 Proto RPC 请求：线缆 body = RpcRequestEnvelope（method + bytes body），路径二
    class ProtoRpcRequest : public BaseMessage
    {
    public:
        using ptr = std::shared_ptr<ProtoRpcRequest>;
        virtual std::string serialize() override
        {
            if (!_envelope.SerializeToString(&_serialized)) {
                LCZ_ERROR("ProtoRpcRequest::serialize failed");
                return "";
            }
            return _serialized;
        }
        virtual bool unserialize(const std::string& msg) override
        {
            if (!_envelope.ParseFromString(msg)) {
                LCZ_ERROR("ProtoRpcRequest::unserialize failed");
                return false;
            }
            return true;
        }
        virtual bool check() override
        {
            if (_envelope.method().empty()) {
                LCZ_ERROR("ProtoRpcRequest: method empty");
                return false;
            }
            return true;
        }
        std::string method() const { return _envelope.method(); }
        void setMethod(const std::string& m) { _envelope.set_method(m); }
        std::string body() const { return _envelope.body(); }
        void setBody(const std::string& b) { _envelope.set_body(b); }
    private:
        lcz_rpc::proto::RpcRequestEnvelope _envelope; // protobuf 请求包体，包含 method + body
        std::string _serialized;                       // 预序列化缓存，避免重复序列化
    };

    // 纯 Proto RPC 响应：线缆 body = RpcResponseEnvelope（rcode + bytes body），路径二
    class ProtoRpcResponse : public BaseMessage
    {
    public:
        using ptr = std::shared_ptr<ProtoRpcResponse>;
        virtual std::string serialize() override
        {
            if (!_envelope.SerializeToString(&_serialized)) {
                LCZ_ERROR("ProtoRpcResponse::serialize failed");
                return "";
            }
            return _serialized;
        }
        virtual bool unserialize(const std::string& msg) override
        {
            if (!_envelope.ParseFromString(msg)) {
                LCZ_ERROR("ProtoRpcResponse::unserialize failed");
                return false;
            }
            return true;
        }
        virtual bool check() override { return true; }
        RespCode rcode() const { return static_cast<RespCode>(_envelope.rcode()); }
        void setRcode(RespCode c) { _envelope.set_rcode(static_cast<int32_t>(c)); }
        std::string body() const { return _envelope.body(); }
        void setBody(const std::string& b) { _envelope.set_body(b); }
        // 服务端过载时建议退避毫秒数，0 表示未设置（非序列化，仅内存传递）
        int64_t retryAfterMs() const { return _retry_after_ms; }
        void setRetryAfterMs(int64_t ms) { _retry_after_ms = ms; }
    private:
        lcz_rpc::proto::RpcResponseEnvelope _envelope; // protobuf 响应包体，包含 rcode + body
        std::string _serialized;                        // 预序列化缓存，避免重复序列化
        int64_t _retry_after_ms = 0;                    // 服务端过载时建议退避毫秒数，0=未设置（非序列化）
    };

    // 纯 Proto 主题请求：与 TopicRequest 字段一致
    class ProtoTopicRequest : public BaseMessage
    {
    public:
        using ptr = std::shared_ptr<ProtoTopicRequest>;
        virtual std::string serialize() override
        {
            if (!_envelope.SerializeToString(&_serialized)) {
                LCZ_ERROR("ProtoTopicRequest::serialize failed");
                return "";
            }
            return _serialized;
        }
        virtual bool unserialize(const std::string& msg) override
        {
            if (!_envelope.ParseFromString(msg)) {
                LCZ_ERROR("ProtoTopicRequest::unserialize failed");
                return false;
            }
            return true;
        }
        virtual bool check() override
        {
            if (_envelope.topic_key().empty()) {
                LCZ_ERROR("主题键不是字符串或为空!");
                return false;
            }
            if (_envelope.optype() < 0) {
                LCZ_ERROR("参数不是对象或为空!");
                return false;
            }
            if (static_cast<TopicOpType>(_envelope.optype()) == TopicOpType::PUBLISH && _envelope.topic_msg().empty()) {
                LCZ_ERROR("主题消息不是字符串或为空!");
                return false;
            }
            switch (forwardStrategy()) {
                case TopicForwardStrategy::FANOUT:
                    if (fanoutLimit() <= 0) { LCZ_ERROR("扇出数量限制不能小于等于0!"); return false; }
                    break;
                case TopicForwardStrategy::SOURCE_HASH:
                    if (shardKey().empty()) { LCZ_ERROR("源哈希键不能为空!"); return false; }
                    break;
                case TopicForwardStrategy::PRIORITY:
                    if (priority() <= 0 && tags().empty()) { LCZ_ERROR("优先级不能小于等于0且标签不能为空!"); return false; }
                    break;
                case TopicForwardStrategy::REDUNDANT:
                    if (redundantCount() <= 1) { LCZ_ERROR("冗余投递数量不能小于等于1!"); return false; }
                    break;
                default:
                    break;
            }
            return true;
        }
        std::string method() const { return _envelope.method(); }
        void setMethod(const std::string& m) { _envelope.set_method(m); }
        std::string topicKey() const { return _envelope.topic_key(); }
        void setTopicKey(const std::string& k) { _envelope.set_topic_key(k); }
        TopicOpType optype() const { return static_cast<TopicOpType>(_envelope.optype()); }
        void setOptype(TopicOpType o) { _envelope.set_optype(static_cast<int32_t>(o)); }
        std::string topicMsg() const { return _envelope.topic_msg(); }
        void setTopicMsg(const std::string& m) { _envelope.set_topic_msg(m); }
        TopicForwardStrategy forwardStrategy() const { return static_cast<TopicForwardStrategy>(_envelope.forward_strategy()); }
        void setForwardStrategy(TopicForwardStrategy s) { _envelope.set_forward_strategy(static_cast<int32_t>(s)); }
        int fanoutLimit() const { return _envelope.fanout(); }
        void setFanoutLimit(int v) { _envelope.set_fanout(v); }
        std::string shardKey() const { return _envelope.shard_key(); }
        void setShardKey(const std::string& k) { _envelope.set_shard_key(k); }
        int priority() const { return _envelope.priority(); }
        void setPriority(int v) { _envelope.set_priority(v); }
        std::vector<std::string> tags() const
        {
            std::vector<std::string> r;
            for (int i = 0; i < _envelope.tags_size(); ++i) r.push_back(_envelope.tags(i));
            return r;
        }
        void setTags(const std::vector<std::string>& v)
        {
            _envelope.clear_tags();
            for (const auto& t : v) _envelope.add_tags(t);
        }
        int redundantCount() const { return _envelope.redundant(); }
        void setRedundantCount(int v) { _envelope.set_redundant(v); }
    private:
        lcz_rpc::proto::TopicRequestEnvelope _envelope; // protobuf 主题请求包体
        std::string _serialized;                         // 预序列化缓存，避免重复序列化
    };

    // 纯 Proto 主题响应
    class ProtoTopicResponse : public BaseMessage
    {
    public:
        using ptr = std::shared_ptr<ProtoTopicResponse>;
        virtual std::string serialize() override
        {
            if (!_envelope.SerializeToString(&_serialized)) {
                LCZ_ERROR("ProtoTopicResponse::serialize failed");
                return "";
            }
            return _serialized;
        }
        virtual bool unserialize(const std::string& msg) override
        {
            if (!_envelope.ParseFromString(msg)) {
                LCZ_ERROR("ProtoTopicResponse::unserialize failed");
                return false;
            }
            return true;
        }
        virtual bool check() override { return true; }
        RespCode rcode() const { return static_cast<RespCode>(_envelope.rcode()); }
        void setRcode(RespCode c) { _envelope.set_rcode(static_cast<int32_t>(c)); }
        std::string result() const { return _envelope.result(); }
        void setResult(const std::string& r) { _envelope.set_result(r); }
    private:
        lcz_rpc::proto::TopicResponseEnvelope _envelope; // protobuf 主题响应包体，包含 rcode + result
        std::string _serialized;                           // 预序列化缓存，避免重复序列化
    };

    // 纯 Proto 服务请求：与 ServiceRequest 字段一致
    class ProtoServiceRequest : public BaseMessage
    {
    public:
        using ptr = std::shared_ptr<ProtoServiceRequest>;
        virtual std::string serialize() override
        {
            if (!_envelope.SerializeToString(&_serialized)) {
                LCZ_ERROR("ProtoServiceRequest::serialize failed");
                return "";
            }
            return _serialized;
        }
        virtual bool unserialize(const std::string& msg) override
        {
            if (!_envelope.ParseFromString(msg)) {
                LCZ_ERROR("ProtoServiceRequest::unserialize failed");
                return false;
            }
            return true;
        }
        virtual bool check() override
        {
            if (_envelope.method().empty()) {
                LCZ_ERROR("Method is not string or null!");
                return false;
            }
            if (_envelope.optype() < 0) {
                LCZ_ERROR("Op type is not integral or null!");
                return false;
            }
            if (static_cast<ServiceOpType>(_envelope.optype()) != ServiceOpType::DISCOVER) {
                if (_envelope.host_ip().empty() || _envelope.host_port() <= 0) {
                    LCZ_ERROR("service discover host is not object or null or ip/port invalid!");
                    return false;
                }
            }
            return true;
        }
        std::string method() const { return _envelope.method(); }
        void setMethod(const std::string& m) { _envelope.set_method(m); }
        ServiceOpType optype() const { return static_cast<ServiceOpType>(_envelope.optype()); }
        void setOptype(ServiceOpType o) { _envelope.set_optype(static_cast<int32_t>(o)); }
        HostInfo host() const { return std::make_pair(_envelope.host_ip(), _envelope.host_port()); }
        void setHost(const HostInfo& h)
        {
            _envelope.set_host_ip(h.first);
            _envelope.set_host_port(h.second);
        }
        int load() const { return _envelope.load(); }
        void setLoad(int v) { _envelope.set_load(v); }
    private:
        lcz_rpc::proto::ServiceRequestEnvelope _envelope; // protobuf 服务请求包体
        std::string _serialized;                           // 预序列化缓存，避免重复序列化
    };

    // 纯 Proto 服务响应：与 ServiceResponse 字段一致
    class ProtoServiceResponse : public BaseMessage
    {
    public:
        using ptr = std::shared_ptr<ProtoServiceResponse>;
        virtual std::string serialize() override
        {
            if (!_envelope.SerializeToString(&_serialized)) {
                LCZ_ERROR("ProtoServiceResponse::serialize failed");
                return "";
            }
            return _serialized;
        }
        virtual bool unserialize(const std::string& msg) override
        {
            if (!_envelope.ParseFromString(msg)) {
                LCZ_ERROR("ProtoServiceResponse::unserialize failed");
                return false;
            }
            return true;
        }
        virtual bool check() override
        {
            if (static_cast<ServiceOpType>(_envelope.optype()) == ServiceOpType::DISCOVER) {
                if (_envelope.method().empty() || _envelope.host_size() == 0) {
                    LCZ_ERROR("service discover method/host invalid!");
                    return false;
                }
            }
            return true;
        }
        RespCode rcode() const { return static_cast<RespCode>(_envelope.rcode()); }
        void setRcode(RespCode c) { _envelope.set_rcode(static_cast<int32_t>(c)); }
        std::string method() const { return _envelope.method(); }
        void setMethod(const std::string& m) { _envelope.set_method(m); }
        ServiceOpType optype() const { return static_cast<ServiceOpType>(_envelope.optype()); }
        void setOptype(ServiceOpType o) { _envelope.set_optype(static_cast<int32_t>(o)); }
        std::vector<HostInfo> hosts() const
        {
            std::vector<HostInfo> r;
            for (int i = 0; i < _envelope.host_size(); ++i) {
                const auto& e = _envelope.host(i);
                r.emplace_back(e.ip(), e.port());
            }
            return r;
        }
        void setHost(const std::vector<HostInfo>& addresses)
        {
            _envelope.clear_host();
            for (const auto& a : addresses) {
                auto* e = _envelope.add_host();
                e->set_ip(a.first);
                e->set_port(a.second);
            }
        }
        std::vector<HostDetail> hostsDetail() const
        {
            std::vector<HostDetail> r;
            for (int i = 0; i < _envelope.host_size(); ++i) {
                const auto& e = _envelope.host(i);
                r.emplace_back(std::make_pair(e.ip(), e.port()), e.load());
            }
            return r;
        }
        void setHostDetails(const std::vector<HostDetail>& addresses)
        {
            _envelope.clear_host();
            for (const auto& d : addresses) {
                auto* e = _envelope.add_host();
                e->set_ip(d.host.first);
                e->set_port(d.host.second);
                e->set_load(d.load);
            }
        }
    private:
        lcz_rpc::proto::ServiceResponseEnvelope _envelope; // protobuf 服务响应包体，包含 rcode + hosts + loads
        std::string _serialized;                            // 预序列化缓存，避免重复序列化
    };

    // 消息工厂类：根据 MsgType 或模板类型创建具体消息对象
    class MessageFactory
    {
        public:
        static BaseMessage::ptr create(MsgType msgtype)
        {
            BaseMessage::ptr msg;
            switch(msgtype)
            {
                case MsgType::REQ_RPC:
                    msg = std::make_shared<RpcRequest>();
                    break;
                case MsgType::RSP_RPC:
                    msg = std::make_shared<RpcResponse>();
                    break;
                case MsgType::REQ_TOPIC:
                    msg = std::make_shared<TopicRequest>();
                    break;
                case MsgType::RSP_TOPIC:
                    msg = std::make_shared<TopicResponse>();
                    break;
                case MsgType::REQ_SERVICE:
                    msg = std::make_shared<ServiceRequest>();
                    break;
                case MsgType::RSP_SERVICE:
                    msg = std::make_shared<ServiceResponse>();
                    break;
                case MsgType::REQ_RPC_PROTO:
                    msg = std::make_shared<ProtoRpcRequest>();
                    break;
                case MsgType::RSP_RPC_PROTO:
                    msg = std::make_shared<ProtoRpcResponse>();
                    break;
                case MsgType::REQ_TOPIC_PROTO:
                    msg = std::make_shared<ProtoTopicRequest>();
                    break;
                case MsgType::RSP_TOPIC_PROTO:
                    msg = std::make_shared<ProtoTopicResponse>();
                    break;
                case MsgType::REQ_SERVICE_PROTO:
                    msg = std::make_shared<ProtoServiceRequest>();
                    break;
                case MsgType::RSP_SERVICE_PROTO:
                    msg = std::make_shared<ProtoServiceResponse>();
                    break;
                default:
                    LCZ_ERROR("Invalid message type!");
                    return nullptr;
            }
            // 自动设置消息类型，确保 msgType 正确
            if (msg) {
                msg->setMsgType(msgtype);
            }
            return msg;
        }
        // 按具体类型和参数创建消息（模板版本）
        template<typename TYPE,typename... ARGS>
        static typename TYPE::ptr create(ARGS&&... args)
        {
            return std::make_shared<TYPE>(std::forward<ARGS>(args)...);
        }

    };
}
