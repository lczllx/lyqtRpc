#pragma once
#include <unordered_map>
#include <string>

#include "publicconfig.hpp"

namespace lcz_rpc
{
// 请求字段宏定义：Json payload 使用的 key
#define KEY_METHOD "method"
#define KEY_PARAMS "parameters"
#define KEY_TOPIC_KEY "topic_key"
#define KEY_TOPIC_MSG "topic_msg"
#define KEY_OPTYPE "optype"
#define KEY_HOST "host"
#define KEY_HOST_IP "ip"
#define KEY_HOST_PORT "port"
#define KEY_RCODE "rcode"
#define KEY_RESULT "result"
#define KEY_RETRY_AFTER_MS "retry_after_ms"
#define KEY_TRACE_ID "trace_id" // 分布式追踪 ID，全链路透传
#define KEY_SPAN_ID "span_id"   // 当前调用跨度 ID
#define KEY_LOAD "load" // 携带负载信息

// Topic 消息需要的扩展字段
#define KEY_TOPIC_FORWARD "forward_strategy" // 当前使用的转发策略
#define KEY_TOPIC_PRIORITY "priority"        // 消息或订阅者的优先级
#define KEY_TOPIC_TAGS "tags"                // 标签过滤用
#define KEY_TOPIC_FANOUT "fanout"            // 扇出数量限制
#define KEY_TOPIC_SHARD_KEY "shard_key"      // 源哈希键
#define KEY_TOPIC_REDUNDANT "redundant"      // 冗余投递数量

    // 消息类型定义
    enum class MsgType
    {
        REQ_RPC = 0,       // RPC请求消息（JSON）
        RSP_RPC,           // RPC响应消息（JSON）
        REQ_TOPIC,         // 主题操作请求
        RSP_TOPIC,         // 主题操作响应
        REQ_SERVICE,       // 服务操作请求
        RSP_SERVICE,       // 服务操作响应
        REQ_RPC_PROTO,     // RPC 请求（纯 Proto）
        RSP_RPC_PROTO,     // RPC 响应（纯 Proto）
        REQ_TOPIC_PROTO,   // 主题请求（纯 Proto）
        RSP_TOPIC_PROTO,   // 主题响应（纯 Proto）
        REQ_SERVICE_PROTO, // 服务请求（纯 Proto）
        RSP_SERVICE_PROTO, // 服务响应（纯 Proto）
        REQ_RPC_FLAT,      // RPC 请求（FlatBuffers，SHM 零拷贝）
        RSP_RPC_FLAT,      // RPC 响应（FlatBuffers，SHM 零拷贝）
    };

    // 响应码类型定义
    enum class RespCode
    {
        SUCCESS = 0,       // 成功处理
        PARSE_FAILED,      // 消息解析失败
        INVALID_MSGTYPE,   // 消息类型错误
        INVALID_MSG,       // 无效消息
        CONNECTION_CLOSED, // 连接已断开
        INVALID_PARAMS,    // 无效的RPC参数
        SERVICE_NOT_FOUND, // 没有找到对应的服务
        INVALID_OPTYPE,    // 无效的操作类型
        TOPIC_NOT_FOUND,   // 没有找到对应的主题
        INTERNAL_ERROR,    // 内部错误
        TIMEOUT,           // 请求超时
        BACKOFF            // 服务端过载，通知客户端退避重试
    };
    // 根据响应码返回对应的错误描述字符串
    static std::string errReason(RespCode code)
    {
        static std::unordered_map<RespCode, std::string> err_map = {
            {RespCode::SUCCESS, "成功处理!"},
            {RespCode::PARSE_FAILED, "消息解析失败!"},
            {RespCode::INVALID_MSGTYPE, "消息类型错误!"},
            {RespCode::INVALID_MSG, "无效消息"},
            {RespCode::CONNECTION_CLOSED, "连接已断开!"},
            {RespCode::INVALID_PARAMS, "无效的Rpc参数!"},
            {RespCode::SERVICE_NOT_FOUND, "没有找到对应的服务!"},
            {RespCode::INVALID_OPTYPE, "无效的操作类型"},
            {RespCode::TOPIC_NOT_FOUND, "没有找到对应的主题!"},
            {RespCode::INTERNAL_ERROR, "内部错误!"},
            {RespCode::TIMEOUT, "请求超时!"},
            {RespCode::BACKOFF, "服务端过载，请退避重试!"}};
        auto it = err_map.find(code);
        if (it == err_map.end())
        {
            return "未知错误！";
        }
        return it->second;
    }

    // RPC请求类型定义
    enum class ReqType
    {
        ASYNC = 0, // 异步请求
        CALLBACK   // 回调请求
    };

    // 主题操作类型定义
    enum class TopicOpType
    {
        CREATE = 0,  // 创建主题
        REMOVE,      // 删除主题
        SUBSCRIBE,   // 订阅主题
        UNSUBSCRIBE, // 取消订阅
        PUBLISH      // 发布消息
    };

    enum class TopicForwardStrategy
    {
        BROADCAST = 0, // 默认广播：对所有订阅者推送
        ROUND_ROBIN,   // 轮询：每次只发给一个订阅者
        FANOUT,        // 扇出：限制本次投递数量
        SOURCE_HASH,   // 源哈希：同 key 命中同一订阅者
        PRIORITY,      // 优先级/标签过滤 + 定向推送
        REDUNDANT      // 冗余投递：同一消息发送给多个订阅者
    }; // 主题消息转发策略

    // 服务操作类型定义
    enum class ServiceOpType
    {
        REGISTER = 0,       // 服务注册
        DISCOVER,           // 服务发现
        ONLINE,             // 服务上线
        OFFLINE,            // 服务下线
        LOAD_REPORT,        // 服务负载上报
        HEARTBEAT_PROVIDER, // 提供者心跳：证明“我能服务”
        UNKNOWN             // 未知操作
    };

    // 负载均衡类型定义
    enum class LoadBalanceStrategy
    {
        ROUND_ROBIN, // 轮询
        RANDOM,      // 随机
        SOURCE_HASH, // 源地址hash
        LOWEST_LOAD  // 最低负载
    }; // 负载均衡类型

    enum class SerializationMethod
    {
        JSON,     // json
        PROTOBUF, // protobuf
    }; // 序列化方法 -目前是json 扩展完添加protobuf
}