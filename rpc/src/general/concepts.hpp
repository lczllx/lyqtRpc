#pragma once
// =============================================================================
// concepts.hpp — 全项目共享的 C++20 Concept 约束
// -----------------------------------------------------------------------------
// 供 client / server / general 的模板接口统一复用。约束失败时编译器直接报在
// 调用点（constraints not satisfied），而不是模板函数体深处的成员调用错误。
//
//   ProtoMessage<T>  具备 protobuf Message 序列化/反序列化接口
//                    （RpcCaller::call_proto / RpcClient::call_proto /
//                      RpcServer::registerProtoHandler / ProtoRpcRouter::registerProtoHandler）
//   RpcMessage<T>    派生自 BaseMessage 的 RPC 消息类型
//                    （MessageFactory::create / Dispacher::registerhandler / CallbackType）
//
// FlatBufferTable 不放在这里：它依赖 flatbuffers::Table，只有 shm_zc_adaptor.hpp
// 用到，就地定义避免把 flatbuffers 头拖进全项目编译单元。
// =============================================================================

#include "abstract.hpp"
#include <concepts>

namespace lcz_rpc
{
    // protobuf Message：能 SerializeToString / ParseFromString。requires 表达式是不求值
    // 语境，t 只是占位名、不会被真的构造，所以不要求 T 可默认构造。
    template <typename T>
    concept ProtoMessage = requires(T t)
    {
        t.SerializeToString(nullptr);
        t.ParseFromString("");
    };

    // RPC 消息：派生自 BaseMessage（自带 ::ptr 别名；BaseMessage 自身也满足）
    template <typename T>
    concept RpcMessage = std::derived_from<T, BaseMessage>;
}
