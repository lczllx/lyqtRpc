#pragma once
#include "message.hpp"
#include <string>
#include <memory>

namespace lcz_rpc
{
    // 序列化器抽象接口：将消息对象编码为线缆字节，或从字节解码为消息对象
    // 不同实现（Protobuf / FlatBuffers / JSON）可插拔替换，不修改消息类本身
    class ISerializer
    {
    public:
        using ptr = std::shared_ptr<ISerializer>;

        // 将消息编码为可在线缆上传输的字节串
        virtual std::string encode(const BaseMessage::ptr &msg) = 0;
        // 从字节串解码为消息对象
        virtual bool decode(const std::string &data, BaseMessage::ptr &msg) = 0;
        // 序列化器名称，用于日志和调试
        virtual const char *name() const = 0;

        virtual ~ISerializer() = default;
    };

    // Protobuf 序列化器：复用消息自带的 serialize()/unserialize()
    class ProtobufSerializer : public ISerializer
    {
    public:
        std::string encode(const BaseMessage::ptr &msg) override
        {
            return msg->serialize();
        }
        bool decode(const std::string &data, BaseMessage::ptr &msg) override
        {
            return msg->unserialize(data);
        }
        const char *name() const override { return "protobuf"; }
    };

} // namespace lcz_rpc
