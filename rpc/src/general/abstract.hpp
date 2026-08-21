#pragma once
#include <memory>
#include <functional>
#include "fields.hpp"
#include "publicconfig.hpp"

namespace lcz_rpc { class ISerializer; }

namespace lcz_rpc {
    // 消息基类，所有具体消息类型都需要实现序列化、反序列化、合法性校验
    class BaseMessage {
    public:
        using ptr = std::shared_ptr<BaseMessage>;  // 智能指针类型定义
        virtual ~BaseMessage(){}
        // 设置消息 ID；RPC/Topic 等都会使用
        virtual void setId(const std::string &id) {_rid = id;}
        // 获取消息ID
        virtual std::string rid() { return _rid; }
        // 设置消息类型
        virtual void setMsgType(MsgType msgtype) {_msgtype = msgtype;}
        // 获取消息类型
        virtual MsgType msgType() { return _msgtype; }
        // 序列化消息
        virtual std::string serialize() = 0;
        // 反序列化消息
        virtual bool unserialize(const std::string &msg) = 0;
        // 检查消息有效性
        virtual bool check() = 0;
    private:
        MsgType _msgtype;        // 消息类型
        std::string _rid;        // 消息ID
    };

    // 缓冲区基类
    class BaseBuffer {
    public:
        using ptr = std::shared_ptr<BaseBuffer>;
        // 获取可读数据大小
        virtual size_t readableSize() = 0;
        // 查看int32数据（不移动读指针）
        virtual int32_t peekInt32() = 0;
        // 跳过int32数据
        virtual void retrieveInt32() = 0;
        // 读取int32数据
        virtual int32_t readInt32() = 0;
        // 读取指定长度的字符串
        virtual std::string retrieveAsString(size_t len) = 0;
    };

    // 协议处理基类
    class BaseProtocol {
    public:
        using ptr = std::shared_ptr<BaseProtocol>;
        // 判断是否能处理缓冲区数据
        virtual bool canProcessed(const BaseBuffer::ptr &buf) = 0;
        // 处理消息
        virtual bool onMessage(const BaseBuffer::ptr &buf, BaseMessage::ptr &msg) = 0;
        // 序列化消息
        virtual std::string serialize(const BaseMessage::ptr &msg) = 0;
        // 注入序列化器（LVProtocol 已实现，其他协议类可空实现）
        virtual void setSerializer(std::shared_ptr<ISerializer>) {}
    };

    // 连接基类
    class BaseConnection {
    public:
        using ptr = std::shared_ptr<BaseConnection>;
        // 发送消息
        virtual void send(const BaseMessage::ptr &msg) = 0;
        // 关闭连接
        virtual void shutdown() = 0;
        // 检查连接状态
        virtual bool connected() = 0;
        //返回对端地址
        virtual std::string peerAddress() const = 0;
    };

    // 连接建立回调
    using ConnectionCallback = std::function<void(const BaseConnection::ptr&)>;
    // 连接关闭回调
    using CloseCallback = std::function<void(const BaseConnection::ptr&)>;
    // 消息接收回调
    using MessageCallback = std::function<void(const BaseConnection::ptr&, BaseMessage::ptr&)>;
    // 服务器基类
    class BaseServer {
    public:
        using ptr = std::shared_ptr<BaseServer>;
        // 设置连接建立回调
        virtual void setConnectionCallback(const ConnectionCallback& cb) {
            _cb_connection = cb;
        }
        // 设置连接关闭回调
        virtual void setCloseCallback(const CloseCallback& cb) {
            _cb_close = cb;
        }
        // 设置消息接收回调
        virtual void setMessageCallback(const MessageCallback& cb) {
            _cb_message = cb;
        }
        // 启动服务器
        virtual void start() = 0;
        // 优雅退出：唤醒事件循环使其从 start() 返回
        virtual void stop() {}
        // 注入序列化器（MuduoServer 已实现）
        virtual void setSerializer(std::shared_ptr<ISerializer>) {}
    protected:
        ConnectionCallback _cb_connection;  // 连接建立回调
        CloseCallback _cb_close;            // 连接关闭回调
        MessageCallback _cb_message;        // 消息接收回调
    };

    // 客户端基类
    class BaseClient {
    public:
        using ptr = std::shared_ptr<BaseClient>;
        // 设置连接建立回调
        virtual void setConnectionCallback(const ConnectionCallback& cb) {
            _cb_connection = cb;
        }
        // 设置连接关闭回调
        virtual void setCloseCallback(const CloseCallback& cb) {
            _cb_close = cb;
        }
        // 设置消息接收回调
        virtual void setMessageCallback(const MessageCallback& cb) {
            _cb_message = cb;
        }
        // 连接服务器
        virtual void connect() = 0;
        // 关闭连接
        virtual void shutdown() = 0;
        // 发送消息
        virtual bool send(const BaseMessage::ptr& msg) = 0;
        // 获取连接对象
        virtual BaseConnection::ptr connection() = 0;
        // 检查连接状态
        virtual bool connected() = 0;
        // 注入序列化器，默认空实现（MuduoClient 覆盖）
        virtual void setSerializer(std::shared_ptr<ISerializer>) {}
    protected:
        ConnectionCallback _cb_connection;  // 连接建立回调
        CloseCallback _cb_close;            // 连接关闭回调
        MessageCallback _cb_message;        // 消息接收回调
    };
}