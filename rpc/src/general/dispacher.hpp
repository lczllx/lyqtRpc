#pragma once

/*区分消息类型
工作流程
注册阶段：为每种消息类型注册处理函数
接收消息：网络层收到消息，调用 Dispacher::onMessage
类型查找：根据 MsgType 找到对应的回调包装器
类型转换：将 BaseMessage 安全转换为具体类型
业务处理：调用具体的业务处理函数*/
#include "net.hpp"
#include "message.hpp"
#include "concepts.hpp"
#include "publicconfig.hpp"
#include "log_system/lcz_log.h"

namespace lcz_rpc
{
    // 消息回调抽象基类：接收 conn 和 msg 的统一接口
    class Callback
    {
        public:
        using ptr=std::shared_ptr<Callback>;
        virtual void onMessage(const BaseConnection::ptr& conn,BaseMessage::ptr& msg)=0;       
    };
    // 类型化回调包装类：将 BaseMessage 转为 T 后调用用户回调
    template<RpcMessage T>
    class CallbackType:public Callback
    {
        public:
        using ptr=std::shared_ptr<CallbackType<T>>;
        using MessageCallback=std::function<void (const BaseConnection::ptr& conn,std::shared_ptr<T>& msg)>;
        // // 支持右值引用的构造函数
        // CallbackType(MessageCallback&& handler) : _handler(std::move(handler)) {}
      
        CallbackType(const MessageCallback &handler):_handler(handler){}
        // 接收 BaseMessage，安全转换为 T 后调用 _handler
        void onMessage(const BaseConnection::ptr& conn,BaseMessage::ptr& msg)
        {
            auto transmit_type=std::dynamic_pointer_cast<T>(msg);
            _handler(conn,transmit_type);// 调用具体类型的处理函数
        }
        private:
        MessageCallback _handler;
    };

    // 消息分发器类：按 MsgType 查找并调用已注册的处理器
    class Dispacher
    {
        public:
        using ptr=std::shared_ptr<Dispacher>;
        // //提供支持右值版本
        // template<typename T>
        // void registerhandler(MsgType msgtype,typename CallbackType<T>::MessageCallback&& handler)
        // {
        //     std::unique_lock<std::mutex> lock(_mutex);
        //     auto cb=std::make_shared<CallbackType<T>>(std::forward<typename CallbackType<T>::MessageCallback>(handler));// 创建类型特定的回调包装器
        //     _handlers.emplace(msgtype,cb);
        // }
         
        // 注册指定消息类型的处理函数
        template<RpcMessage T>
        void registerhandler(MsgType msgtype,const typename CallbackType<T>::MessageCallback& handler)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto cb=std::make_shared<CallbackType<T>>(handler);// 创建类型特定的回调包装器
            _handlers.emplace(msgtype,cb);
        }
        // 根据消息类型查找并调用对应的处理器
        void onMessage(const BaseConnection::ptr& conn,BaseMessage::ptr& msg)
        {
             if (!msg) {
                LCZ_ERROR("收到空消息");
                return;
            }
            std::unique_lock<std::mutex> lock(_mutex);
            auto it=_handlers.find(msg->msgType());
            if(it!=_handlers.end())
            {
                return it->second->onMessage(conn,msg);// 调用对应的处理器
            }
            //没有找到指定类型的处理回调
            LCZ_ERROR("收到未知消息类型 msgtype=%d (REQ_RPC=0..RSP_SERVICE=5, REQ_RPC_PROTO=6, RSP_RPC_PROTO=7, REQ_TOPIC_PROTO=8, RSP_TOPIC_PROTO=9, REQ_SERVICE_PROTO=10, RSP_SERVICE_PROTO=11)",
                 static_cast<int>(msg->msgType()));
            conn->shutdown();//关闭未知消息的连接

        }
        private:
        std::mutex _mutex;
        std::unordered_map<MsgType,Callback::ptr>_handlers;
    };
}