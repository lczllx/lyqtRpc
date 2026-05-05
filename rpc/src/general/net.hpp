#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include "log_system/lcz_log.h"

// muduo 网络库头文件
#include <muduo/net/TcpServer.h>//tcp服务器 
#include <muduo/net/TcpClient.h>//tcp客户端
#include <muduo/net/TcpConnection.h>//tcp连接
#include <muduo/net/EventLoop.h>//事件循环
#include <muduo/net/EventLoopThread.h>//事件循环线程
#include <muduo/net/InetAddress.h>//网络地址
#include <muduo/net/Buffer.h>//缓冲区
#include <muduo/net/Callbacks.h>//回调函数
#include <muduo/base/CountDownLatch.h>//倒计时器
#include <muduo/net/EventLoopThread.h>
#include <muduo/base/Timestamp.h>

#include "abstract.hpp"
#include "message.hpp"
#include "publicconfig.hpp"

namespace lcz_rpc
{
    // Muduo 缓冲区适配器类：将 muduo::net::Buffer 封装成 BaseBuffer 接口
    class MuduoBuffer:public BaseBuffer
    {
        public:
        using ptr = std::shared_ptr<MuduoBuffer>;
        MuduoBuffer(muduo::net::Buffer* buffer)
        {
            _buffer = buffer;
        }
         // 获取可读数据大小
         virtual size_t readableSize() override
         {
            return _buffer->readableBytes();
         }
         // 查看int32数据（不移动读指针）
         virtual int32_t peekInt32() override
         {
            //muduo库是网络库，从缓冲区中获取int32数据，会进行网络字节序到主机字节序的转换
            return _buffer->peekInt32();
         }
         // 跳过int32数据
         virtual void retrieveInt32() override
         {
            _buffer->retrieveInt32();
         }
         // 读取int32数据
         virtual int32_t readInt32() override
         {
            return _buffer->readInt32();
         }
         // 读取指定长度的字符串
         virtual std::string retrieveAsString(size_t len) override
         {
            return _buffer->retrieveAsString(len);
         }
        private:
        muduo::net::Buffer* _buffer;
       
    };
    // Buffer 工厂类：创建 MuduoBuffer 实例
    class BufferFactory
    {
      public:
      template<typename... ARGS>
      static BaseBuffer::ptr create(ARGS&& ...args)
      {
         return std::make_shared<MuduoBuffer>(std::forward<ARGS>(args)...);
      }
    };
    // LV 协议类：基于「长度+类型+id+body」的简单网络协议
    class LVProtocol :public BaseProtocol
    {
      public:
          using ptr = std::shared_ptr<LVProtocol>;
          // 判断缓冲区中是否已有完整消息可解析
          virtual bool canProcessed(const BaseBuffer::ptr &buf) override
          {
            // 检查是否有足够的数据读取长度字段
            if(buf->readableSize() < _totalfield_len)
            {
               return false;
            }
            // 检查完整消息是否已到达（peekInt32 返回的是消息体长度）
            int32_t body_len = buf->peekInt32();
            if(body_len > buf->readableSize() - _totalfield_len)
            {
               return false;  // 数据不完整，继续等待
            }
            return true;
          }
          // 从缓冲区解析一条完整消息，填充 msg
          virtual bool onMessage(const BaseBuffer::ptr &buf, BaseMessage::ptr &msg) override
          {
            if(!canProcessed(buf)){return false;}
            int32_t total_len=buf->readInt32();
            MsgType msgtype=static_cast<MsgType>(buf->readInt32());
            int32_t id_len=buf->readInt32();
            int32_t data_len=total_len-_msgidfield_len-_msgtypefield_len-id_len;
            
            std::string id=buf->retrieveAsString(id_len);     
            std::string data=buf->retrieveAsString(data_len);
            msg=MessageFactory::create(msgtype);
            if(msg.get()==nullptr){LCZ_ERROR("创建消息失败");return false;}
            bool ret=msg->unserialize(data);//反序列化数据
            if(!ret){LCZ_ERROR("反序列化数据失败");return false;}
            msg->setId(id);
            msg->setMsgType(msgtype);
            return true;
          }
          // 将消息序列化为「长度+类型+id+body」格式的字节串
          virtual std::string serialize(const BaseMessage::ptr &msg) override
          {
            //len msgtype idlen id data
            std::string data=msg->serialize();//序列化数据
            if(data.empty()){LCZ_ERROR("序列化数据失败");return "";}    
            std::string id=msg->rid();

            //获取消息类型和ID长度和数据长度
            int32_t msgtype = static_cast<int32_t>(msg->msgType());  
            int32_t id_len = static_cast<int32_t>(id.size());        
            int32_t data_len = static_cast<int32_t>(data.size());

            //计算总长度
            int32_t total_len=_msgtypefield_len+_msgidfield_len+id_len+data_len;

            //转换为网络字节序，保证跨平台移植性
            auto total_len_net = htonl(total_len);
            auto msgtype_net = htonl(msgtype);
            auto id_len_net = htonl(id_len);

            std::string output;
            output.reserve(total_len);
            output.append((char*)&total_len_net,_totalfield_len);
            output.append((char*)&msgtype_net,_msgtypefield_len);
            output.append((char*)&id_len_net,_msgidfield_len);
            output.append(id);
            output.append(data);
            return output;
          }
          private:
          const size_t _totalfield_len=4;      
          const size_t _msgtypefield_len=4;
          const size_t _msgidfield_len=4;
      };
      // 协议工厂类：创建 LVProtocol 实例
      class ProtocolFactory
      {
        public:
        template<typename... ARGS>
        static BaseProtocol::ptr create(ARGS&& ...args)
        {
          return std::make_shared<LVProtocol>(std::forward<ARGS>(args)...);
        }
       
      };
      // Muduo 连接类：BaseConnection 的 muduo 实现，负责序列化和底层 send/shutdown
      class MuduoConnection :public BaseConnection
      {
         public:
         using ptr = std::shared_ptr<MuduoConnection>;
         MuduoConnection(const muduo::net::TcpConnectionPtr& connection,const BaseProtocol::ptr& protocol)
         {
            _connection = connection;
            _protocol = protocol;
         }
         // 将消息序列化后通过底层 TcpConnection 发送
          virtual void send(const BaseMessage::ptr &msg)override
          {
            if (!_connection->connected()) return;
            std::string data=_protocol->serialize(msg);
            _connection->send(data);
          }
          // 关闭底层 TCP 连接
          virtual void shutdown()override
          {
            _connection->shutdown();
          }
          // 检查底层连接是否仍然有效
          virtual bool connected()override
          {
            return _connection->connected();
          }
          // 检查底层连接是否仍然有效
          virtual std::string peerAddress()const override
          {
            return _connection->peerAddress().toIpPort();  // muduo::InetAddress::toIpPort() → "10.0.0.1:8889"
          }
          // 获取底层连接的 EventLoop，供 Requestor 等设置超时定时器
          muduo::net::EventLoop* getLoop() const
          {
            return _connection->getLoop();
          }
         private:
         muduo::net::TcpConnectionPtr _connection;
         BaseProtocol::ptr _protocol;
        
      };
      // 连接工厂类：创建 MuduoConnection 实例
      class ConnectionFactory
      {
         public:
         template<typename... ARGS>
         static BaseConnection::ptr create(ARGS&& ...args)
         {
            return std::make_shared<MuduoConnection>(std::forward<ARGS>(args)...);
         }
      };
      // Muduo 服务器类：基于 muduo::net::TcpServer 的 BaseServer 实现
      class MuduoServer :public BaseServer 
      {
         public:
             using ptr = std::shared_ptr<MuduoServer>;
            MuduoServer(const int port)
            :_server(&_baseloop,muduo::net::InetAddress("0.0.0.0",port),"MuduoServer",muduo::net::TcpServer::kReusePort/*启用端口重用*/)
            ,_protocol(ProtocolFactory::create()){}
             // 启动服务器并进入事件循环（阻塞）
            virtual void start() override
            {
               _server.setConnectionCallback(std::bind(&MuduoServer::onConnection,this,std::placeholders::_1));
               _server.setMessageCallback(std::bind(&MuduoServer::onMessage,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
               _server.start();//开始监听
               _baseloop.loop();//启动事件循环
            }
            private:
            // 连接建立/断开时维护 _connections 映射并触发回调
            void onConnection(const muduo::net::TcpConnectionPtr& conn)
            {
              if(conn->connected())
              {
               LCZ_DEBUG("新连接建立");
               auto muduo_conn=ConnectionFactory::create(conn,_protocol);
               {
                  std::unique_lock<std::mutex> lock(_mutex);
                  _connections[conn]=muduo_conn;
               }
               if(_cb_connection)_cb_connection(muduo_conn);
              }
              else
              {
                 LCZ_DEBUG("连接断开");
                 BaseConnection::ptr muduo_conn;
                 {                
                     std::unique_lock<std::mutex> lock(_mutex);
                     auto it=_connections.find(conn);
                     if(it==_connections.end())
                     {
                       return;
                     }
                     muduo_conn=it->second;
                     _connections.erase(it);
                     if(_cb_close)_cb_close(muduo_conn);
                  }                
              }
            }
           // 从 Buffer 解析完整消息并调用 _cb_message 派发
           void onMessage(const muduo::net::TcpConnectionPtr& conn,muduo::net::Buffer* buf,muduo::Timestamp receiveTime)
           {
              auto base_buf=BufferFactory::create(buf);            
              while(true)
              {
                 if(_protocol->canProcessed(base_buf)==false)
                 {
                    LCZ_DEBUG("数据不完整，继续等待");
                    if(base_buf->readableSize()>_maxdatalen)
                    {
                     conn->shutdown();
                       LCZ_ERROR("数据长度超过最大值");
                       return;
                    }
                    break;
                 }
                 
                 BaseMessage::ptr msg;
               bool ret = _protocol->onMessage(base_buf, msg);
               if (ret == false) {
               conn->shutdown();
               LCZ_ERROR("缓冲区中数据错误！");
               return ;
               }
               //LCZ_DEBUG("消息反序列化成功！")
               BaseConnection::ptr base_conn;
               {
               std::unique_lock<std::mutex> lock(_mutex);
               auto it = _connections.find(conn);
               if (it == _connections.end()) {
               conn->shutdown();
               return;
               }
               base_conn = it->second;
               }
               //LCZ_DEBUG("调⽤回调函数进⾏消息处理！");
               if (_cb_message) _cb_message(base_conn, msg);
              }
           }
         private:
            const size_t _maxdatalen=1024*1024*10;   //10M   
            muduo::net::EventLoop _baseloop;
            muduo::net::TcpServer _server;
            BaseProtocol::ptr _protocol;
            std::unordered_map<muduo::net::TcpConnectionPtr/**<-- 网络连接指针 */,BaseConnection::ptr/**<-- 抽象连接指针 */> _connections;
            std::mutex _mutex;
      };
      // 服务器工厂类：创建 MuduoServer 实例
      class ServerFactory
      {
         public:
         template<typename... ARGS>
         static BaseServer::ptr create(ARGS&& ...args)
         {
            return std::make_shared<MuduoServer>(std::forward<ARGS>(args)...);
         }
      };
      // Muduo 客户端类：基于 muduo::net::TcpClient 的 BaseClient 实现，支持同步 connect/shutdown
      class MuduoClient :public BaseClient 
      {
         public:
            using ptr = std::shared_ptr<MuduoClient>;
           
            MuduoClient(const std::string& sip,const int sport)
            :_protocol(ProtocolFactory::create())
            ,_baceloop(_loopthread.startLoop())  // 独立的事件循环线程
            ,_downlatch(new muduo::CountDownLatch(1))
            ,_client(_baceloop,muduo::net::InetAddress(sip,sport),"MuduoClient"){}
            // 连接建立/断开时更新 _connection
            void onConnection(const muduo::net::TcpConnectionPtr& conn)
            {
              if(conn->connected())
              {
                 LCZ_DEBUG("连接建立");
                 _connection = ConnectionFactory::create(conn, _protocol);
                 _downlatch->countDown();
              }
              else{
                LCZ_DEBUG("连接断开");_connection.reset();
              } 
           }
           // 从 Buffer 解析消息并调用 _cb_message 派发
           void onMessage(const muduo::net::TcpConnectionPtr& conn,muduo::net::Buffer* buf,muduo::Timestamp receiveTime)
           {
             auto bace_buf=BufferFactory::create(buf);
            
             while(true)
             {
                if(_protocol->canProcessed(bace_buf)==false)
                {
                   LCZ_DEBUG("数据不完整，继续等待");
                   if(bace_buf->readableSize()>_maxdatalen)
                   {
                      LCZ_ERROR("数据长度超过最大值");
                      return;
                   }
                   break;
                }           
                BaseMessage::ptr msg;
                bool ret=_protocol->onMessage(bace_buf,msg);
                if(!ret){conn->shutdown();LCZ_ERROR("处理消息失败");return;}
                if(_cb_message) _cb_message(_connection,msg);
             }
           }
           // 连接服务器并阻塞等待连接建立（支持断线重连）
           virtual void connect() override
           {
             _client.setConnectionCallback(std::bind(&MuduoClient::onConnection,this,std::placeholders::_1));
             _client.setMessageCallback(std::bind(&MuduoClient::onMessage,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
              // 每次 connect 重建 latch，支持重复调用（断线重连等场景）
              _downlatch.reset(new muduo::CountDownLatch(1));
              _client.connect();
               _downlatch->wait();
               LCZ_DEBUG("连接服务器成功！");
            }
             // 断开与服务器的连接
             virtual void shutdown() override
             {
               _client.disconnect();
             }
           // 通过已建立的连接发送消息，失败返回 false
           virtual bool send(const BaseMessage::ptr& msg) override
           {
               if (!_connection) {LCZ_ERROR("连接对象为空"); return false;}
               if (!_connection->connected()) {LCZ_ERROR("底层连接已断开");return false;}
               _connection->send(msg);
               return true;
           
           }
             // 获取封装后的连接对象
             virtual BaseConnection::ptr connection()  override
             {
               if(_connection.get()==nullptr){LCZ_ERROR("连接不存在");return nullptr;}
               return _connection;
             }
             // 检查是否已连接且连接有效
             virtual bool connected()  override
             {
               return _connection && _connection->connected();
             }
        private:
           const size_t _maxdatalen=1024*1024*10;   //10M 
           BaseProtocol::ptr _protocol;
           muduo::net::EventLoopThread _loopthread;
           muduo::net::EventLoop* _baceloop;
           std::unique_ptr<muduo::CountDownLatch> _downlatch;
           muduo::net::TcpClient _client;
           BaseConnection::ptr _connection;
      };
      // 客户端工厂类：创建 MuduoClient 实例
      class ClientFactory
      {
         public:
         template<typename... ARGS>
         static BaseClient::ptr create(ARGS&& ...args)
         {
            return std::make_shared<MuduoClient>(std::forward<ARGS>(args)...);
         }
      };
}
