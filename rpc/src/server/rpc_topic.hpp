#pragma once
#include "general/dispacher.hpp"
#include "rpc_registry.hpp"
#include "rpc_router.hpp"
#include "client/rpc_client.hpp"
#include <unordered_set>
#include <iostream>
#include <algorithm>
#include "general/publicconfig.hpp"
#include "general/log_system/lcz_log.h"
namespace lcz_rpc
{
    namespace server
    {
        // 主题管理器类：负责 Topic 请求的创建/删除/订阅/发布等业务逻辑
        class TopicManager
        {
        public:
            using ptr = std::shared_ptr<TopicManager>;
             // 订阅者结构体：保存连接、优先级、标签及订阅的主题列表
             struct Subscribe
             {
                 using ptr = std::shared_ptr<Subscribe>;
                 std::chrono::steady_clock::time_point lastSeen{std::chrono::steady_clock::now()};//上次心跳时间
                 std::mutex mutex;
                 BaseConnection::ptr conn;
                 int priority = 0;
                 std::vector<std::string> tags;// 构造时把 priority/tags 从 TopicRequest (订阅消息) 里拉出来
                 std::unordered_set<std::string> topics; // 订阅的主题
                 Subscribe(const BaseConnection::ptr &sub_conn) : conn(sub_conn) {}
                 // 添加订阅的主题
                 void addTopic(const std::string &topicname)
                 {
                     std::unique_lock<std::mutex> lock(mutex);
                     topics.insert(topicname);
                 }
                 // 取消订阅指定主题
                 void removeTopic(const std::string &topicname)
                 {
                     std::unique_lock<std::mutex> lock(mutex);
                     topics.erase(topicname);
                 }
             };//Subscribe
             // 主题结构体：维护订阅者集合，按转发策略向订阅者分发消息
             struct Topic
             {
                 using ptr = std::shared_ptr<Topic>;
                 std::mutex mutex;
                 std::string topic_name;
                 std::unordered_set<Subscribe::ptr> subscribes; // 管理订阅者
                 size_t rr_cursor = 0;//rr轮转cursor
                 std::mt19937 rng{std::random_device{}()};//随机数生成器
                 Topic(const std::string &newtopic) : topic_name(newtopic) {}
                 // 添加订阅者
                 void addSubscribe(const Subscribe::ptr &sub)
                 {
                     std::unique_lock<std::mutex> lock(mutex);
                     subscribes.insert(sub);
                 }
                 // 移除订阅者
                 void delSubscribe(const Subscribe::ptr &sub)
                 {
                     std::unique_lock<std::mutex> lock(mutex);
                     subscribes.erase(sub);
                 }
                 // 根据转发策略向订阅者分发消息
                 void pushMessage(const BaseMessage::ptr &msg)
                 {             
                     std::vector<Subscribe::ptr> copy_sub;//使用额外的vector避免直接在锁内发送
                    {
                        std::unique_lock<std::mutex> lock(mutex);
                        copy_sub = std::vector<Subscribe::ptr>(subscribes.begin(), subscribes.end());
                    }
                    return dispatchMessage(msg,copy_sub);
                 }
                 // 按 forwardStrategy 选择分发方式（广播/轮询/扇出等）
                 void dispatchMessage(const BaseMessage::ptr &msg,const std::vector<Subscribe::ptr> &copy_sub)
                 {
                    auto topic_msg=std::dynamic_pointer_cast<TopicRequest>(msg);
                    TopicForwardStrategy strategy = topic_msg->forwardStrategy();
                    switch(strategy)
                    {
                        case TopicForwardStrategy::BROADCAST:
                            broadcastSend(msg,copy_sub);
                            break;
                        case TopicForwardStrategy::ROUND_ROBIN:
                            roundRobinSend(msg,copy_sub);
                            break;
                        case TopicForwardStrategy::FANOUT:
                            fanoutSend(msg,copy_sub);
                            break;
                        case TopicForwardStrategy::SOURCE_HASH:
                            sourceHashSend(msg,copy_sub);
                            break;
                        case TopicForwardStrategy::PRIORITY:
                            prioritySend(msg,copy_sub);
                            break;
                        case TopicForwardStrategy::REDUNDANT:
                            redundantSend(msg,copy_sub);
                            break;
                        default:
                        //默认广播
                        broadcastSend(msg,copy_sub);
                        break;
                    }
                 }
                 // 向所有订阅者广播
                 void broadcastSend(const BaseMessage::ptr &msg,const std::vector<Subscribe::ptr> &copy_sub)
                 {
                   if(copy_sub.empty())return;
                   for(auto &subscribe : copy_sub)
                       subscribe->conn->send(msg);
                 }
                 // 轮询方式发送给一个订阅者
                 void roundRobinSend(const BaseMessage::ptr &msg,const std::vector<Subscribe::ptr> &copy_sub)
                 {
                    if(copy_sub.empty())return;
                    Subscribe::ptr cur_sub;//
                    {
                        std::unique_lock<std::mutex> lock(mutex);
                        if (subscribes.empty()) return; // 可能被并发清空
                        rr_cursor %= subscribes.size();
                        cur_sub = copy_sub[rr_cursor % copy_sub.size()];//获取当前轮转的订阅者
                        rr_cursor = (rr_cursor + 1) % (copy_sub.size());//更新轮转cursor
                    }
                    cur_sub->conn->send(msg);
                 }
                 // 随机选取 fanoutLimit 个订阅者发送
                 void fanoutSend(const BaseMessage::ptr &msg,const std::vector<Subscribe::ptr> &copy_sub)
                 {
                    if(copy_sub.empty())return;
                    auto topic_msg=std::dynamic_pointer_cast<TopicRequest>(msg);
                    int fanout_limit = topic_msg->fanoutLimit();//获取扇出数量限制
                    if(fanout_limit<=0||fanout_limit>=copy_sub.size())broadcastSend(msg,copy_sub);
                    //复制订阅者列表
                    std::vector<Subscribe::ptr> fanout_sub(copy_sub.begin(), copy_sub.end());
                    {
                        std::unique_lock<std::mutex> lock(mutex);
                        std::shuffle(fanout_sub.begin(), fanout_sub.end(), rng);//将订阅者打乱
                    }
                    //发送扇出消息
                    for(int i=0;i<fanout_limit;i++)
                    {
                        fanout_sub[i]->conn->send(msg);
                    }
                 }
                 // 按 shardKey 哈希固定命中同一订阅者
                 void sourceHashSend(const BaseMessage::ptr &msg,const std::vector<Subscribe::ptr> &copy_sub)
                 {
                    if(copy_sub.empty())return;
                    auto topic_msg=std::dynamic_pointer_cast<TopicRequest>(msg);
                    std::string shard_key = topic_msg->shardKey();
                    if(shard_key.empty())return;
                    size_t hash_value = std::hash<std::string>()(shard_key);//计算源哈希键的哈希值
                    size_t pos = hash_value % copy_sub.size();
                    copy_sub[pos]->conn->send(msg);
                 }
                 // 按优先级和标签过滤，选最高优先级匹配者轮转发送
                 void prioritySend(const BaseMessage::ptr &msg,const std::vector<Subscribe::ptr> &copy_sub)
                 {
                    if (copy_sub.empty()) return;
                    auto topic_msg=std::dynamic_pointer_cast<TopicRequest>(msg);
                    const auto accesslable_tags = topic_msg->tags();//获取准入的标签
                    auto matchtags = [&](const Subscribe::ptr &sub) {
                        if (accesslable_tags.empty()) return true;//如果准入标签为空，则匹配所有订阅者
                        std::unordered_set<std::string> tagset(sub->tags.begin(), sub->tags.end());//获取订阅者的标签
                        for (const auto &tag : accesslable_tags)
                            if (!tagset.count(tag)) return false;//如果订阅者不包含准入标签，则不匹配
                        return true;
                    };//匹配标签的lambda表达式
                
                    int max_priority = std::numeric_limits<int>::min();//初始化最大优先级为int的最小值
                    std::vector<Subscribe::ptr> candidates;//用来存储符合条件的订阅者
                    for (auto &sub : copy_sub)
                    {
                        if (!matchtags(sub)) continue;//如果订阅者不包含准入标签，则不匹配
                        if (sub->priority > max_priority)
                        {
                            max_priority = sub->priority;
                            candidates.clear();
                            candidates.push_back(sub);
                        }
                        else if (sub->priority == max_priority)
                        {
                            candidates.push_back(sub);//如果优先级相同，则添加到候选列表
                        }
                    }
                    if (candidates.empty()) {
                        broadcastSend(msg,copy_sub);
                        return; 
                    }
                
                    // 静态变量，跨所有 Topic 实例共享，跨调用保持状态
                    // 注意：这意味着多个不同 Topic 的 PRIORITY 策略共用同一个轮询游标
                    static size_t pri_cursor = 0;
                    auto cur_sub = candidates[pri_cursor % candidates.size()];//获取当前优先级的订阅者
                    pri_cursor = (pri_cursor + 1) % candidates.size();//更新优先级cursor
                    cur_sub->conn->send(msg);//发送消息

                 }
                 // 随机选 redundantCount 个订阅者发送（冗余投递）
                 void redundantSend(const BaseMessage::ptr &msg,const std::vector<Subscribe::ptr> &copy_sub)
                 {
                    if(copy_sub.empty())return;
                    auto topic_msg=std::dynamic_pointer_cast<TopicRequest>(msg);
                    int redundant_count = topic_msg->redundantCount();//获取冗余数量限制
                    if(redundant_count<=1)//如果冗余数量小于等于1，则广播发送
                    {
                       return broadcastSend(msg,copy_sub);//广播发送
                    }
                    //随机挑选redundant_count个订阅者发送消息
                    std::vector<Subscribe::ptr> redundant_sub(copy_sub.begin(), copy_sub.end());
                    {
                        std::unique_lock<std::mutex> lock(mutex);
                        std::shuffle(redundant_sub.begin(), redundant_sub.end(), rng);//将订阅者打乱
                    }
                    redundant_count=std::min(redundant_count,static_cast<int>(copy_sub.size()));
                    for(int i=0;i<redundant_count;++i)
                    {
                        redundant_sub[i]->conn->send(msg);
                    }
                    return;
                }
             };//Topic
            //TopicManager() {}
            // 处理主题请求：创建/删除/订阅/取消订阅/发布
            void ontopicRequest(const BaseConnection::ptr &conn, TopicRequest::ptr &msg)
            {
                bool ret = true;
                switch (msg->optype())
                {
                case TopicOpType::CREATE:
                    topicCreate(conn, msg);
                    break;
                case TopicOpType::REMOVE:
                    topicRemove(conn, msg);
                    break;
                case TopicOpType::SUBSCRIBE:
                    ret=topicSubscribe(conn, msg);
                    break;
                case TopicOpType::UNSUBSCRIBE:
                    topicancelSubscribe(conn, msg);
                    break;
                case TopicOpType::PUBLISH:
                    ret=topicPublish(conn, msg);
                    break;
                default:
                    return errorResponse(conn, msg,RespCode::INVALID_OPTYPE);
                }
                if (!ret)
                    return errorResponse(conn, msg,RespCode::TOPIC_NOT_FOUND);
                return topicResponse(conn, msg);
            }
            // 连接断开时从各主题移除该订阅者
            void onconnShoutdown(const BaseConnection::ptr& conn) 
            {
                std::vector<Topic::ptr> topics;
                Subscribe::ptr subscribe;
                auto key = conn.get();
                {
                    std::unique_lock<std::mutex>lock(_mutex);
                    auto it=_subscribes.find(key);
                    if(it==_subscribes.end())return ;
                    subscribe=it->second;
                    for(auto&topic_name :subscribe->topics)
                    {
                        auto topic_it=_topics.find(topic_name);
                        if(topic_it!=_topics.end())topics.emplace_back(topic_it->second);
                        
                    }
                    _subscribes.erase(it);//从订阅者map中删除订阅者
                }
                for(auto& topic:topics)
                {
                    topic->delSubscribe(subscribe);
                }
            }

        private:
            // 发送主题操作成功响应
            void topicResponse(const BaseConnection::ptr &conn,const TopicRequest::ptr &msg)
            {
                auto resp_msg = MessageFactory::create<TopicResponse>();
                resp_msg->setMsgType(MsgType::RSP_TOPIC);
                resp_msg->setRcode(RespCode::SUCCESS);
                resp_msg->setId(msg->rid());
                conn->send(resp_msg);
            }
            // 发送主题操作错误响应
            void errorResponse(const BaseConnection::ptr &conn,const TopicRequest::ptr &msg,RespCode rcode)
            {
                auto resp_msg = MessageFactory::create<TopicResponse>();
                resp_msg->setMsgType(MsgType::RSP_TOPIC);
                resp_msg->setRcode(rcode);
                resp_msg->setId(msg->rid());
                conn->send(resp_msg);
            }
            // 创建主题（已存在则忽略）
            void topicCreate(const BaseConnection::ptr &conn,const TopicRequest::ptr &msg)
            {
                std::unique_lock<std::mutex> lock(_mutex);
                std::string topic_name = msg->topicKey();
                if (_topics.count(topic_name)) //如果主题已存在，则直接返回
                {
                    LCZ_ERROR("topic %s already exists", topic_name.c_str());
                    return;
                }
                _topics.emplace(topic_name, std::make_shared<Topic>(topic_name));//如果主题不存在，则创建主题
            }
            // 删除主题并通知订阅者移除
            void topicRemove(const BaseConnection::ptr &conn,const TopicRequest::ptr &msg)
            {
                std::unordered_set<Subscribe::ptr> subscribes;
                std::string topic_name = msg->topicKey();
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto it = _topics.find(topic_name);
                    if (it == _topics.end())
                        return;
                    // Topic::subscribes 有自己的互斥量，复制前需要持有内部锁避免迭代器失效
                    {
                        std::unique_lock<std::mutex> topic_lock(it->second->mutex);
                        subscribes = it->second->subscribes;
                    }
                    _topics.erase(it);
                }
                for (auto &subscribe : subscribes)
                {
                    subscribe->removeTopic(topic_name);
                }
            }
            // 将连接加入主题的订阅者列表
            bool topicSubscribe(const BaseConnection::ptr &conn,const TopicRequest::ptr &msg)
            {
                Topic::ptr topic;
                Subscribe::ptr subscribe;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto topic_it = _topics.find(msg->topicKey());
                    if (topic_it == _topics.end())
                    {
                        return false;
                    }
                    topic = topic_it->second;
                    
                    auto key = conn.get();
                    auto subscribe_it = _subscribes.find(key);
                    if (subscribe_it == _subscribes.end())//没有找到订阅者，创建订阅者-这里出现了错误
                    {
                        subscribe = std::make_shared<Subscribe>(conn);
                        _subscribes[key] = subscribe;
                    }
                    else
                    {
                        subscribe = subscribe_it->second;
                    }
                }
                subscribe->priority = msg->priority();//设置订阅者优先级
                subscribe->tags = msg->tags();//设置订阅者标签
                topic->addSubscribe(subscribe);//添加订阅者到其订阅的主题
                subscribe->addTopic(msg->topicKey());//添加其订阅的主题到订阅者
                return true;
            }
            // 从主题的订阅者列表中移除该连接
            void topicancelSubscribe(const BaseConnection::ptr &conn,const TopicRequest::ptr &msg)
            {
                // 如果找不到不需要特殊处理
                Topic::ptr topic;
                Subscribe::ptr subscribe;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto topic_it = _topics.find(msg->topicKey());
                    if (topic_it != _topics.end())
                    {
                        topic = topic_it->second;
                    }
                    auto key = conn.get();
                    auto subscribe_it = _subscribes.find(key);
                    if (subscribe_it != _subscribes.end())
                    {
                        subscribe = subscribe_it->second;
                    }
                }
               
                if (subscribe)
                    subscribe->removeTopic(msg->topicKey());
                if (topic && subscribe)
                    topic->delSubscribe(subscribe);
                // 如果该订阅者已没有订阅的主题，可在此处考虑从 _subscribes 中清理，保持映射紧凑
            }
            // 向主题的订阅者分发发布消息
            bool topicPublish(const BaseConnection::ptr &conn,const TopicRequest::ptr &msg)
            {
                // 没有找到要发布的主题，返回false
                Topic::ptr topic;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    auto topic_it = _topics.find(msg->topicKey());
                    if (topic_it != _topics.end())
                    {
                        topic = topic_it->second;
                    }
                    else
                    {
                        return false;
                    }
                }
                topic->pushMessage(msg);
                return true;
            }

        private:
            std::mutex _mutex;
            std::unordered_map<std::string, Topic::ptr> _topics;
            std::unordered_map<BaseConnection*, Subscribe::ptr> _subscribes;
        };
    }
}