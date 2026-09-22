// TopicManager 单元测试：订阅后推送回调 / 无订阅忽略 / 非 PUBLISH 操作忽略
#include <gtest/gtest.h>
#include <memory>
// 注意：不能直接 include rpc_topic.hpp——它内部先 include server/rpc_topic.hpp，
// 而后者又 include rpc_client.hpp，会形成 include 环导致 client::TopicManager 未定义。
// 必须像 example 那样先 include rpc_client.hpp（它在 :8 先引入 client/rpc_topic.hpp 建立正确顺序）。
#include "src/client/rpc_client.hpp"

using lcz_rpc::client::TopicManager;
using lcz_rpc::client::Requestor;
using lcz_rpc::BaseConnection;
using lcz_rpc::BaseMessage;
using lcz_rpc::MessageFactory;
using lcz_rpc::TopicRequest;
using lcz_rpc::TopicResponse;
using lcz_rpc::TopicOpType;
using lcz_rpc::RespCode;

namespace
{
    // echo=true 时回灌 SUCCESS 的 TopicResponse，使 subscribeTopic 的 commonRequest 成功、回调得以注册
    class FakeConnection : public BaseConnection, public std::enable_shared_from_this<FakeConnection>
    {
    public:
        Requestor::ptr requestor;
        bool echo = false;
        void send(const BaseMessage::ptr &msg) override
        {
            if (echo && requestor)
            {
                auto resp = MessageFactory::create<TopicResponse>();
                resp->setId(msg->rid());
                resp->setRcode(RespCode::SUCCESS);
                BaseMessage::ptr r = resp;
                requestor->onResponse(shared_from_this(), r);
            }
        }
        void shutdown() override {}
        bool connected() override { return true; }
        std::string peerAddress() const override { return "10.0.0.1:8080"; }
    };

    TopicRequest::ptr makePublish(const std::string &topic, const std::string &msg)
    {
        auto t = MessageFactory::create<TopicRequest>();
        t->setOptype(TopicOpType::PUBLISH);
        t->setTopicKey(topic);
        t->setTopicMsg(msg);
        return t;
    }
}

// 订阅成功 + 收到 PUBLISH 推送 → 触发回调（topic_name, topic_msg）
TEST(TopicManagerTest, SubscribeThenPublishInvokesCallback)
{
    auto requestor = std::make_shared<Requestor>();
    TopicManager tm(requestor);
    auto conn = std::make_shared<FakeConnection>();
    conn->requestor = requestor;
    conn->echo = true;

    std::string got_topic, got_msg;
    ASSERT_TRUE(tm.subscribeTopic(conn, "news",
        [&](const std::string &t, const std::string &m) { got_topic = t; got_msg = m; }));

    tm.onTopicPublish(conn, makePublish("news", "hello"));
    EXPECT_EQ(got_topic, "news");
    EXPECT_EQ(got_msg, "hello");
}

// PUBLISH 到未订阅的主题 → 不触发已订阅其它主题的回调
TEST(TopicManagerTest, PublishToUnsubscribedTopicIgnored)
{
    auto requestor = std::make_shared<Requestor>();
    TopicManager tm(requestor);
    auto conn = std::make_shared<FakeConnection>();
    conn->requestor = requestor;
    conn->echo = true;

    bool called = false;
    ASSERT_TRUE(tm.subscribeTopic(conn, "a",
        [&](const std::string &, const std::string &) { called = true; }));

    tm.onTopicPublish(conn, makePublish("b", "msg")); // 订阅的是 a，不是 b
    EXPECT_FALSE(called);
}

// 非 PUBLISH 操作（CREATE）→ 忽略，不崩溃、不触发回调
TEST(TopicManagerTest, NonPublishOptypeIgnored)
{
    auto requestor = std::make_shared<Requestor>();
    TopicManager tm(requestor);
    auto conn = std::make_shared<FakeConnection>();

    auto t = MessageFactory::create<TopicRequest>();
    t->setOptype(TopicOpType::CREATE);
    t->setTopicKey("news");
    EXPECT_NO_THROW(tm.onTopicPublish(conn, t));
}
