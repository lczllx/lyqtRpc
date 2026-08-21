// DiscoverManager / PwithDManager 单元测试：服务发现通知 + 注册/发现/负载/心跳协调
#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <memory>
#include <string>
#include <vector>
#include "src/server/rpc_registry.hpp"
#include "src/server/memory_registry_store.hpp"
#include "src/general/abstract.hpp"

using lcz_rpc::server::DiscoverManager;
using lcz_rpc::server::PwithDManager;
using lcz_rpc::server::MemoryRegistryStore;
using lcz_rpc::BaseConnection;
using lcz_rpc::BaseMessage;
using lcz_rpc::ServiceRequest;
using lcz_rpc::ServiceResponse;
using lcz_rpc::ServiceOpType;
using lcz_rpc::RespCode;
using lcz_rpc::MessageFactory;
using lcz_rpc::HostInfo;

namespace
{
    // 记录发送的消息，供断言通知（ServiceRequest）与响应（ServiceResponse）
    class RecordingConnection : public BaseConnection
    {
    public:
        std::vector<BaseMessage::ptr> sent;
        void send(const BaseMessage::ptr &msg) override { sent.push_back(msg); }
        void shutdown() override {}
        bool connected() override { return true; }
        std::string peerAddress() const override { return "rec:1"; }
    };

    // 从已发送消息中取最后一个 ServiceRequest（用于断言上线/下线通知）
    ServiceRequest::ptr lastServiceRequest(const std::shared_ptr<RecordingConnection> &conn)
    {
        if (conn->sent.empty())
            return nullptr;
        return std::dynamic_pointer_cast<ServiceRequest>(conn->sent.back());
    }

    ServiceRequest::ptr makeServiceReq(ServiceOpType optype, const std::string &method,
                                       const HostInfo &host, int load)
    {
        auto req = MessageFactory::create<ServiceRequest>();
        req->setMethod(method);
        req->setOptype(optype);
        req->setHost(host);
        req->setLoad(load);
        return req;
    }
}

// ===================== DiscoverManager =====================

TEST(DiscoverManagerTest, AddAndGetDiscoverer)
{
    DiscoverManager dm;
    auto conn = std::make_shared<RecordingConnection>();
    auto d = dm.addDiscoverer(conn, HostInfo("10.0.0.1", 8080), "echo");
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(dm.getProvider(conn), d);
}

TEST(DiscoverManagerTest, GetUnknownDiscovererNull)
{
    DiscoverManager dm;
    auto conn = std::make_shared<RecordingConnection>();
    EXPECT_EQ(dm.getProvider(conn), nullptr);
}

// 上线通知广播给所有关注该 method 的 discoverer
TEST(DiscoverManagerTest, OnlineNotifyBroadcastsToDiscoverers)
{
    DiscoverManager dm;
    auto c1 = std::make_shared<RecordingConnection>();
    auto c2 = std::make_shared<RecordingConnection>();
    dm.addDiscoverer(c1, HostInfo(), "echo");
    dm.addDiscoverer(c2, HostInfo(), "echo");

    HostInfo host("10.0.0.9", 9999);
    dm.onlineNotify("echo", host);

    auto m1 = lastServiceRequest(c1);
    auto m2 = lastServiceRequest(c2);
    ASSERT_NE(m1, nullptr);
    ASSERT_NE(m2, nullptr);
    EXPECT_EQ(m1->optype(), ServiceOpType::ONLINE);
    EXPECT_EQ(m1->method(), "echo");
    EXPECT_EQ(m1->host(), host);
    EXPECT_EQ(m2->optype(), ServiceOpType::ONLINE);
}

// 删除 discoverer 后不再收到通知
TEST(DiscoverManagerTest, DelDiscovererStopsNotifications)
{
    DiscoverManager dm;
    auto conn = std::make_shared<RecordingConnection>();
    dm.addDiscoverer(conn, HostInfo(), "echo");
    dm.delProvider(conn);

    dm.onlineNotify("echo", HostInfo("10.0.0.9", 9999));
    EXPECT_TRUE(conn->sent.empty());
}

TEST(DiscoverManagerTest, OfflineNotifySendsOffline)
{
    DiscoverManager dm;
    auto conn = std::make_shared<RecordingConnection>();
    dm.addDiscoverer(conn, HostInfo(), "echo");
    dm.offlineNotify("echo", HostInfo("10.0.0.9", 9999));

    auto m = lastServiceRequest(conn);
    ASSERT_NE(m, nullptr);
    EXPECT_EQ(m->optype(), ServiceOpType::OFFLINE);
}

// ===================== PwithDManager =====================

TEST(PwithDManagerTest, RegisterRespondsSuccess)
{
    auto store = std::make_shared<MemoryRegistryStore>();
    PwithDManager mgr(store);
    auto conn = std::make_shared<RecordingConnection>();

    mgr.onserviceRequest(conn, makeServiceReq(ServiceOpType::REGISTER, "echo", HostInfo("10.0.0.1", 8080), 5));

    ASSERT_EQ(conn->sent.size(), 1u);
    auto resp = std::dynamic_pointer_cast<ServiceResponse>(conn->sent[0]);
    ASSERT_NE(resp, nullptr);
    EXPECT_EQ(resp->rcode(), RespCode::SUCCESS);
    EXPECT_EQ(resp->optype(), ServiceOpType::REGISTER);
    EXPECT_EQ(store->methodHost("echo").size(), 1u); // 注册成功可发现
}

// 发现返回主机列表（含负载）
TEST(PwithDManagerTest, DiscoverReturnsHostList)
{
    auto store = std::make_shared<MemoryRegistryStore>();
    PwithDManager mgr(store);
    auto provider = std::make_shared<RecordingConnection>();
    mgr.onserviceRequest(provider, makeServiceReq(ServiceOpType::REGISTER, "echo", HostInfo("10.0.0.1", 8080), 5));

    auto discoverer = std::make_shared<RecordingConnection>();
    mgr.onserviceRequest(discoverer, makeServiceReq(ServiceOpType::DISCOVER, "echo", HostInfo(), 0));

    ASSERT_EQ(discoverer->sent.size(), 1u);
    auto resp = std::dynamic_pointer_cast<ServiceResponse>(discoverer->sent[0]);
    ASSERT_NE(resp, nullptr);
    EXPECT_EQ(resp->rcode(), RespCode::SUCCESS);
    EXPECT_EQ(resp->optype(), ServiceOpType::DISCOVER);

    auto hosts = resp->hostsDetail();
    ASSERT_EQ(hosts.size(), 1u);
    EXPECT_EQ(hosts[0].host, HostInfo("10.0.0.1", 8080));
    EXPECT_EQ(hosts[0].load, 5);
}

// 负载上报：已知成功，未知返回 SERVICE_NOT_FOUND
TEST(PwithDManagerTest, LoadReportSuccessAndUnknownFailure)
{
    auto store = std::make_shared<MemoryRegistryStore>();
    PwithDManager mgr(store);
    auto conn = std::make_shared<RecordingConnection>();
    mgr.onserviceRequest(conn, makeServiceReq(ServiceOpType::REGISTER, "echo", HostInfo("10.0.0.1", 8080), 5));

    auto good = std::make_shared<RecordingConnection>();
    mgr.onserviceRequest(good, makeServiceReq(ServiceOpType::LOAD_REPORT, "echo", HostInfo("10.0.0.1", 8080), 99));
    auto resp_ok = std::dynamic_pointer_cast<ServiceResponse>(good->sent[0]);
    EXPECT_EQ(resp_ok->rcode(), RespCode::SUCCESS);

    auto bad = std::make_shared<RecordingConnection>();
    mgr.onserviceRequest(bad, makeServiceReq(ServiceOpType::LOAD_REPORT, "nope", HostInfo("10.0.0.1", 8080), 99));
    auto resp_bad = std::dynamic_pointer_cast<ServiceResponse>(bad->sent[0]);
    EXPECT_EQ(resp_bad->rcode(), RespCode::SERVICE_NOT_FOUND);
}

// 心跳：已知 provider 成功，未知失败
TEST(PwithDManagerTest, HeartbeatSuccessAndUnknownFailure)
{
    auto store = std::make_shared<MemoryRegistryStore>();
    PwithDManager mgr(store);
    auto conn = std::make_shared<RecordingConnection>();
    mgr.onserviceRequest(conn, makeServiceReq(ServiceOpType::REGISTER, "echo", HostInfo("10.0.0.1", 8080), 5));

    auto good = std::make_shared<RecordingConnection>();
    mgr.onserviceRequest(good, makeServiceReq(ServiceOpType::HEARTBEAT_PROVIDER, "echo", HostInfo("10.0.0.1", 8080), 0));
    auto resp_ok = std::dynamic_pointer_cast<ServiceResponse>(good->sent[0]);
    EXPECT_EQ(resp_ok->rcode(), RespCode::SUCCESS);

    auto bad = std::make_shared<RecordingConnection>();
    mgr.onserviceRequest(bad, makeServiceReq(ServiceOpType::HEARTBEAT_PROVIDER, "echo", HostInfo("10.0.0.2", 8080), 0));
    auto resp_bad = std::dynamic_pointer_cast<ServiceResponse>(bad->sent[0]);
    EXPECT_EQ(resp_bad->rcode(), RespCode::SERVICE_NOT_FOUND);
}

TEST(PwithDManagerTest, UnknownOpTypeReturnsInvalid)
{
    auto store = std::make_shared<MemoryRegistryStore>();
    PwithDManager mgr(store);
    auto conn = std::make_shared<RecordingConnection>();

    mgr.onserviceRequest(conn, makeServiceReq(ServiceOpType::UNKNOWN, "echo", HostInfo("10.0.0.1", 8080), 0));

    auto resp = std::dynamic_pointer_cast<ServiceResponse>(conn->sent[0]);
    ASSERT_NE(resp, nullptr);
    EXPECT_EQ(resp->rcode(), RespCode::INVALID_OPTYPE);
}

// 注册 provider 时，向关注该 method 的 discoverer 广播 ONLINE
TEST(PwithDManagerTest, RegisterNotifiesOnlineDiscoverers)
{
    auto store = std::make_shared<MemoryRegistryStore>();
    PwithDManager mgr(store);

    auto discoverer = std::make_shared<RecordingConnection>();
    mgr.onserviceRequest(discoverer, makeServiceReq(ServiceOpType::DISCOVER, "echo", HostInfo(), 0));

    auto provider = std::make_shared<RecordingConnection>();
    mgr.onserviceRequest(provider, makeServiceReq(ServiceOpType::REGISTER, "echo", HostInfo("10.0.0.1", 8080), 5));

    ASSERT_EQ(discoverer->sent.size(), 2u); // discover 响应 + online 通知
    auto notify = std::dynamic_pointer_cast<ServiceRequest>(discoverer->sent[1]);
    ASSERT_NE(notify, nullptr);
    EXPECT_EQ(notify->optype(), ServiceOpType::ONLINE);
    EXPECT_EQ(notify->method(), "echo");
    EXPECT_EQ(notify->host(), HostInfo("10.0.0.1", 8080));
}

// sweep 过期 provider 时，向 discoverer 广播 OFFLINE
TEST(PwithDManagerTest, SweepNotifiesOffline)
{
    auto store = std::make_shared<MemoryRegistryStore>();
    PwithDManager mgr(store);

    auto discoverer = std::make_shared<RecordingConnection>();
    mgr.onserviceRequest(discoverer, makeServiceReq(ServiceOpType::DISCOVER, "echo", HostInfo(), 0));

    auto provider = std::make_shared<RecordingConnection>();
    HostInfo host("10.0.0.1", 8080);
    mgr.onserviceRequest(provider, makeServiceReq(ServiceOpType::REGISTER, "echo", host, 5));

    std::this_thread::sleep_for(std::chrono::milliseconds(5)); // 让 lastheartbeat 过期

    auto expired = mgr.sweepAndNotify(0);
    ASSERT_EQ(expired.size(), 1u);

    ASSERT_EQ(discoverer->sent.size(), 3u); // discover 响应 + online 通知 + offline 通知
    auto notify = std::dynamic_pointer_cast<ServiceRequest>(discoverer->sent[2]);
    ASSERT_NE(notify, nullptr);
    EXPECT_EQ(notify->optype(), ServiceOpType::OFFLINE);
    EXPECT_EQ(notify->host(), host);
}
