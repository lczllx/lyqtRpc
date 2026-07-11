// =============================================================================
// test_shm_channel.cc — 共享内存 Transport 单测
// -----------------------------------------------------------------------------
// 总测什么：create/open 生命周期、单帧读写往返、双通道不串扰、
//           跳过帧边界 wrap、大消息、空读返回 false、反复销毁重建不残留。
// 不测什么：跨进程（Phase 2）、eventfd 通知（Phase 3）、多线程竞态。
// =============================================================================

#include <gtest/gtest.h>
#include <cstring>
#include <thread>
#include <chrono>

#include "src/general/shm_channel.hpp"
#include "src/server/shm_server.hpp"
#include "src/client/shm_client.hpp"
#include "src/general/shm_connection.hpp"
#include "src/general/message.hpp"

using lcz_rpc::ShmChannel;
using lcz_rpc::MsgType;

class ShmChannelTest : public ::testing::Test {
protected:
    void SetUp() override {
        shm_unlink("/test_shm_gtest");
        shm_unlink("/lcz_shm_test");
    }
    void TearDown() override {
        shm_unlink("/test_shm_gtest");
        shm_unlink("/lcz_shm_test");
    }
};

// -----------------------------------------------------------------------------
// §1 生命周期
// -----------------------------------------------------------------------------

// create + destroy：共享内存文件在 /dev/shm 下创建和销毁
TEST_F(ShmChannelTest, CreateAndDestroy) {
    ShmChannel ch;
    ASSERT_TRUE(ch.create("/test_shm_gtest", 1024, 1024));
    EXPECT_TRUE(ch.is_open());
    ch.destroy();
    EXPECT_FALSE(ch.is_open());
    // 确认文件已删除
    int fd = shm_open("/test_shm_gtest", O_RDWR, 0);
    EXPECT_LT(fd, 0);
}

// open 不存在的文件 → 返回 false
TEST_F(ShmChannelTest, OpenNonExistent) {
    ShmChannel ch;
    EXPECT_FALSE(ch.open("/test_shm_gtest_nonexistent"));
}

// 重复 create 同一个名字 → 第二次失败（O_EXCL）
TEST_F(ShmChannelTest, DoubleCreateFails) {
    ShmChannel ch1, ch2;
    ASSERT_TRUE(ch1.create("/test_shm_gtest", 1024, 1024));
    EXPECT_FALSE(ch2.create("/test_shm_gtest", 1024, 1024)); // 防多 Server
    ch1.destroy();
}

// 重复 open 同一个通道 → 第二次失败（_addr 已非空）
TEST_F(ShmChannelTest, DoubleOpenFails) {
    ShmChannel ch1, ch2;
    ASSERT_TRUE(ch1.create("/test_shm_gtest", 1024, 1024));
    ASSERT_TRUE(ch2.open("/test_shm_gtest"));
    EXPECT_FALSE(ch2.open("/test_shm_gtest"));
    ch1.destroy();
    ch2.destroy();
}

// -----------------------------------------------------------------------------
// §2 单帧读写往返
// -----------------------------------------------------------------------------

// 写一帧 → 同一进程读回 → 数据一致
TEST_F(ShmChannelTest, SingleFrameRoundtrip) {
    ShmChannel ch;
    ASSERT_TRUE(ch.create("/test_shm_gtest", 4096, 4096));

    std::string msg = "hello shared memory";
    ASSERT_TRUE(ch.write_request(msg, MsgType::REQ_RPC_PROTO));

    std::string out; MsgType type;
    ASSERT_TRUE(ch.read_request(out, type));
    EXPECT_EQ(out, msg);
    EXPECT_EQ(type, MsgType::REQ_RPC_PROTO);

    ch.destroy();
}

// 请求 + 响应两个方向各写各读，互不串扰
TEST_F(ShmChannelTest, DualChannelNoCrossTalk) {
    ShmChannel ch;
    ASSERT_TRUE(ch.create("/test_shm_gtest", 4096, 4096));

    ASSERT_TRUE(ch.write_request("req1", MsgType::REQ_RPC_PROTO));
    ASSERT_TRUE(ch.write_response("resp1", MsgType::RSP_RPC_PROTO));

    std::string body; MsgType t;

    ASSERT_TRUE(ch.read_request(body, t));
    EXPECT_EQ(body, "req1");
    EXPECT_EQ(t, MsgType::REQ_RPC_PROTO);

    ASSERT_TRUE(ch.read_response(body, t));
    EXPECT_EQ(body, "resp1");
    EXPECT_EQ(t, MsgType::RSP_RPC_PROTO);

    ch.destroy();
}

// 空读返回 false（不阻塞）
TEST_F(ShmChannelTest, ReadEmptyReturnsFalse) {
    ShmChannel ch;
    ASSERT_TRUE(ch.create("/test_shm_gtest", 4096, 4096));

    std::string body; MsgType t;
    EXPECT_FALSE(ch.read_request(body, t));
    EXPECT_FALSE(ch.read_response(body, t));

    ch.destroy();
}

// -----------------------------------------------------------------------------
// §3 跳过帧边界：消息正好卡在 buffer 尾部
// -----------------------------------------------------------------------------

// buffer 很小，故意用一个填满大部分空间的负载，再写一帧触发 skip
TEST_F(ShmChannelTest, WrapAroundWithSkipFrame) {
    // 控制区 4KB + 请求区 64B + 响应区 64B（极小，方便触发 wrap）
    static const size_t REQ_SIZE = 64;
    ShmChannel ch;
    ASSERT_TRUE(ch.create("/test_shm_gtest", REQ_SIZE, 64));

    // 先写一帧占位但不消费 → read_idx 停在 0
    std::string aligner(REQ_SIZE - 8 - 4, 'A');  // 尽量占满 64B，留 4B 给跳过帧
    ASSERT_TRUE(ch.write_request(aligner, MsgType::REQ_RPC_PROTO));

    // 消费掉
    std::string tmp; MsgType t;
    ASSERT_TRUE(ch.read_request(tmp, t));

    // 现在 write_idx 应该在 buffer 末尾附近。再写一帧正常长度 →
    // 应该触发跳过帧并正常写入
    ASSERT_TRUE(ch.write_request("wrap_test", MsgType::REQ_RPC_PROTO));
    ASSERT_TRUE(ch.read_request(tmp, t));
    EXPECT_EQ(tmp, "wrap_test");

    ch.destroy();
}

// -----------------------------------------------------------------------------
// §4 边界条件
// -----------------------------------------------------------------------------

// body 为空串 → 帧头 8 字节 + 空 body
TEST_F(ShmChannelTest, EmptyBody) {
    ShmChannel ch;
    ASSERT_TRUE(ch.create("/test_shm_gtest", 4096, 4096));
    ASSERT_TRUE(ch.write_request("", MsgType::REQ_RPC_PROTO));

    std::string out; MsgType t;
    ASSERT_TRUE(ch.read_request(out, t));
    EXPECT_EQ(out, "");
    EXPECT_EQ(t, MsgType::REQ_RPC_PROTO);
    ch.destroy();
}

// 多条消息按序读出不乱
TEST_F(ShmChannelTest, MultipleFramesOrdered) {
    ShmChannel ch;
    ASSERT_TRUE(ch.create("/test_shm_gtest", 4096, 4096));

    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(ch.write_request(std::to_string(i), MsgType::REQ_RPC_PROTO));
    }
    for (int i = 0; i < 10; ++i) {
        std::string out; MsgType t;
        ASSERT_TRUE(ch.read_request(out, t));
        EXPECT_EQ(out, std::to_string(i));
    }
    ch.destroy();
}

// -----------------------------------------------------------------------------
// §5 单进程 Server/Client 模拟
// -----------------------------------------------------------------------------

// 同一进程内 create + open 两个 ShmChannel → 模拟跨进程读写
TEST_F(ShmChannelTest, SimulatedCrossProcess) {
    ShmChannel server, client;
    ASSERT_TRUE(server.create("/test_shm_gtest", 1024 * 1024, 1024 * 1024));
    ASSERT_TRUE(client.open("/test_shm_gtest"));

    // Client → Server（请求方向）
    ASSERT_TRUE(client.write_request("req_from_client", MsgType::REQ_RPC_PROTO));
    std::string body; MsgType t;
    ASSERT_TRUE(server.read_request(body, t));
    EXPECT_EQ(body, "req_from_client");

    // Server → Client（响应方向）
    ASSERT_TRUE(server.write_response("resp_from_server", MsgType::RSP_RPC_PROTO));
    ASSERT_TRUE(client.read_response(body, t));
    EXPECT_EQ(body, "resp_from_server");

    server.destroy();
}

// =============================================================================
// §6 ShmConnection：虚拟连接句柄
// =============================================================================

// send() 将消息转发到注入的 _sender 回调
TEST_F(ShmChannelTest, ShmConnectionSendDelegates) {
    lcz_rpc::ShmConnection conn;
    bool called = false;
    conn.setSender([&](const lcz_rpc::BaseMessage::ptr&) { called = true; });

    // 构造一个最简单的消息
    auto msg = lcz_rpc::MessageFactory::create(lcz_rpc::MsgType::REQ_RPC);
    conn.send(msg);
    EXPECT_TRUE(called);
}

// connected() 始终为 true（SHM 通道打开即视为连接）
TEST_F(ShmChannelTest, ShmConnectionAlwaysConnected) {
    lcz_rpc::ShmConnection conn;
    EXPECT_TRUE(conn.connected());
}

// peerAddress() 返回 shm:// 前缀的标识符
TEST_F(ShmChannelTest, ShmConnectionPeerAddress) {
    lcz_rpc::ShmConnection conn;
    conn.setName("test_conn");
    EXPECT_EQ(conn.peerAddress(), "shm://test_conn");
}

// =============================================================================
// §7 ShmServer / ShmClient：完整 RPC 往返
// =============================================================================

// Server start + Client connect → send 请求 → Server 处理 → 回响应 → Client 收到
TEST_F(ShmChannelTest, ServerClientRoundtrip) {
    lcz_rpc::ShmServer server("lcz_shm_test_notify", "lcz_shm_test", 1024*1024, 1024*1024, 16, 1);

    bool handled = false;
    std::mutex mtx;
    std::condition_variable cv;
    int result = 0;
    bool got_response = false;

    server.setMessageCallback([&](const lcz_rpc::BaseConnection::ptr& conn,
                                   lcz_rpc::BaseMessage::ptr& msg) {
        auto req = std::dynamic_pointer_cast<lcz_rpc::RpcRequest>(msg);
        ASSERT_NE(req, nullptr);
        EXPECT_EQ(req->method(), "add");

        int r = req->params()["num1"].asInt() + req->params()["num2"].asInt();
        auto resp = lcz_rpc::MessageFactory::create<lcz_rpc::RpcResponse>();
        resp->setId(req->rid());
        resp->setMsgType(lcz_rpc::MsgType::RSP_RPC);
        resp->setRcode(lcz_rpc::RespCode::SUCCESS);
        resp->setResult(r);
        conn->send(resp);
        handled = true;
    });

    // 后台线程跑 server（start 阻塞在 epoll 事件循环）
    std::thread server_thread([&]() { server.start(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Client 连接（后台 epoll 线程自动收响应）
    lcz_rpc::ShmClient client("lcz_shm_test_notify");
    client.setMessageCallback([&](const lcz_rpc::BaseConnection::ptr&,
                                   lcz_rpc::BaseMessage::ptr& msg) {
        auto resp = std::dynamic_pointer_cast<lcz_rpc::RpcResponse>(msg);
        if (resp && resp->rcode() == lcz_rpc::RespCode::SUCCESS) {
            std::lock_guard<std::mutex> lk(mtx);
            result = resp->result().asInt();
            got_response = true;
            cv.notify_one();
        }
    });
    client.connect();
    ASSERT_TRUE(client.connected());

    // 发请求
    auto req = lcz_rpc::MessageFactory::create<lcz_rpc::RpcRequest>();
    req->setId("test-001");
    req->setMsgType(lcz_rpc::MsgType::REQ_RPC);
    req->setMethod("add");
    Json::Value params;
    params["num1"] = 10;
    params["num2"] = 20;
    req->setParams(params);
    ASSERT_TRUE(client.send(req));

    // 等 Client 后台线程收到响应
    {
        std::unique_lock<std::mutex> lk(mtx);
        EXPECT_TRUE(cv.wait_for(lk, std::chrono::seconds(2), [&]{ return got_response; }));
    }
    EXPECT_EQ(result, 30);
    EXPECT_TRUE(handled);

    server.stop();
    server_thread.join();
}
