// =============================================================================
// test_shm_channel.cc — 共享内存 Transport 单测
// -----------------------------------------------------------------------------
// 总测什么：create/open 生命周期、单帧读写往返、双通道不串扰、
//           跳过帧边界 wrap、大消息、空读返回 false、反复销毁重建不残留。
// 不测什么：跨进程（Phase 2）、eventfd 通知（Phase 3）、多线程竞态。
// =============================================================================

#include <gtest/gtest.h>
#include <cstring>
#include <memory>
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

// 创建者已退出的残留段：create 回收后重建，而不是永久失败
TEST_F(ShmChannelTest, StaleSegmentWithDeadCreatorIsReclaimed) {
    const char *name = "/test_shm_stale";
    shm_unlink(name);
    const size_t total = lcz_rpc::SHM_CTRL_SIZE + 64 + 64;

    int fd = shm_open(name, O_CREAT | O_RDWR | O_EXCL, S_IRUSR | S_IWUSR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, total), 0);
    void *addr = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_NE(addr, MAP_FAILED);
    auto *ctrl = new (addr) lcz_rpc::ShmControl();
    ctrl->creator_pid.store(INT32_MAX); // 不存在的 pid
    ctrl->magic.store(lcz_rpc::SHM_MAGIC);
    ctrl->version.store(lcz_rpc::SHM_VERSION);
    ctrl->ready.store(true, std::memory_order_release);
    munmap(addr, total);
    close(fd);

    ShmChannel ch;
    ASSERT_TRUE(ch.create(name, 64, 64));
    ASSERT_TRUE(ch.write_request("reclaimed", MsgType::REQ_RPC_PROTO));
    std::string out; MsgType t;
    ASSERT_TRUE(ch.read_request(out, t));
    EXPECT_EQ(out, "reclaimed");

    ch.destroy();
    shm_unlink(name);
}

// 旧格式残留（没有 pid 字段，magic/version 为 0）：ready 后布局不兼容，同样回收
TEST_F(ShmChannelTest, OldFormatSegmentIsReclaimed) {
    const char *name = "/test_shm_old_format";
    shm_unlink(name);
    const size_t total = lcz_rpc::SHM_CTRL_SIZE + 64 + 64;

    int fd = shm_open(name, O_CREAT | O_RDWR | O_EXCL, S_IRUSR | S_IWUSR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, total), 0);
    void *addr = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_NE(addr, MAP_FAILED);
    auto *ctrl = new (addr) lcz_rpc::ShmControl();
    ctrl->ready.store(true, std::memory_order_release); // pid/magic/version 保持 0
    munmap(addr, total);
    close(fd);

    ShmChannel ch;
    ASSERT_TRUE(ch.create(name, 64, 64));
    ch.destroy();
    shm_unlink(name);
}

// 残留段 ready=true 但魔数不对：open 拒绝，不按错误布局解析
TEST_F(ShmChannelTest, OpenRejectsInvalidMagic) {
    const char *name = "/test_shm_bad_magic";
    shm_unlink(name);
    const size_t total = lcz_rpc::SHM_CTRL_SIZE + 64 + 64;

    int fd = shm_open(name, O_CREAT | O_RDWR | O_EXCL, S_IRUSR | S_IWUSR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, total), 0);
    void *addr = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_NE(addr, MAP_FAILED);
    auto *ctrl = new (addr) lcz_rpc::ShmControl();
    ctrl->creator_pid.store(1);
    ctrl->req_channel.data_size = 64;
    ctrl->resp_channel.data_size = 64;
    ctrl->ready.store(true, std::memory_order_release); // magic/version 保持 0
    munmap(addr, total);
    close(fd);

    ShmChannel ch;
    EXPECT_FALSE(ch.open(name));
    shm_unlink(name);
}

// 版本不匹配同样拒绝
TEST_F(ShmChannelTest, OpenRejectsVersionMismatch) {
    const char *name = "/test_shm_bad_version";
    shm_unlink(name);
    const size_t total = lcz_rpc::SHM_CTRL_SIZE + 64 + 64;

    int fd = shm_open(name, O_CREAT | O_RDWR | O_EXCL, S_IRUSR | S_IWUSR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, total), 0);
    void *addr = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_NE(addr, MAP_FAILED);
    auto *ctrl = new (addr) lcz_rpc::ShmControl();
    ctrl->magic.store(lcz_rpc::SHM_MAGIC);
    ctrl->version.store(lcz_rpc::SHM_VERSION + 1);
    ctrl->req_channel.data_size = 64;
    ctrl->resp_channel.data_size = 64;
    ctrl->ready.store(true, std::memory_order_release);
    munmap(addr, total);
    close(fd);

    ShmChannel ch;
    EXPECT_FALSE(ch.open(name));
    shm_unlink(name);
}

// 创建者迟迟不 ready：open 超时返回，而不是永久自旋
TEST_F(ShmChannelTest, OpenTimesOutWhenNotReady) {
    const char *name = "/test_shm_not_ready";
    shm_unlink(name);
    const size_t total = lcz_rpc::SHM_CTRL_SIZE + 64 + 64;

    int fd = shm_open(name, O_CREAT | O_RDWR | O_EXCL, S_IRUSR | S_IWUSR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, total), 0);
    void *addr = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_NE(addr, MAP_FAILED);
    auto *ctrl = new (addr) lcz_rpc::ShmControl();
    ctrl->creator_pid.store(static_cast<int32_t>(getpid()));
    ctrl->magic.store(lcz_rpc::SHM_MAGIC);
    ctrl->version.store(lcz_rpc::SHM_VERSION);
    ctrl->req_channel.data_size = 64;
    ctrl->resp_channel.data_size = 64;
    // ready 保持 false，模拟创建者初始化中途死亡
    munmap(addr, total);
    close(fd);

    ShmChannel ch;
    auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(ch.open(name, 50));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    EXPECT_LT(elapsed, 1000);

    shm_unlink(name);
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

// 尾部只剩 1–3 字节：生产者写不下完整 4B 跳过标记，只清零残尾。
// 消费者必须跳过残尾，不能按 4B 读 frame_len（请求方向会读进响应区）
TEST_F(ShmChannelTest, PartialSkipFrameAtTailRequest) {
    static const size_t SZ = 64;
    for (size_t tail = 1; tail <= 3; ++tail) {
        SCOPED_TRACE(tail);
        std::string name = "/test_shm_partial_req_" + std::to_string(tail);
        shm_unlink(name.c_str());

        ShmChannel ch;
        ASSERT_TRUE(ch.create(name, SZ, SZ));

        // 响应区写入非零数据：旧实现越界读到它，会把垃圾当 frame_len
        ASSERT_TRUE(ch.write_response("XYZ", MsgType::RSP_RPC_PROTO));

        // frame_len = SZ - tail → write_idx 停在尾部剩 tail 字节处
        std::string aligner(SZ - 8 - tail, 'A');
        ASSERT_TRUE(ch.write_request(aligner, MsgType::REQ_RPC_PROTO));
        std::string tmp; MsgType t;
        ASSERT_TRUE(ch.read_request(tmp, t));

        // 触发 wrap：生产者写部分跳过标记，再从头部写新帧
        ASSERT_TRUE(ch.write_request("x", MsgType::REQ_RPC_PROTO));
        ASSERT_TRUE(ch.read_request(tmp, t));
        EXPECT_EQ(tmp, "x");
        EXPECT_EQ(t, MsgType::REQ_RPC_PROTO);

        ch.destroy();
        shm_unlink(name.c_str());
    }
}

// 响应方向同理：总大小 8192 时响应区末尾正好是页边界，
// 按 4B 越界读会跨进未映射页（SIGSEGV）
TEST_F(ShmChannelTest, PartialSkipFrameAtTailResponse) {
    static const size_t SZ = 2048; // 4096 + 2048 + 2048 = 8192
    shm_unlink("/test_shm_partial_resp");

    ShmChannel ch;
    ASSERT_TRUE(ch.create("/test_shm_partial_resp", SZ, SZ));

    std::string aligner(SZ - 8 - 1, 'A'); // frame_len = SZ - 1，尾巴剩 1 字节
    ASSERT_TRUE(ch.write_response(aligner, MsgType::RSP_RPC_PROTO));
    std::string tmp; MsgType t;
    ASSERT_TRUE(ch.read_response(tmp, t));

    ASSERT_TRUE(ch.write_response("x", MsgType::RSP_RPC_PROTO));
    ASSERT_TRUE(ch.read_response(tmp, t));
    EXPECT_EQ(tmp, "x");
    EXPECT_EQ(t, MsgType::RSP_RPC_PROTO);

    ch.destroy();
    shm_unlink("/test_shm_partial_resp");
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

// send() 将消息转发到注入的 _sender 回调，并把回调结果原样返回
TEST_F(ShmChannelTest, ShmConnectionSendDelegates) {
    lcz_rpc::ShmConnection conn;
    bool called = false;
    bool send_ok = true;
    conn.setSender([&](const lcz_rpc::BaseMessage::ptr&) { called = true; return send_ok; });

    // 构造一个最简单的消息
    auto msg = lcz_rpc::MessageFactory::create(lcz_rpc::MsgType::REQ_RPC);
    EXPECT_TRUE(conn.send(msg));
    EXPECT_TRUE(called);

    // 底层写失败必须冒泡，调用方才能 fail fast
    send_ok = false;
    EXPECT_FALSE(conn.send(msg));
}

// 新建连接默认存活；传输层标记断开后 connected() 立即变 false
TEST_F(ShmChannelTest, ShmConnectionTracksLiveness) {
    lcz_rpc::ShmConnection conn;
    EXPECT_TRUE(conn.connected());
    conn.markDead();
    EXPECT_FALSE(conn.connected());
}

// 创建者就是本进程：peerAlive() 为 true
TEST_F(ShmChannelTest, PeerAliveForOwnSegment) {
    const char *name = "/test_shm_peer_alive";
    shm_unlink(name);
    ShmChannel ch;
    ASSERT_TRUE(ch.create(name, 64, 64));
    EXPECT_TRUE(ch.peerAlive());
    ch.destroy();
}

// 创建者进程已不存在（kill(pid,0)=ESRCH）：peerAlive() 为 false
TEST_F(ShmChannelTest, PeerGoneWhenCreatorPidDead) {
    const char *name = "/test_shm_peer_dead";
    shm_unlink(name);
    const size_t total = lcz_rpc::SHM_CTRL_SIZE + 64 + 64;
    int fd = shm_open(name, O_CREAT | O_RDWR | O_EXCL, S_IRUSR | S_IWUSR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, total), 0);
    void *addr = mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ASSERT_NE(addr, MAP_FAILED);
    auto *ctrl = new (addr) lcz_rpc::ShmControl();
    ctrl->magic.store(lcz_rpc::SHM_MAGIC);
    ctrl->version.store(lcz_rpc::SHM_VERSION);
    ctrl->creator_pid.store(INT32_MAX); // 不存在的 pid
    ctrl->req_channel.data_size = 64;
    ctrl->resp_channel.data_size = 64;
    ctrl->ready.store(true, std::memory_order_release);
    munmap(addr, total);
    close(fd);

    ShmChannel ch;
    ASSERT_TRUE(ch.open(name));
    EXPECT_FALSE(ch.peerAlive());
    ch.destroy();
    shm_unlink(name);
}

// 创建者 destroy() 后 ready 被撤掉：已打开的对端立即看到断连
TEST_F(ShmChannelTest, PeerGoneAfterCreatorDestroy) {
    const char *name = "/test_shm_peer_destroy";
    shm_unlink(name);
    ShmChannel server, client;
    ASSERT_TRUE(server.create(name, 64, 64));
    ASSERT_TRUE(client.open(name));
    EXPECT_TRUE(client.peerAlive());

    server.destroy();
    EXPECT_FALSE(client.peerAlive());
    client.destroy();
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

// Server 退出后，Client 后台线程在 500ms 轮询里发现创建者消失并把连接标死
TEST_F(ShmChannelTest, ClientMarksDeadAfterServerDestroyed) {
    auto server = std::make_unique<lcz_rpc::ShmServer>(
        "lcz_shm_test_notify_gone", "lcz_shm_test_gone", 1024*1024, 1024*1024, 16, 1);
    server->setMessageCallback([](const lcz_rpc::BaseConnection::ptr&, lcz_rpc::BaseMessage::ptr&) {});
    std::thread server_thread([&]() { server->start(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    lcz_rpc::ShmClient client("lcz_shm_test_notify_gone");
    client.connect();
    ASSERT_TRUE(client.connected());

    server->stop();
    server_thread.join();
    server.reset(); // 析构里 destroy 所有通道：ready 置 false

    bool dead = false;
    for (int i = 0; i < 30 && !dead; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        dead = !client.connected();
    }
    EXPECT_TRUE(dead);
}
