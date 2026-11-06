// =============================================================================
// test_shm_fault.cc — SHM 传输的故障注入用例
// -----------------------------------------------------------------------------
// 与 test_shm_channel.cc 的分工：那边用手工构造的共享内存段模拟错误状态
// （错误 magic、creator_pid 已死等），这里只制造真实进程故障：
//   对端崩溃：创建者被 SIGKILL，既不撤 ready 也不清理段
//   半写：    生产者拿到 write_ptr 写了一半就死，没有 commit（write_idx 不前进）
//   残留段：  崩溃进程留下的 ready=true 段，新 create 必须能回收并正常使用
//   队列满：  服务端 handler 阻塞不排空 → ShmClient::send 返回 BACKOFF，恢复后可用
//   notify 丢失：服务端写入响应但不写 eventfd，客户端靠 500ms 超时轮询仍能收到
// 内存序相关的故障（发布顺序、跨进程可见性）见 test_shm_memory_order.cc。
// =============================================================================

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <csignal>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include "src/general/shm_channel.hpp"
#include "src/general/message.hpp"
#include "src/server/shm_server.hpp"
#include "src/client/shm_client.hpp"

using lcz_rpc::MsgType;
using lcz_rpc::RpcError;
using lcz_rpc::ShmChannel;

namespace
{
    // 带 pid 的名字，避免 ctest 并行跑用例时互相踩段/踩 socket 文件
    std::string shm_name(const char *tag)
    {
        return std::string("/lcz_fault_") + tag + "_" + std::to_string(getpid());
    }

    std::string notify_path(const char *tag)
    {
        return "/tmp/lcz_fault_notify_" + std::string(tag) + "_" + std::to_string(getpid());
    }

    lcz_rpc::RpcRequest::ptr make_request(const std::string &id)
    {
        auto req = lcz_rpc::MessageFactory::create<lcz_rpc::RpcRequest>();
        req->setId(id);
        req->setMethod("add");
        Json::Value params;
        params["num1"] = 1;
        params["num2"] = 2;
        req->setParams(params);
        return req;
    }
}

// -----------------------------------------------------------------------------
// §1 创建者被杀：段无人清理，新 create 必须能回收（真实崩溃，不是合成 pid）
// -----------------------------------------------------------------------------
TEST(ShmFaultTest, CrashedCreatorSegmentIsReclaimed)
{
    const std::string name = shm_name("crash_reclaim");
    shm_unlink(name.c_str());

    pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0)
    {
        ShmChannel ch;
        if (!ch.create(name, 64 * 1024, 64 * 1024))
            _exit(2);
        // create() 返回说明 ready 已经置位；不走 destroy()，模拟断电式退出
        raise(SIGKILL);
        _exit(3);
    }

    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFSIGNALED(status)) << "child status=" << status;
    EXPECT_EQ(WTERMSIG(status), SIGKILL);

    // 残留段 ready 仍为 true、creator_pid 已不存在：回收后必须能正常收发
    ShmChannel ch;
    ASSERT_TRUE(ch.create(name, 64 * 1024, 64 * 1024));
    ASSERT_TRUE(ch.write_request("reclaimed-after-crash", MsgType::REQ_RPC_PROTO));
    std::string out;
    MsgType type;
    ASSERT_TRUE(ch.read_request(out, type));
    EXPECT_EQ(out, "reclaimed-after-crash");
    EXPECT_EQ(type, MsgType::REQ_RPC_PROTO);

    ch.destroy();
    shm_unlink(name.c_str());
}

// -----------------------------------------------------------------------------
// §2 已连接的客户端：创建者被 SIGKILL 后必须判定对端死亡
// -----------------------------------------------------------------------------
TEST(ShmFaultTest, OpenClientDetectsCreatorCrash)
{
    const std::string name = shm_name("peer_crash");
    shm_unlink(name.c_str());

    int ready_pipe[2];
    ASSERT_EQ(pipe(ready_pipe), 0);

    pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0)
    {
        close(ready_pipe[0]);
        ShmChannel ch;
        if (!ch.create(name, 64 * 1024, 64 * 1024))
            _exit(2);
        char c = 'R';
        if (write(ready_pipe[1], &c, 1) != 1)
            _exit(3);
        // 保持存活，等父进程发 SIGKILL；pause() 也会被信号打断但仍依赖父进程收尸
        for (;;)
            pause();
    }

    close(ready_pipe[1]);
    char c = 0;
    ASSERT_EQ(read(ready_pipe[0], &c, 1), 1);
    close(ready_pipe[0]);

    ShmChannel client;
    ASSERT_TRUE(client.open(name));
    EXPECT_TRUE(client.peerAlive());

    ASSERT_EQ(kill(pid, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFSIGNALED(status));

    // ready 没被撤掉，只能靠 creator_pid 的存活检查发现对端已死
    EXPECT_FALSE(client.peerAlive());

    client.destroy();
    shm_unlink(name.c_str());
}

// -----------------------------------------------------------------------------
// §3 半写：生产者写了 body 却没提交，对端不能看到撕裂帧，段还能继续用
// -----------------------------------------------------------------------------
TEST(ShmFaultTest, HalfWrittenFrameIsNotPublished)
{
    const std::string name = shm_name("half_write");
    shm_unlink(name.c_str());

    ShmChannel srv;
    ASSERT_TRUE(srv.create(name, 4096, 4096));

    pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0)
    {
        ShmChannel producer;
        if (!producer.open(name))
            _exit(2);
        // 零拷贝路径：body 直接写进 ring，commit 之前 write_idx 不前进。
        // 这里写满一半内容就退出，模拟发布前崩溃。
        std::span<char> span = producer.req_write_ptr();
        if (span.empty())
            _exit(3);
        const size_t n = std::min(span.size(), static_cast<size_t>(1024));
        memset(span.data(), 0xAB, n);
        _exit(0); // 不调用 req_commit()
    }

    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);

    // 未提交的半个帧不可见：read_request 必须返回 false 而不是读到 0xAB
    std::string body;
    MsgType type;
    EXPECT_FALSE(srv.read_request(body, type));

    // 生产者位置没动，后续帧直接覆盖这块残留并正常发布
    ASSERT_TRUE(srv.write_request("after-half-write", MsgType::REQ_RPC_PROTO));
    ASSERT_TRUE(srv.read_request(body, type));
    EXPECT_EQ(body, "after-half-write");
    EXPECT_EQ(type, MsgType::REQ_RPC_PROTO);

    srv.destroy();
    shm_unlink(name.c_str());
}

// -----------------------------------------------------------------------------
// §4 notify 丢失（响应方向）：服务端写进 ring 但不写 eventfd，客户端仍要收到
// -----------------------------------------------------------------------------
// 用真实 ShmClient + 一个只做握手的假服务端，避免为了丢通知去改生产代码。
TEST(ShmFaultTest, ClientReceivesResponseWithoutNotify)
{
    const std::string name = shm_name("lost_notify");
    const std::string path = notify_path("lost_notify");
    shm_unlink(name.c_str());
    unlink(path.c_str());

    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    ASSERT_GE(listen_fd, 0);
    struct sockaddr_un addr = {};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    ASSERT_EQ(bind(listen_fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)), 0);
    ASSERT_EQ(listen(listen_fd, 1), 0);
    // connect 失败时 accept 也要能超时返回，否则下面的 join 会永久挂住
    struct timeval tv = {5, 0};
    setsockopt(listen_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    ShmChannel srv;
    ASSERT_TRUE(srv.create(name, 64 * 1024, 64 * 1024));

    std::mutex mtx;
    std::condition_variable cv;
    bool got_response = false;
    int result = -1;
    std::atomic<bool> handshake_ok{false};

    std::thread accept_thread([&] {
        int conn_fd = ::accept(listen_fd, nullptr, nullptr);
        if (conn_fd < 0)
            return;
        int req_fd = -1, resp_fd = -1;
        handshake_ok.store(ShmChannel::handshake_server(conn_fd, req_fd, resp_fd, name));
        close(conn_fd);
        if (!handshake_ok.load())
            return;

        srv.set_req_notify_fd(req_fd);
        srv.set_resp_notify_fd(-1); // 模拟通知丢失：write_response 里 notify_resp() 变空操作

        auto resp = lcz_rpc::MessageFactory::create<lcz_rpc::RpcResponse>();
        resp->setId("lost-notify-1");
        resp->setMsgType(MsgType::RSP_RPC);
        resp->setRcode(lcz_rpc::RespCode::SUCCESS);
        resp->setResult(7);
        srv.write_response(resp->serialize(), MsgType::RSP_RPC);

        close(resp_fd);
    });

    lcz_rpc::ShmClient client(path);
    client.setMessageCallback([&](const lcz_rpc::BaseConnection::ptr &,
                                  lcz_rpc::BaseMessage::ptr &msg) {
        auto resp = std::dynamic_pointer_cast<lcz_rpc::RpcResponse>(msg);
        if (!resp)
            return;
        std::lock_guard<std::mutex> lk(mtx);
        result = resp->result().asInt();
        got_response = true;
        cv.notify_all();
    });
    client.connect();
    ASSERT_TRUE(client.connected());

    accept_thread.join();
    ASSERT_TRUE(handshake_ok.load());

    {
        std::unique_lock<std::mutex> lk(mtx);
        EXPECT_TRUE(cv.wait_for(lk, std::chrono::seconds(3), [&] { return got_response; }));
    }
    EXPECT_EQ(result, 7);

    client.shutdown();
    srv.destroy();
    close(listen_fd);
    shm_unlink(name.c_str());
    unlink(path.c_str());
}

// -----------------------------------------------------------------------------
// §5 队列满（端到端）：handler 阻塞不排空 → send 返回 BACKOFF，恢复后继续可用
// -----------------------------------------------------------------------------
TEST(ShmFaultTest, ClientSendReportsBackoffWhenRingFull)
{
    const std::string name = shm_name("full_ring");
    const std::string path = notify_path("full_ring");
    shm_unlink(name.c_str());
    unlink(path.c_str());

    // 小 ring + 单 worker：handler 阻塞时请求只会在 ring 里堆积
    lcz_rpc::ShmServer server(path, name, 4096, 4096, 16, 1);

    std::mutex mtx;
    std::condition_variable cv;
    bool entered = false;
    bool release = false;
    std::set<std::string> seen;

    server.setMessageCallback([&](const lcz_rpc::BaseConnection::ptr &,
                                  lcz_rpc::BaseMessage::ptr &msg) {
        auto req = std::dynamic_pointer_cast<lcz_rpc::RpcRequest>(msg);
        std::unique_lock<std::mutex> lk(mtx);
        if (req)
            seen.insert(req->rid());
        entered = true;
        cv.notify_all();
        // 第一个请求把 worker 卡住；放行后其余请求直接放行
        cv.wait(lk, [&] { return release; });
    });

    std::thread server_thread([&] { server.start(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    lcz_rpc::ShmClient client(path);
    client.connect();
    ASSERT_TRUE(client.connected());

    ASSERT_TRUE(client.send(make_request("first")));
    {
        std::unique_lock<std::mutex> lk(mtx);
        ASSERT_TRUE(cv.wait_for(lk, std::chrono::seconds(2), [&] { return entered; }));
    }

    // 服务端不排空：一直发到本地 ring 满，send 必须把本地过载报给调用方
    bool got_backoff = false;
    for (int i = 0; i < 100 && !got_backoff; ++i)
    {
        if (!client.send(make_request("flood-" + std::to_string(i))))
        {
            got_backoff = true;
            ASSERT_NE(client.connection(), nullptr);
            EXPECT_EQ(client.connection()->lastSendError(), RpcError::BACKOFF);
        }
    }
    EXPECT_TRUE(got_backoff) << "ring never filled after 100 sends";

    // 放行 handler：worker 排空后，同一个 client 必须能继续发送并被服务端处理
    {
        std::lock_guard<std::mutex> lk(mtx);
        release = true;
    }
    cv.notify_all();

    bool sent_after_release = false;
    for (int i = 0; i < 40 && !sent_after_release; ++i)
    {
        sent_after_release = client.send(make_request("after-release"));
        if (!sent_after_release)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_TRUE(sent_after_release);

    {
        std::unique_lock<std::mutex> lk(mtx);
        EXPECT_TRUE(cv.wait_for(lk, std::chrono::seconds(3), [&] {
            return seen.count("after-release") > 0;
        }));
    }

    client.shutdown();
    server.stop();
    server_thread.join();
    shm_unlink(name.c_str());
    unlink(path.c_str());
}
