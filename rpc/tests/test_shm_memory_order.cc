// =============================================================================
// test_shm_memory_order.cc — SHM ring 的内存序验证（跨线程 + 跨进程）
// -----------------------------------------------------------------------------
// 为什么单独有这一组：
//   ring 的正确性依赖两对 release/acquire（生产者发布 write_idx、消费者发布
//   read_idx）。x86 的 store-store / load-load 天然有序，把任一 store 改成
//   relaxed，功能测试照样全绿 —— 「跑通了」不能证明内存序对。
//
// 这里给两种互补的检查：
//   1) 同映射双线程：生产/消费者用同一份 _ctrl 与数据区，TSan 能看到两边
//      的访问并据此检查 happens-before。正确实现 TSan 干净；把发布改成
//      relaxed，TSan 立即在 body 上报 data race。
//      跑法：cmake -DLCZ_RPC_TSAN=ON 单独建目录，再跑本文件。
//   2) 真跨进程（fork 后子进程自己 open）：两次映射、两个地址空间，双向
//      往返，逐帧校验序号 + 长度 + 每个字节 + 校验和。写侧提前发布（先挪
//      write_idx 再拷贝 body）会读回撕裂帧，本用例能抓到。
// =============================================================================

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <sys/wait.h>
#include <signal.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "src/general/shm_channel.hpp"

using lcz_rpc::MsgType;
using lcz_rpc::ShmChannel;

namespace
{
    constexpr size_t RING_SIZE = 4096;

    // 帧内容完全由 seq 决定：长度、每个字节、校验和都能被对端独立重算，
    // 因此「读到的字节与重算结果不一致」只可能是发布顺序错了（撕裂读）。
    // 布局：[8B seq][8B 校验和][payload]
    std::string make_body(uint64_t seq)
    {
        const size_t payload_len = 8 + (seq * 37) % 600;
        std::string body(16 + payload_len, '\0');
        memcpy(body.data(), &seq, 8);

        uint64_t sum = 0;
        for (size_t i = 0; i < payload_len; ++i)
        {
            char c = static_cast<char>((seq * 131 + i * 17) & 0xFF);
            body[16 + i] = c;
            sum += static_cast<unsigned char>(c);
        }
        memcpy(body.data() + 8, &sum, 8);
        return body;
    }

    bool check_body(const std::string &body, uint64_t seq)
    {
        const size_t payload_len = 8 + (seq * 37) % 600;
        if (body.size() != 16 + payload_len)
            return false;

        uint64_t got_seq = 0;
        memcpy(&got_seq, body.data(), 8);
        if (got_seq != seq)
            return false;

        uint64_t got_sum = 0;
        memcpy(&got_sum, body.data() + 8, 8);

        uint64_t calc = 0;
        for (size_t i = 0; i < payload_len; ++i)
        {
            char expect = static_cast<char>((seq * 131 + i * 17) & 0xFF);
            if (body[16 + i] != expect)
                return false;
            calc += static_cast<unsigned char>(body[16 + i]);
        }
        return calc == got_sum;
    }

    std::string unique_name(const char *tag)
    {
        return std::string("/lcz_shm_mo_") + tag + "_" + std::to_string(getpid());
    }

    using Clock = std::chrono::steady_clock;
}

// -----------------------------------------------------------------------------
// §1 同映射双线程：TSan 下检查发布是否真的建立了 happens-before
// -----------------------------------------------------------------------------
// 生产者交替走两条发送路径（write_request 与 req_write_ptr/req_commit），
// 两条都在环形区边界上覆盖到跳过帧；消费者逐帧校验。
TEST(ShmMemoryOrderTest, TwoThreadsSameMappingPreserveFrames)
{
    constexpr uint64_t COUNT = 20000;
    const std::string name = unique_name("same_map");
    shm_unlink(name.c_str());

    ShmChannel ch;
    ASSERT_TRUE(ch.create(name, RING_SIZE, RING_SIZE));

    std::atomic<bool> stop{false};
    std::atomic<bool> producer_failed{false};
    std::atomic<uint64_t> produced{0};

    std::thread producer([&] {
        for (uint64_t seq = 0; seq < COUNT; ++seq)
        {
            if (stop.load(std::memory_order_relaxed))
                return;
            const std::string body = make_body(seq);
            bool ok = false;
            if (seq % 2 == 0)
            {
                // 普通路径：内部 memcpy 完三部分后 release 发布 write_idx
                ok = ch.write_request(body, MsgType::REQ_RPC_PROTO, 100);
            }
            else
            {
                // 零拷贝路径：body 直接写进 ring，req_commit 里 release 发布
                std::span<char> span = ch.req_write_ptr();
                if (span.size() >= body.size())
                {
                    memcpy(span.data(), body.data(), body.size());
                    ch.req_commit(body.size(), MsgType::REQ_RPC_PROTO);
                    ok = true;
                }
                else
                {
                    // 尾部连续空间放不下：交给普通路径写跳过帧
                    ok = ch.write_request(body, MsgType::REQ_RPC_PROTO, 100);
                }
            }
            if (!ok)
            {
                producer_failed.store(true, std::memory_order_relaxed);
                return;
            }
            produced.store(seq + 1, std::memory_order_relaxed);
        }
    });

    bool intact = true;
    uint64_t first_bad = 0;
    uint64_t expected = 0;
    const auto deadline = Clock::now() + std::chrono::seconds(20);
    while (expected < COUNT)
    {
        if (producer_failed.load(std::memory_order_relaxed) || Clock::now() >= deadline)
            break;
        std::string body;
        MsgType type;
        if (!ch.read_request(body, type))
        {
            std::this_thread::yield();
            continue;
        }
        if (type != MsgType::REQ_RPC_PROTO || !check_body(body, expected))
        {
            intact = false;
            first_bad = expected;
            break;
        }
        ++expected;
    }
    stop.store(true, std::memory_order_relaxed);
    producer.join();

    EXPECT_FALSE(producer_failed.load()) << "producer stopped at seq " << produced.load();
    EXPECT_TRUE(intact) << "first corrupted frame at seq " << first_bad;
    EXPECT_EQ(expected, COUNT);
    EXPECT_EQ(produced.load(), COUNT);

    ch.destroy();
    shm_unlink(name.c_str());
}

// -----------------------------------------------------------------------------
// §2 真跨进程：两次映射、两个地址空间，双向往返
// -----------------------------------------------------------------------------
// 父进程 create 后 fork；子进程自己 open（独立页表），消费请求并原样回显响应。
// 父进程逐条 ping-pong，避免把 ring 灌满。逐帧校验内容和顺序。
TEST(ShmMemoryOrderTest, CrossProcessBothDirections)
{
    constexpr uint64_t COUNT = 10000;
    const std::string name = unique_name("xproc");
    shm_unlink(name.c_str());

    ShmChannel parent;
    ASSERT_TRUE(parent.create(name, RING_SIZE, RING_SIZE));

    pid_t pid = fork();
    ASSERT_GE(pid, 0);

    if (pid == 0)
    {
        // 子进程：不复用父进程的 ShmChannel 对象，自己 open 一份新映射
        ShmChannel child;
        if (!child.open(name))
            _exit(2);

        uint64_t expected = 0;
        const auto deadline = Clock::now() + std::chrono::seconds(20);
        while (expected < COUNT)
        {
            std::string body;
            MsgType type;
            if (!child.read_request(body, type))
            {
                if (Clock::now() >= deadline)
                    _exit(3); // 超时：父进程没把数据送过来
                std::this_thread::yield();
                continue;
            }
            if (type != MsgType::REQ_RPC_PROTO || !check_body(body, expected))
                _exit(4); // 读回撕裂帧 / 顺序错
            if (!child.write_response(body, MsgType::RSP_RPC_PROTO, 500))
                _exit(5); // 响应 ring 写不进去
            ++expected;
        }
        _exit(0);
    }

    bool intact = true;
    uint64_t first_bad = 0;
    for (uint64_t seq = 0; seq < COUNT && intact; ++seq)
    {
        if (!parent.write_request(make_body(seq), MsgType::REQ_RPC_PROTO, 1000))
        {
            intact = false;
            first_bad = seq;
            break;
        }

        bool got = false;
        const auto deadline = Clock::now() + std::chrono::seconds(5);
        while (Clock::now() < deadline)
        {
            std::string body;
            MsgType type;
            if (!parent.read_response(body, type))
            {
                std::this_thread::yield();
                continue;
            }
            got = true;
            if (type != MsgType::RSP_RPC_PROTO || !check_body(body, seq))
            {
                intact = false;
                first_bad = seq;
            }
            break;
        }
        if (!got)
        {
            intact = false;
            first_bad = seq;
        }
    }

    // 父进程提前失败时子进程还在自旋，直接收掉，避免等它的 20s 超时
    if (!intact)
        kill(pid, SIGKILL);

    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);

    EXPECT_TRUE(intact) << "first bad seq " << first_bad;
    EXPECT_TRUE(WIFEXITED(status)) << "child status=" << status;
    EXPECT_EQ(WEXITSTATUS(status), 0);

    parent.destroy();
    shm_unlink(name.c_str());
}

// 子进程自行 open 时，父进程 create 写的 magic/version/data_size 必须跨进程可见
// （ready 的 release/acquire 是这条链上唯一的同步点）。
// 子进程连不上或读不到约定布局就以非 0 退出，父进程据此断言。
TEST(ShmMemoryOrderTest, CrossProcessSeesPublishedLayout)
{
    const std::string name = unique_name("layout");
    shm_unlink(name.c_str());

    ShmChannel parent;
    ASSERT_TRUE(parent.create(name, 8192, 16384));

    pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0)
    {
        ShmChannel child;
        if (!child.open(name))
            _exit(2);
        // data_size 是 create 里在 ready.store(release) 之前写的普通字段，
        // 子进程在 ready.load(acquire) 之后读到必须就是 8192/16384
        if (child.req_capacity() != 8192 || child.resp_capacity() != 16384)
            _exit(3);
        if (!child.peerAlive())
            _exit(4);
        _exit(0);
    }

    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    EXPECT_TRUE(WIFEXITED(status)) << "child status=" << status;
    EXPECT_EQ(WEXITSTATUS(status), 0);

    parent.destroy();
    shm_unlink(name.c_str());
}
