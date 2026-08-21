// AsyncCircuitStore 单元测试：本地缓存命中/穿透、后台刷盘、同 key 覆盖、删除、析构兜底
#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <functional>
#include <memory>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_map>
#include "src/server/async_circuit_store.hpp"
#include "src/server/memory_circuit_store.hpp"
#include "src/server/circuit_store.hpp"

using lcz_rpc::server::AsyncCircuitStore;
using lcz_rpc::server::MemoryCircuitStore;
using lcz_rpc::server::ICircuitStateStore;
using lcz_rpc::CircuitStatus;
using lcz_rpc::CircuitState;

namespace
{
    // 轮询等待条件成立，最多 max_ms 毫秒，返回是否最终成立
    bool WaitUntil(const std::function<bool()> &cond, int max_ms = 2000)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(max_ms);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (cond())
                return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return cond();
    }

    // 测试用：统计底层 store 被调用的次数，验证缓存命中 / 穿透次数
    class SpyCircuitStore : public ICircuitStateStore
    {
    public:
        int load_count = 0;
        CircuitStatus load_value;

        bool save(std::string_view, std::string_view, const CircuitStatus &) override { return true; }
        CircuitStatus load(std::string_view, std::string_view) override { ++load_count; return load_value; }
        bool remove(std::string_view, std::string_view) override { return true; }
    };

    // 测试用：底层 save 阻塞在信号量上，让测试能精确卡在「swap 出批次、save 在途」
    // 的时间窗口里插入 remove，确定性地复现 remove 与在途刷盘的竞态。
    class BlockingSpyStore : public ICircuitStateStore
    {
    public:
        std::mutex m;
        std::condition_variable cv;
        std::atomic<bool> save_entered{false}; // worker 已进入 save（批次已 swap）
        bool allow = false;                    // 测试持锁置 true 放行 save
        std::unordered_map<std::string, CircuitStatus> data; // key = method:host

        bool save(std::string_view method, std::string_view host, const CircuitStatus &status) override
        {
            std::string k = std::string(method) + ":" + std::string(host);
            save_entered = true;
            std::unique_lock<std::mutex> lk(m);
            cv.wait(lk, [this] { return allow; });
            data[k] = status;
            return true;
        }
        CircuitStatus load(std::string_view method, std::string_view host) override
        {
            std::string k = std::string(method) + ":" + std::string(host);
            std::lock_guard<std::mutex> lk(m);
            auto it = data.find(k);
            return it == data.end() ? CircuitStatus{} : it->second;
        }
        bool remove(std::string_view method, std::string_view host) override
        {
            std::string k = std::string(method) + ":" + std::string(host);
            std::lock_guard<std::mutex> lk(m);
            return data.erase(k) > 0;
        }
    };
}

// save 同步写本地缓存，随后 load 立即命中缓存，与后台是否已刷盘无关
TEST(AsyncCircuitStoreTest, SaveImmediatelyVisibleThroughCache)
{
    auto underlying = std::make_shared<MemoryCircuitStore>();
    AsyncCircuitStore store(underlying, std::chrono::milliseconds(10));

    CircuitStatus s;
    s.state = CircuitState::OPEN;
    EXPECT_TRUE(store.save("echo", "h:1", s));

    EXPECT_EQ(store.load("echo", "h:1").state, CircuitState::OPEN);
}

// 缓存 miss 时穿透底层一次，并回填缓存，后续同 key 读取不再穿透
TEST(AsyncCircuitStoreTest, LoadMissReadsThroughAndBackfills)
{
    auto spy = std::make_shared<SpyCircuitStore>();
    spy->load_value.state = CircuitState::OPEN;
    AsyncCircuitStore store(spy, std::chrono::milliseconds(10));

    EXPECT_EQ(store.load("echo", "h:1").state, CircuitState::OPEN);
    EXPECT_EQ(spy->load_count, 1); // 首次穿透底层

    EXPECT_EQ(store.load("echo", "h:1").state, CircuitState::OPEN);
    EXPECT_EQ(spy->load_count, 1); // 命中缓存，不再穿透
}

// 后台线程把 save 的状态异步刷入底层 store
TEST(AsyncCircuitStoreTest, BackgroundFlushWritesToUnderlying)
{
    auto underlying = std::make_shared<MemoryCircuitStore>();
    AsyncCircuitStore store(underlying, std::chrono::milliseconds(10));

    CircuitStatus s;
    s.state = CircuitState::OPEN;
    EXPECT_TRUE(store.save("echo", "h:1", s));

    EXPECT_TRUE(WaitUntil([&]
                          { return underlying->load("echo", "h:1").state == CircuitState::OPEN; }));
}

// 同 key 多次 save，最终落盘最后一次状态（待刷队列去重后的可观察效果）
TEST(AsyncCircuitStoreTest, SaveLastWriteWins)
{
    auto underlying = std::make_shared<MemoryCircuitStore>();
    AsyncCircuitStore store(underlying, std::chrono::milliseconds(10));

    CircuitStatus open;
    open.state = CircuitState::OPEN;
    CircuitStatus half;
    half.state = CircuitState::HALF_OPEN;

    store.save("echo", "h:1", open);
    store.save("echo", "h:1", half); // 同 key 覆盖

    EXPECT_TRUE(WaitUntil([&]
                          { return underlying->load("echo", "h:1").state == CircuitState::HALF_OPEN; }));
}

// remove 同步删底层、传播底层返回值，且 async 缓存读回为默认 CLOSED
TEST(AsyncCircuitStoreTest, RemovePropagatesToUnderlying)
{
    auto underlying = std::make_shared<MemoryCircuitStore>();
    CircuitStatus s;
    s.state = CircuitState::OPEN;
    underlying->save("echo", "h:1", s); // 直接写底层，绕过 async

    AsyncCircuitStore store(underlying, std::chrono::milliseconds(10));

    EXPECT_TRUE(store.remove("echo", "h:1")); // 底层有记录 -> true
    EXPECT_EQ(underlying->load("echo", "h:1").state, CircuitState::CLOSED);
    EXPECT_EQ(store.load("echo", "h:1").state, CircuitState::CLOSED); // 缓存穿透底层
}

// 析构兜底：join 后台线程 + 冲刷残留待刷数据，状态不丢失
TEST(AsyncCircuitStoreTest, DestroyFlushesPendingToUnderlying)
{
    auto underlying = std::make_shared<MemoryCircuitStore>();
    CircuitStatus s;
    s.state = CircuitState::OPEN;

    {
        AsyncCircuitStore store(underlying, std::chrono::milliseconds(10));
        store.save("echo", "h:1", s);
    } // 析构：worker flush 或兜底 drain，最终都应落盘

    EXPECT_EQ(underlying->load("echo", "h:1").state, CircuitState::OPEN);
}

// 竞态回归：remove 发生在「批次已 swap、底层 save 在途」时，不应让旧状态被复活
TEST(AsyncCircuitStoreTest, RemoveBeatsInFlightFlush)
{
    auto spy = std::make_shared<BlockingSpyStore>();
    AsyncCircuitStore store(spy, std::chrono::milliseconds(10));

    CircuitStatus s;
    s.state = CircuitState::OPEN;
    store.save("echo", "h:1", s);

    // 等 worker 把 OPEN swap 进本地批次并卡在底层 save（在途）
    ASSERT_TRUE(WaitUntil([&] { return spy->save_entered.load(); }));

    // 在途刷盘期间 remove：应抑制这条 OPEN 落盘，且不被复活
    store.remove("echo", "h:1");

    // 放行 worker，让它完成 save + 补偿撤销
    {
        std::lock_guard<std::mutex> lk(spy->m);
        spy->allow = true;
    }
    spy->cv.notify_all();

    // 最终底层不应残留 OPEN（补偿撤销生效）
    EXPECT_TRUE(WaitUntil([&]
                          { return spy->load("echo", "h:1").state == CircuitState::CLOSED; }));
}
