// shm_zc_adaptor 单元测试：RingBufAllocator 顺序分配/OOM + ShmZcReader 零拷贝映射/strval
#include <gtest/gtest.h>
#include <span>
#include <string>
#include "src/general/shm_zc_adaptor.hpp"
#include "rpc_message_generated.h"
#include "flatbuffers/flatbuffers.h"

using lcz_rpc::RingBufAllocator;
using lcz_rpc::ShmZcReader;

// 顺序分配：指针连续推进，used() 累计
TEST(RingBufAllocatorTest, SequentialAllocAdvancesOffset)
{
    char buf[64];
    RingBufAllocator alloc(std::span<char>(buf, sizeof(buf)));

    uint8_t *a = alloc.allocate(4);
    uint8_t *b = alloc.allocate(8);
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(a, reinterpret_cast<uint8_t *>(buf));
    EXPECT_EQ(b, a + 4);
    EXPECT_EQ(alloc.used(), 12u);
}

// 超出容量 → 返回 nullptr，used() 不变
TEST(RingBufAllocatorTest, OomReturnsNull)
{
    char buf[8];
    RingBufAllocator alloc(std::span<char>(buf, sizeof(buf)));
    ASSERT_NE(alloc.allocate(8), nullptr);
    EXPECT_EQ(alloc.used(), 8u);
    EXPECT_EQ(alloc.allocate(1), nullptr); // 越界
    EXPECT_EQ(alloc.used(), 8u);           // 不推进
}

// deallocate 是 no-op（ring buffer 由 commit 提交），不影响 used
TEST(RingBufAllocatorTest, DeallocateIsNoop)
{
    char buf[16];
    RingBufAllocator alloc(std::span<char>(buf, sizeof(buf)));
    uint8_t *p = alloc.allocate(4);
    alloc.deallocate(p, 4);
    EXPECT_EQ(alloc.used(), 4u);
}

// 空数据 → as<T> 返回 nullptr
TEST(ShmZcReaderTest, EmptyDataAsNull)
{
    ShmZcReader reader(std::span<const char>{});
    EXPECT_EQ(reader.as<lcz_rpc::fb::RpcRequest>(), nullptr);
}

// 合法 FlatBuffer → 零拷贝映射，字段可读
TEST(ShmZcReaderTest, ValidDataMaps)
{
    flatbuffers::FlatBufferBuilder fbb;
    auto root = lcz_rpc::fb::CreateRpcRequestDirect(fbb, "req-1", "add", "trace-1", "span-1", nullptr);
    fbb.Finish(root);

    ShmZcReader reader(std::span<const char>(
        reinterpret_cast<const char *>(fbb.GetBufferPointer()), fbb.GetSize()));
    const auto *req = reader.as<lcz_rpc::fb::RpcRequest>();
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(std::string(req->id()->str()), "req-1");
    EXPECT_EQ(std::string(req->method()->str()), "add");
    EXPECT_EQ(std::string(req->trace_id()->str()), "trace-1");
    EXPECT_EQ(std::string(req->span_id()->str()), "span-1");
}

// data()/size() 透传原始字节区间
TEST(ShmZcReaderTest, DataAndSizeAccessors)
{
    const char buf[] = "abc";
    ShmZcReader reader(std::span<const char>(buf, 3));
    EXPECT_EQ(reader.size(), 3u);
    EXPECT_EQ(static_cast<const void *>(reader.data()), static_cast<const void *>(buf));
}

// strval：null → 空串；合法 String → 内容
TEST(ShmZcReaderTest, Strval)
{
    EXPECT_EQ(ShmZcReader::strval(nullptr), "");

    flatbuffers::FlatBufferBuilder fbb;
    fbb.Finish(fbb.CreateString("hello"));
    const auto *s = flatbuffers::GetRoot<flatbuffers::String>(fbb.GetBufferPointer());
    EXPECT_EQ(ShmZcReader::strval(s), "hello");
}
