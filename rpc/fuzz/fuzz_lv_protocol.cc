// =============================================================================
// fuzz_lv_protocol.cc — LV 定帧协议的 libFuzzer 模糊测试入口
// -----------------------------------------------------------------------------
// 目标：任意字节流喂给 LVProtocol::canProcessed + onMessage，抓
//   ① 长度字段（total_len/id_len）的整数溢出（UBSan signed-integer-overflow）
//   ② 负数长度隐式转为巨大 size_t 导致的越界/异常分配（ASan）
//   ③ 非法 MsgType 的健壮性
//
// 编译：需 Clang（-fsanitize=fuzzer 仅 Clang 支持），由 fuzz/CMakeLists.txt 驱动。
// 运行：./bin/lcz_rpc_lv_fuzzer -max_total_time=60 -print_final_stats=1
// =============================================================================

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "src/general/message.hpp"
#include "src/general/net.hpp"
#include "string_buffer.hpp"

using lcz_rpc::LVProtocol;
using lcz_rpc::BaseMessage;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size == 0)
        return 0;

    // 任意字节构造一个「内存缓冲区」，模拟网络对端发来的一个 TCP 段
    std::string bytes(reinterpret_cast<const char *>(data), size);
    auto buf = std::make_shared<lcz_rpc::test::StringBuffer>(std::move(bytes));

    LVProtocol proto;
    // 限制帧数，防 fuzz 构造出大量粘包帧导致单次输入超时
    for (int i = 0; i < 8; ++i)
    {
        if (!proto.canProcessed(buf))
            break;
        BaseMessage::ptr msg;
        proto.onMessage(buf, msg); // 返回值有意忽略：解析失败不应 crash
    }
    return 0;
}
