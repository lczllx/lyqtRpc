# =============================================================================
# fuzz/CMakeLists.txt — libFuzzer 模糊测试目标
# -----------------------------------------------------------------------------
# 仅当 LCZ_RPC_BUILD_FUZZER=ON 且编译器为 Clang 时由主 CMakeLists 加入。
# -fsanitize=fuzzer,address,undefined 的作用：
#   fuzzer    → libFuzzer 自动提供 main + 变异驱动（需 Clang，g++ 不支持）
#   address   → ASan 抓越界 / use-after-free / 异常大分配
#   undefined → UBSan 抓整数溢出 / 除零 / 空指针
# -fno-sanitize-recover=all 让任何 sanitizer 命中都立即 abort（libFuzzer 记为 crash），
# 否则 UBSan 的 signed-integer-overflow 默认只打印警告、不会停下。
#
# 注意：LVProtocol / MessageFactory / unserialize 均为 header-only，
# 会随本 target 一起用 sanitizer 编译，故核心解析路径已被 instrument。
# =============================================================================

add_executable(lcz_rpc_lv_fuzzer fuzz_lv_protocol.cc)

# 复用 tests/ 的 string_buffer.hpp（与 muduo::net::Buffer 在 int32 读写语义上一致的假缓冲区）
target_include_directories(lcz_rpc_lv_fuzzer PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests
)

target_link_libraries(lcz_rpc_lv_fuzzer PRIVATE lcz_rpc)

target_compile_options(lcz_rpc_lv_fuzzer PRIVATE
    -fsanitize=fuzzer,address,undefined
    -fno-omit-frame-pointer
    -fno-sanitize-recover=all
    -g
)

target_link_options(lcz_rpc_lv_fuzzer PRIVATE
    -fsanitize=fuzzer,address,undefined
)

set_target_properties(lcz_rpc_lv_fuzzer PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
