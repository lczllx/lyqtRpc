# =============================================================================
# tests/CMakeLists.txt — 汇总单元测试可执行文件 lcz_rpc_unit_tests
# -----------------------------------------------------------------------------
# 默认关闭：cmake -DLCZ_RPC_BUILD_TESTS=ON 开启。
# 依赖：系统 GTest（如 Ubuntu: sudo apt install libgtest-dev），见各 .cc 文件头注释。
#
# 源文件职责概览：
#   string_buffer.hpp          测试用 BaseBuffer，供 LV 测试注入字节流
#   test_lv_protocol.cc        LV 基础：JSON RPC/Service 发现、半包、粘包
#   test_lv_more_messages.cc   LV 扩展：Topic/Service/Proto 全路径往返
#   test_message_factory.cc    MessageFactory 分组冒烟 + 非法 MsgType
#   test_message_factory_exhaustive.cc  12 种合法 MsgType 表驱动
#   test_message_checks.cc     check() 负例
#   test_err_reason.cc         errReason 文案
#   test_rate_limiter.cc        TokenBucket 令牌桶限流器
#   test_serializer.cc          ISerializer / ProtobufSerializer 编解码
#   test_shm_channel.cc         ShmChannel 共享内存单进程闭环
#   test_metrics.cc             Prometheus 指标内存模型（Counter/Gauge/Histogram/导出格式）
#   test_metrics_server.cc      /metrics HTTP 端点起停 + process_* 采集
# =============================================================================
find_package(GTest REQUIRED CONFIG)

add_executable(lcz_rpc_unit_tests
  test_lv_protocol.cc
  test_lv_more_messages.cc
  test_message_factory.cc
  test_message_factory_exhaustive.cc
  test_message_checks.cc
  test_err_reason.cc
  test_rate_limiter.cc
  test_serializer.cc
  test_shm_channel.cc
  test_metrics.cc
  test_metrics_server.cc
)

target_include_directories(lcz_rpc_unit_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(lcz_rpc_unit_tests
  PRIVATE
    lcz_rpc
    GTest::gtest_main
)
set_target_properties(lcz_rpc_unit_tests PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

include(GoogleTest)
gtest_discover_tests(lcz_rpc_unit_tests)
