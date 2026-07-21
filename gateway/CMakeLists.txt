# =============================================================================
# gateway/CMakeLists.txt — API Gateway 构建（作为 rpc 的子目录被引入）
# =============================================================================
# 由 rpc/CMakeLists.txt 中的 option(LCZ_RPC_BUILD_GATEWAY) 控制开关。
# 链接 lcz_rpc INTERFACE 库 → 自动获得 muduo + protobuf + jsoncpp + curl。
# =============================================================================

add_executable(gateway_server example/gateway_server.cc)

target_include_directories(gateway_server
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../rpc/src    # client/ server/ general/
    PRIVATE ${CMAKE_BINARY_DIR}                       # rpc_envelope.pb.h
)

target_link_libraries(gateway_server
    PRIVATE lcz_rpc
)

set_target_properties(gateway_server PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
