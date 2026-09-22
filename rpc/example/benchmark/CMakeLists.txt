# Protobuf 版（默认序列化）
add_executable(benchmark_server benchmark_server.cc)
target_link_libraries(benchmark_server PRIVATE lcz_rpc)
set_target_properties(benchmark_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(benchmark_client benchmark_client.cc)
target_link_libraries(benchmark_client PRIVATE lcz_rpc)
set_target_properties(benchmark_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# JSON 版（原版副本，保留用于对比）
add_executable(benchmark_server_json benchmark_server_json.cc)
target_link_libraries(benchmark_server_json PRIVATE lcz_rpc)
set_target_properties(benchmark_server_json PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(benchmark_client_json benchmark_client_json.cc)
target_link_libraries(benchmark_client_json PRIVATE lcz_rpc)
set_target_properties(benchmark_client_json PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# 一致性哈希负载均衡微基准（不依赖网络，直接压 MethodHost）
add_executable(benchmark_consistent_hash benchmark_consistent_hash.cc)
target_link_libraries(benchmark_consistent_hash PRIVATE lcz_rpc)
set_target_properties(benchmark_consistent_hash PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)




