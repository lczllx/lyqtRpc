add_executable(shm_server shm_server.cc)
target_link_libraries(shm_server PRIVATE lcz_rpc)
set_target_properties(shm_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(shm_client shm_client.cc)
target_link_libraries(shm_client PRIVATE lcz_rpc)
set_target_properties(shm_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(shm_benchmark_server shm_benchmark_server.cc)
target_link_libraries(shm_benchmark_server PRIVATE lcz_rpc)
set_target_properties(shm_benchmark_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(shm_benchmark_client shm_benchmark_client.cc)
target_link_libraries(shm_benchmark_client PRIVATE lcz_rpc)
set_target_properties(shm_benchmark_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# SHM + Protobuf 零拷贝（写端 SerializeToArray 直写 ring buffer）
add_executable(shm_proto_server shm_proto_server.cc)
target_link_libraries(shm_proto_server PRIVATE lcz_rpc)
set_target_properties(shm_proto_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(shm_proto_client shm_proto_client.cc)
target_link_libraries(shm_proto_client PRIVATE lcz_rpc)
set_target_properties(shm_proto_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(shm_benchmark_proto_server shm_benchmark_proto_server.cc)
target_link_libraries(shm_benchmark_proto_server PRIVATE lcz_rpc)
set_target_properties(shm_benchmark_proto_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(shm_benchmark_proto_client shm_benchmark_proto_client.cc)
target_link_libraries(shm_benchmark_proto_client PRIVATE lcz_rpc)
set_target_properties(shm_benchmark_proto_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# 零拷贝 FlatBuffers 示例（仅在 FlatBuffers 可用时构建）
if(LCZ_HAVE_FLATBUFFERS)
  add_executable(shm_server_zc shm_server_zc.cc)
  target_link_libraries(shm_server_zc PRIVATE lcz_rpc lcz_flatbuf)
  target_include_directories(shm_server_zc PRIVATE ${FLATBUFFERS_GEN_DIR})
  set_target_properties(shm_server_zc PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

  add_executable(shm_client_zc shm_client_zc.cc)
  target_link_libraries(shm_client_zc PRIVATE lcz_rpc lcz_flatbuf)
  target_include_directories(shm_client_zc PRIVATE ${FLATBUFFERS_GEN_DIR})
  set_target_properties(shm_client_zc PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

  add_executable(shm_benchmark_server_zc shm_benchmark_server_zc.cc)
  target_link_libraries(shm_benchmark_server_zc PRIVATE lcz_rpc lcz_flatbuf)
  target_include_directories(shm_benchmark_server_zc PRIVATE ${FLATBUFFERS_GEN_DIR})
  set_target_properties(shm_benchmark_server_zc PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

  add_executable(shm_benchmark_client_zc shm_benchmark_client_zc.cc)
  target_link_libraries(shm_benchmark_client_zc PRIVATE lcz_rpc lcz_flatbuf)
  target_include_directories(shm_benchmark_client_zc PRIVATE ${FLATBUFFERS_GEN_DIR})
  set_target_properties(shm_benchmark_client_zc PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
endif()
