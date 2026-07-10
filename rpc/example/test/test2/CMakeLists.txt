add_executable(test2_server test_server.cc)
set_target_properties(test2_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
target_link_libraries(test2_server PRIVATE lcz_rpc)

add_executable(test2_client test_client.cc)
set_target_properties(test2_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
target_link_libraries(test2_client PRIVATE lcz_rpc)

