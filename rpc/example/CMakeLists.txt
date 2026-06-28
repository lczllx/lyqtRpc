add_executable(message_test message_test.cc)
target_link_libraries(message_test PRIVATE lcz_rpc)#链接 lcz_rpc

add_executable(despacher_server_test despacher_server_test.cc)
target_link_libraries(despacher_server_test PRIVATE lcz_rpc)

add_executable(despacher_client_test despacher_client_test.cc)
target_link_libraries(despacher_client_test PRIVATE lcz_rpc)

add_subdirectory(test)
add_subdirectory(benchmark)
add_subdirectory(shm)

