add_executable(shm_server shm_server.cc)
target_link_libraries(shm_server PRIVATE lcz_rpc)
set_target_properties(shm_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(shm_client shm_client.cc)
target_link_libraries(shm_client PRIVATE lcz_rpc)
set_target_properties(shm_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
