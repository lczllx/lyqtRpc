#找系统已经有的CRUL库，方便后续链接，如果找不到 CURL，CMake 会报错并停止配置
find_package(CURL REQUIRED)

#新增静态库 编译 registry_store 实现，给 lcz_rpc 链接用
add_library(lcz_registry_store STATIC
    server/memory_registry_store.cpp
    server/etcd_registry_store.cpp
    server/etcd_leader_election.cpp
    server/memory_circuit_store.cpp
    server/etcd_circuit_store.cpp
    server/async_circuit_store.cpp
    client/node_breaker.cpp
    client/circuit_breaker.cpp
)
target_include_directories(lcz_registry_store
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../muduo>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/..>
)
target_link_libraries(lcz_registry_store
  PUBLIC
    CURL::libcurl
)

#接口库 不产出动静态库，只做为头文件路径和链接依赖的聚合目标
add_library(lcz_rpc INTERFACE)

# 生成头在上级 build 目录（rpc/build/proto/），由 lcz_rpc_proto 提供
target_include_directories(lcz_rpc
  INTERFACE # 链接此库的目标继承这些路径
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../muduo>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/..>
    $<INSTALL_INTERFACE:include>
)
#任何链接 lcz_rpc 的可执行文件，自动带上这些库
target_link_libraries(lcz_rpc
  INTERFACE
    lcz_registry_store
    muduo_net
    muduo_base
    jsoncpp_lib
    lcz_rpc_proto
    pthread
)
