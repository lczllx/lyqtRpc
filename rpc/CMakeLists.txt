cmake_minimum_required(VERSION 3.16)

project(lcz_rpc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON) #编译器必须支持 C++17
set(CMAKE_CXX_EXTENSIONS OFF)  #不要 GNU 扩展
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer") #perf 采样需要帧指针
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) #生成 compile_commands.json（给 IDE）

option(LCZ_RPC_BUILD_EXAMPLES "构建 example 目录中的示例程序" ON)
option(LCZ_RPC_BUILD_TESTS "构建单元测试（GoogleTest，需系统已安装，如 Ubuntu: libgtest-dev）" OFF)

# 统一使用系统安装的 jsoncpp（头文件路径 <jsoncpp/json/json.h>）
# 先尝试 CMake 配置，若无则用 pkg-config（部分系统仅提供 pkg-config）
find_package(jsoncpp QUIET)
if(jsoncpp_FOUND)
    if(NOT TARGET jsoncpp_lib)
        add_library(jsoncpp_lib INTERFACE)
        target_link_libraries(jsoncpp_lib INTERFACE jsoncpp::jsoncpp)
    endif()
    message(STATUS "使用系统安装的 jsoncpp (CMake)")
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(JsonCpp jsoncpp)
        if(NOT JsonCpp_FOUND)
            pkg_check_modules(JsonCpp libjsoncpp)
        endif()
    endif()
    if(JsonCpp_FOUND)
        add_library(jsoncpp_lib INTERFACE)
        target_include_directories(jsoncpp_lib INTERFACE ${JsonCpp_INCLUDE_DIRS})
        target_link_libraries(jsoncpp_lib INTERFACE ${JsonCpp_LINK_LIBRARIES})
        message(STATUS "使用系统安装的 jsoncpp (pkg-config)")
    else()
        # 最后尝试：直接找头文件和库（部分环境 pkg-config 包名不同）
        find_path(JSONCPP_INCLUDE_DIR jsoncpp/json/json.h PATHS /usr/include /usr/local/include)
        find_library(JSONCPP_LIBRARY jsoncpp PATHS /usr/lib /usr/local/lib /usr/lib/x86_64-linux-gnu)
        if(JSONCPP_INCLUDE_DIR AND JSONCPP_LIBRARY)
            add_library(jsoncpp_lib INTERFACE)
            target_include_directories(jsoncpp_lib INTERFACE ${JSONCPP_INCLUDE_DIR})
            target_link_libraries(jsoncpp_lib INTERFACE ${JSONCPP_LIBRARY})
            message(STATUS "使用系统安装的 jsoncpp (find_path/find_library)")
        else()
            message(FATAL_ERROR "未找到 jsoncpp。请安装: Ubuntu/Debian: sudo apt install libjsoncpp-dev; CentOS/RHEL: sudo yum install jsoncpp-devel")
        endif()
    endif()
endif()

# Protobuf（路径二：纯 Proto RPC）
find_package(Protobuf QUIET)
if(NOT Protobuf_FOUND)
    message(FATAL_ERROR "未找到 Protobuf。请安装:\n  Ubuntu/Debian: sudo apt install protobuf-compiler libprotobuf-dev\n  CentOS/RHEL:   sudo yum install protobuf-compiler protobuf-devel\n或运行 autobuild/build.sh 自动安装")
endif()
set(LCZ_PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/proto/rpc_envelope.proto")#protoc 编译 → .pb.cc + .pb.h
set(LCZ_PROTOBUF_IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR})
protobuf_generate_cpp(LCZ_PROTO_SRCS LCZ_PROTO_HDRS ${LCZ_PROTO_FILE})
add_library(lcz_rpc_proto STATIC ${LCZ_PROTO_SRCS}) #产物：liblcz_rpc_proto.a
target_link_libraries(lcz_rpc_proto PUBLIC protobuf::libprotobuf)
target_include_directories(lcz_rpc_proto PUBLIC
  ${CMAKE_CURRENT_BINARY_DIR}
  ${Protobuf_INCLUDE_DIRS}
)

# 若 git clone 时没用 --recursive，muduo 目录为空 → 自动从 GitHub 拉取
if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/muduo/CMakeLists.txt")
    message(STATUS "muduo 子模块未初始化，自动拉取...")
    set(MUDUO_URL "https://github.com/chenshuo/muduo/archive/f1fc77e0c13b80e5086ff457362c8a86d1b609d4.tar.gz")
    set(MUDUO_TGZ "${CMAKE_BINARY_DIR}/muduo.tar.gz")

    file(DOWNLOAD "${MUDUO_URL}" "${MUDUO_TGZ}" SHOW_PROGRESS STATUS dl_status)
    list(GET dl_status 0 dl_code)
    if(NOT dl_code EQUAL 0)
        list(GET dl_status 1 dl_msg)
        message(FATAL_ERROR "muduo 下载失败: ${dl_msg}\n请手动执行: git submodule update --init")
    endif()

    execute_process(
        COMMAND tar -xzf "${MUDUO_TGZ}"
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
        RESULT_VARIABLE tar_result
    )
    file(REMOVE "${MUDUO_TGZ}")
    if(NOT tar_result EQUAL 0)
        message(FATAL_ERROR "muduo 解压失败，请手动执行: git submodule update --init")
    endif()

    file(GLOB muduo_src_dir LIST_DIRECTORIES true "${CMAKE_BINARY_DIR}/muduo-*")
    list(LENGTH muduo_src_dir muduo_count)
    if(muduo_count EQUAL 0)
        message(FATAL_ERROR "muduo 解压后未找到源码目录，请手动执行: git submodule update --init")
    endif()
    list(GET muduo_src_dir 0 muduo_dir)
    file(RENAME "${muduo_dir}" "${CMAKE_CURRENT_SOURCE_DIR}/muduo")
    message(STATUS "muduo 拉取完成")
endif()

# FlatBuffers（SHM 零拷贝序列化适配层）
# 优先使用项目本地安装，其次系统安装
set(_fb_prefix "${CMAKE_CURRENT_SOURCE_DIR}/../flatbuffers_install")
set(_fb_inc     "${_fb_prefix}/include")
set(_fb_flatc   "${_fb_prefix}/bin/flatc")

if(NOT EXISTS "${_fb_flatc}")
  # 尝试系统安装
  find_program(_fb_flatc NAMES flatc)
  find_path(_fb_inc NAMES flatbuffers/flatbuffers.h PATHS /usr/include /usr/local/include)
endif()

if(EXISTS "${_fb_flatc}" AND EXISTS "${_fb_inc}")
  set(FLATBUFFERS_COMPILER    "${_fb_flatc}")
  set(FLATBUFFERS_INCLUDE_DIR "${_fb_inc}")
  message(STATUS "FlatBuffers: flatc=${FLATBUFFERS_COMPILER} include=${FLATBUFFERS_INCLUDE_DIR}")
else()
  message(WARNING "未找到 FlatBuffers。SHM 零拷贝示例将跳过编译。\n  安装: sudo apt install flatbuffers-compiler libflatbuffers-dev")
endif()

# FlatBuffers schema 编译
set(FLATBUFFERS_FBS_FILE "${CMAKE_CURRENT_SOURCE_DIR}/proto/rpc_message.fbs")
set(FLATBUFFERS_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/flatbuf_generated")
if(EXISTS "${FLATBUFFERS_COMPILER}" AND EXISTS "${FLATBUFFERS_FBS_FILE}")
  file(MAKE_DIRECTORY "${FLATBUFFERS_GEN_DIR}")
  execute_process(
    COMMAND "${FLATBUFFERS_COMPILER}" --cpp --gen-object-api
            -o "${FLATBUFFERS_GEN_DIR}"
            "${FLATBUFFERS_FBS_FILE}"
    RESULT_VARIABLE flatc_result
    OUTPUT_VARIABLE flatc_output
    ERROR_VARIABLE  flatc_error
  )
  if(flatc_result EQUAL 0)
    message(STATUS "FlatBuffers schema 编译成功 → ${FLATBUFFERS_GEN_DIR}")
    # 创建 FlatBuffers 接口库
    add_library(lcz_flatbuf INTERFACE)
    target_include_directories(lcz_flatbuf INTERFACE
      "${FLATBUFFERS_GEN_DIR}"
      "${FLATBUFFERS_INCLUDE_DIR}"
    )
    set(LCZ_HAVE_FLATBUFFERS TRUE)
  else()
    message(WARNING "FlatBuffers schema 编译失败: ${flatc_error}")
    set(LCZ_HAVE_FLATBUFFERS FALSE)
  endif()
else()
  message(STATUS "FlatBuffers 编译器或 schema 不可用，跳过零拷贝示例")
  set(LCZ_HAVE_FLATBUFFERS FALSE)
endif()

set(MUDUO_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)#关掉 muduo 自带的 example
# 进入 muduo/CMakeLists.txt->创建 muduo_net (STATIC)
#→ libmuduo_net.a→ 创建 muduo_base (STATIC) → libmuduo_base.a→ 回到当前文件
add_subdirectory(muduo)

add_subdirectory(src)

if(LCZ_RPC_BUILD_EXAMPLES)
  add_subdirectory(example)
endif()

if(LCZ_RPC_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

