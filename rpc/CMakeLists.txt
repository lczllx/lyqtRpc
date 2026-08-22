cmake_minimum_required(VERSION 3.16)

project(lcz_rpc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON) #编译器必须支持 C++20
set(CMAKE_CXX_EXTENSIONS OFF)  #不要 GNU 扩展
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer") #perf 采样需要帧指针
# 上面的 set(CMAKE_CXX_FLAGS) 会被 muduo/CMakeLists.txt 里的 string(REPLACE ... CMAKE_CXX_FLAGS) 覆盖，
# 导致 muduo 用 -O2 编译（默认 -fomit-frame-pointer），fp 回溯在 muduo 这层断掉、火焰图整段 [unknown]。
# add_compile_options 走 COMPILE_OPTIONS 目录属性、不被变量覆盖，能穿透到 muduo 子目录。
add_compile_options(-fno-omit-frame-pointer)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) #生成 compile_commands.json（给 IDE）

# ---- lyqtrpc_build_info 版本元数据 ----
# 在 cmake 配置时捕获 git commit（HEAD），构建时有此定义则嵌入二进制
execute_process(COMMAND git rev-parse --short=8 HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE LCZ_GIT_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
if(NOT LCZ_GIT_COMMIT)
    set(LCZ_GIT_COMMIT "unknown")
endif()
string(TIMESTAMP LCZ_BUILD_TIME "%Y-%m-%dT%H:%M:%SZ")
add_compile_definitions(
    LCZ_RPC_VERSION="0.5.0"
    LCZ_GIT_COMMIT="${LCZ_GIT_COMMIT}"
    LCZ_BUILD_TIME="${LCZ_BUILD_TIME}"
)

option(LCZ_RPC_BUILD_EXAMPLES "构建 example 目录中的示例程序" ON)
option(LCZ_RPC_BUILD_TESTS "构建单元测试（GoogleTest，需系统已安装，如 Ubuntu: libgtest-dev）" OFF)
option(LCZ_RPC_SANITIZE "启用 AddressSanitizer + UndefinedBehaviorSanitizer（仅测试用，会拖慢 2~5 倍，勿用于生产）" OFF)
option(LCZ_RPC_BUILD_FUZZER "构建 libFuzzer 模糊测试目标（需 Clang，-fsanitize=fuzzer 仅 Clang 支持）" OFF)

# ---- Sanitizers：内存错误 / 未定义行为检测 ----
# ASan 抓越界/use-after-free/double-free/泄漏；UBSan 抓整数溢出/除零/空指针解引用。
# 只应在测试阶段开启；生产构建保持 OFF。TSan 因与 ASan 冲突，需单独编译，这里不混入。
if(LCZ_RPC_SANITIZE)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address,undefined)
    message(STATUS "Sanitizers: ASan + UBSan 已启用")
endif()

# jsoncpp：CONFIG（vcpkg）→ MODULE（apt FindJsonCpp.cmake）→ pkg-config → 手动找
find_package(jsoncpp CONFIG QUIET)
if(NOT jsoncpp_FOUND)
    find_package(jsoncpp MODULE QUIET)
endif()
if(jsoncpp_FOUND)
    if(NOT TARGET jsoncpp_lib)
        add_library(jsoncpp_lib INTERFACE)
        # vcpkg 的 target 是 JsonCpp::JsonCpp，apt/system 的是 jsoncpp::jsoncpp
        if(TARGET JsonCpp::JsonCpp)
            target_link_libraries(jsoncpp_lib INTERFACE JsonCpp::JsonCpp)
        elseif(TARGET jsoncpp::jsoncpp)
            target_link_libraries(jsoncpp_lib INTERFACE jsoncpp::jsoncpp)
        endif()
    endif()
    message(STATUS "jsoncpp: CMake find_package ✓")
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
        message(STATUS "jsoncpp: pkg-config ✓")
    else()
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

# Protobuf：CONFIG（vcpkg）→ MODULE（apt FindProtobuf.cmake）
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
    find_package(Protobuf MODULE QUIET)
endif()
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

# ---- Fuzzing：libFuzzer 驱动 LV 协议解析 ----
if(LCZ_RPC_BUILD_FUZZER)
    if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        message(FATAL_ERROR "libFuzzer 需要 Clang 编译器（-fsanitize=fuzzer 仅 Clang 支持），当前是 ${CMAKE_CXX_COMPILER_ID}。请用 fuzz preset：cmake --preset fuzz -B build-fuzz")
    endif()
    add_subdirectory(fuzz)
endif()

if(LCZ_RPC_BUILD_EXAMPLES)
  add_subdirectory(example)
endif()

if(LCZ_RPC_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

option(LCZ_RPC_BUILD_GATEWAY "Build API Gateway" ON)
if(LCZ_RPC_BUILD_GATEWAY)
  add_subdirectory(../gateway ${CMAKE_BINARY_DIR}/gateway)
endif()

