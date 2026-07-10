#!/usr/bin/env bash

# RPC框架自动构建脚本
# 功能：检查依赖、拉取muduo子模块、配置并编译项目
# 用法: bash build.sh   （不要用 sh build.sh）

set -e  # 遇到错误立即退出

# ====== bash 守卫：防止用 sh/dash 运行 ======
if [ -z "${BASH_VERSION:-}" ]; then
    echo "[ERROR] 请用 bash 运行此脚本: bash $0" >&2
    exit 1
fi

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查命令是否存在（POSIX 兼容写法：>/dev/null 2>&1 替代 &>）
check_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        print_error "$1 未安装，请先安装 $1"
        exit 1
    fi
}

# 尝试安装系统依赖（无则安装）
# 用法: try_install_pkg "检测命令或条件" "apt包名" "yum/dnf包名"
try_install_pkg() {
    local check="$1"
    local pkg_apt="$2"
    local pkg_yum="$3"
    if eval "$check" 2>/dev/null; then
        return 0
    fi
    print_warn "未检测到依赖，尝试自动安装..."
    if command -v apt-get >/dev/null 2>&1; then
        sudo -n apt-get update -qq 2>/dev/null || true
        if sudo apt-get install -y "$pkg_apt"; then
            print_info "已安装 $pkg_apt ✓"
            return 0
        fi
    elif command -v dnf >/dev/null 2>&1; then
        if sudo dnf install -y "$pkg_yum"; then
            print_info "已安装 $pkg_yum ✓"
            return 0
        fi
    elif command -v yum >/dev/null 2>&1; then
        if sudo yum install -y "$pkg_yum"; then
            print_info "已安装 $pkg_yum ✓"
            return 0
        fi
    fi
    print_error "自动安装失败。请手动安装："
    echo "  Ubuntu/Debian: sudo apt-get install $pkg_apt"
    echo "  CentOS/RHEL:   sudo yum install $pkg_yum"
    exit 1
}

# 获取脚本所在目录（根目录的autobuild），然后切换到rpc目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RPC_DIR="$PROJECT_ROOT/rpc"
cd "$RPC_DIR"

print_info "开始构建 RPC 框架..."
print_info "脚本目录: $SCRIPT_DIR"
print_info "项目根目录: $PROJECT_ROOT"
print_info "RPC 目录: $RPC_DIR"

# 1. 检查必要的工具
print_info "检查构建工具..."
check_command git
check_command cmake
check_command g++

# 检查CMake版本
CMAKE_VERSION=$(cmake --version | head -n1 | cut -d' ' -f3)
CMAKE_MAJOR=$(echo "$CMAKE_VERSION" | cut -d'.' -f1)
CMAKE_MINOR=$(echo "$CMAKE_VERSION" | cut -d'.' -f2)
if [ "$CMAKE_MAJOR" -lt 3 ] || ([ "$CMAKE_MAJOR" -eq 3 ] && [ "$CMAKE_MINOR" -lt 16 ]); then
    print_error "CMake 版本过低，需要 >= 3.16，当前版本: $CMAKE_VERSION"
    exit 1
fi
print_info "CMake 版本: $CMAKE_VERSION ✓"

# 检查g++版本
GXX_VERSION=$(g++ --version | head -n1 | cut -d' ' -f4)
print_info "g++ 版本: $GXX_VERSION ✓"

# 2. 检查并安装系统依赖（没有则自动安装）
print_info "检查系统依赖..."

# Boost（muduo 需要）
try_install_pkg "dpkg -l libboost-dev 2>/dev/null | grep -q '^ii' || test -d /usr/include/boost" "libboost-dev" "boost-devel"
print_info "Boost 库已就绪 ✓"

# jsoncpp（需有头文件或 pkg-config，仅运行时库不够）
try_install_pkg "pkg-config --exists jsoncpp 2>/dev/null || test -f /usr/include/jsoncpp/json/json.h" "libjsoncpp-dev" "jsoncpp-devel"
print_info "jsoncpp 已就绪 ✓"

# Protobuf（rpc 需要 protobuf-compiler + libprotobuf-dev）
try_install_pkg "pkg-config --exists protobuf 2>/dev/null || test -f /usr/include/google/protobuf/descriptor.h" "protobuf-compiler libprotobuf-dev" "protobuf-compiler protobuf-devel"
print_info "Protobuf 已就绪 ✓"

# CURL（etcd 注册中心需要 libcurl）
try_install_pkg "ldconfig -p 2>/dev/null | grep -q libcurl || test -f /usr/include/x86_64-linux-gnu/curl/curl.h || test -f /usr/include/curl/curl.h" "libcurl4-openssl-dev" "libcurl-devel"
print_info "CURL 已就绪 ✓"

# 3. 初始化并更新git子模块（muduo）
print_info "初始化 git 子模块（muduo）..."

# 在 rpc 目录下执行 git submodule（git 会自动处理相对路径）
if [ ! -d "muduo" ] || [ -z "$(ls -A muduo 2>/dev/null)" ]; then
    print_info "muduo 目录为空，初始化子模块..."
    git submodule update --init --recursive
else
    print_info "muduo 目录已存在，同步父项目记录的提交（不拉远程）..."
    git submodule update --recursive
fi

if [ ! -d "muduo" ] || [ -z "$(ls -A muduo 2>/dev/null)" ]; then
    print_error "muduo 子模块初始化失败"
    exit 1
fi

print_info "muduo 子模块已就绪 ✓"

# 4. 创建构建目录
BUILD_DIR="build"
print_info "创建构建目录: $BUILD_DIR"
mkdir -p "$BUILD_DIR"

# 5. 配置CMake
print_info "配置 CMake..."
cd "$BUILD_DIR"

# 检查是否已有CMakeCache，如果有则询问是否清理
if [ -f "CMakeCache.txt" ]; then
    print_warn "检测到已存在的构建配置"
    read -r -p "是否清理并重新配置？(y/n) " -n 1
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_info "清理构建目录..."
        rm -rf ./*
    fi
fi

# CMake配置
CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DLCZ_RPC_BUILD_EXAMPLES=ON
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
)

print_info "CMake 配置参数: ${CMAKE_ARGS[*]}"
cmake "${CMAKE_ARGS[@]}" ..

if [ $? -ne 0 ]; then
    print_error "CMake 配置失败"
    exit 1
fi

print_info "CMake 配置成功 ✓"

# 6. 编译项目
print_info "开始编译项目..."
CPU_CORES=$(nproc 2>/dev/null || echo 4)
print_info "使用 $CPU_CORES 个CPU核心进行并行编译"

cmake --build . -j"$CPU_CORES"

if [ $? -ne 0 ]; then
    print_error "编译失败"
    exit 1
fi

print_info "编译成功 ✓"

# 7. 显示构建结果（路径相对于项目根目录 RPC/）
cd "$PROJECT_ROOT"
echo ""
print_info "=========================================="
print_info "构建完成！"
print_info "=========================================="
echo ""
print_info "所有可执行文件 (统一输出到 rpc/$BUILD_DIR/bin/):"
if [ -d "rpc/$BUILD_DIR/bin" ]; then
    find "rpc/$BUILD_DIR/bin" -type f -executable | sort | while read -r file; do
        echo "  - $file"
    done
fi

echo ""
print_info "常用运行示例:"
echo "  # 基本 RPC（test1）"
echo "  ./rpc/$BUILD_DIR/bin/test1_registry_server"
echo "  ./rpc/$BUILD_DIR/bin/test1_rpc_server"
echo "  ./rpc/$BUILD_DIR/bin/test1_rpc_client"
echo ""
echo "  # 注册中心 + 发现（test4）"
echo "  ./rpc/$BUILD_DIR/bin/test4_registry_server"
echo "  ./rpc/$BUILD_DIR/bin/test4_provider_server"
echo "  ./rpc/$BUILD_DIR/bin/test4_consumer_client"
echo ""
echo "  # 发布/订阅（test3）"
echo "  ./rpc/$BUILD_DIR/bin/test3_topic_server"
echo "  ./rpc/$BUILD_DIR/bin/test3_subscribe_client"
echo "  ./rpc/$BUILD_DIR/bin/test3_publish_client"
echo ""
echo "  # 熔断/超时（test1）"
echo "  ./rpc/$BUILD_DIR/bin/circuit_breaker_test_server"
echo "  ./rpc/$BUILD_DIR/bin/circuit_breaker_test_client"
echo "  ./rpc/$BUILD_DIR/bin/test1_slow_rpc_server"
echo "  ./rpc/$BUILD_DIR/bin/test1_timeout_test_client"
echo ""
echo "  # 共享内存/基准测试"
echo "  ./rpc/$BUILD_DIR/bin/shm_server"
echo "  ./rpc/$BUILD_DIR/bin/shm_client"
echo "  ./rpc/$BUILD_DIR/bin/benchmark_server"
echo "  ./rpc/$BUILD_DIR/bin/benchmark_client"
echo ""
echo "  # 一键演示脚本"
echo "  bash demosh/demo.sh etcd"
echo "  bash demosh/demo.sh all"
echo ""
print_info "构建脚本执行完成！"
