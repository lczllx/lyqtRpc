#!/bin/bash

# 快速构建脚本（简化版）
# 适用于已经配置好环境的用户
# 若无依赖，请先运行 build.sh 自动安装

set -e

# 获取脚本所在目录（根目录的autobuild），然后切换到rpc目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RPC_DIR="$PROJECT_ROOT/rpc"
cd "$RPC_DIR"

# ====== 轻量依赖检查（只检测，不安装；缺少则引导至 build.sh） ======
check_dep() {
    local name="$1"
    local check_cmd="$2"
    local pkgs="$3"
    if eval "$check_cmd" 2>/dev/null; then
        echo "  [✓] $name"
    else
        echo "  [✗] $name 未安装"
        echo "      请运行: bash autobuild/build.sh（自动安装）"
        echo "      或手动: sudo apt install $pkgs"
        MISSING_DEPS=1
    fi
}

echo "[INFO] 检查依赖..."
MISSING_DEPS=0
check_dep "Boost"       "dpkg -l libboost-dev 2>/dev/null | grep -q '^ii' || test -d /usr/include/boost"   "libboost-dev"
check_dep "jsoncpp"     "pkg-config --exists jsoncpp 2>/dev/null || test -f /usr/include/jsoncpp/json/json.h" "libjsoncpp-dev"
check_dep "Protobuf"    "pkg-config --exists protobuf 2>/dev/null || test -f /usr/include/google/protobuf/descriptor.h" "protobuf-compiler libprotobuf-dev"
check_dep "CURL"        "ldconfig -p 2>/dev/null | grep -q libcurl || test -f /usr/include/x86_64-linux-gnu/curl/curl.h || test -f /usr/include/curl/curl.h" "libcurl4-openssl-dev"

if [ "$MISSING_DEPS" -ne 0 ]; then
    echo ""
    echo "[ERROR] 缺少依赖，请先安装后再构建。"
    exit 1
fi
echo ""

# 初始化子模块（在 rpc 目录下执行，git 会自动处理相对路径）
if [ ! -d "muduo" ] || [ -z "$(ls -A muduo 2>/dev/null)" ]; then
    echo "[INFO] 初始化 muduo 子模块..."
    git submodule update --init --recursive
fi

# 构建
mkdir -p build
cd build

if [ ! -f "CMakeCache.txt" ]; then
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DLCZ_RPC_BUILD_EXAMPLES=ON \
          -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
          ..
fi

cmake --build . -j$(nproc)

echo "[INFO] 构建完成！"

