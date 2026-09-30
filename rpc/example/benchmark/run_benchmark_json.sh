#!/bin/sh
# RPC 性能测试脚本（JSON 序列化）
# 用法: bash run_benchmark_json.sh    （任意位置执行均可）

SERVER_PORT=8889
REGISTRY_PORT=8080
USE_DISCOVER=0
RATE_LIMIT=0  # 0=不限流
STEADY_SECS=${STEADY_SECS:-30}  # 稳态压测时长；README 性能表的 lyqtRpc 列即 30 秒稳态采样

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ====== 自动检测 build/bin 路径 ======
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
find_bin() {
    local dir="$SCRIPT_DIR"
    while [ "$dir" != "/" ]; do
        if [ -f "$dir/CMakeCache.txt" ] && [ -d "$dir/bin" ]; then
            echo "$dir/bin"; return 0
        fi
        dir="$(dirname "$dir")"
    done
    dir="$SCRIPT_DIR"
    while [ "$dir" != "/" ]; do
        if [ -d "$dir/build/bin" ]; then
            echo "$dir/build/bin"; return 0
        fi
        dir="$(dirname "$dir")"
    done
    return 1
}
BIN_DIR="$(find_bin)"
if [ -z "$BIN_DIR" ]; then
    printf '%b\n' "${YELLOW}错误: 找不到 build/bin 目录，请先编译项目${NC}"
    exit 1
fi

printf '%b\n' "${GREEN}========== RPC 性能测试脚本（JSON）==========${NC}"

if [ ! -f "$BIN_DIR/benchmark_server_json" ]; then
    printf '%b\n' "${YELLOW}错误: 找不到 benchmark_server_json，请先编译${NC}"
    exit 1
fi
if [ ! -f "$BIN_DIR/benchmark_client_json" ]; then
    printf '%b\n' "${YELLOW}错误: 找不到 benchmark_client_json，请先编译${NC}"
    exit 1
fi

printf '%b\n' "${GREEN}启动服务端（JSON）...${NC}"
"$BIN_DIR/benchmark_server_json" $SERVER_PORT $USE_DISCOVER $REGISTRY_PORT $RATE_LIMIT &
SERVER_PID=$!
sleep 2

if ! kill -0 $SERVER_PID 2>/dev/null; then
    printf '%b\n' "${YELLOW}错误: 服务端启动失败${NC}"
    exit 1
fi

TEST_REQUESTS=20000

printf '\n%b\n' "${GREEN}========== 测试 1: 单线程 ==========${NC}"
"$BIN_DIR/benchmark_client_json" single add $TEST_REQUESTS 0 0 $USE_DISCOVER 127.0.0.1 $SERVER_PORT $REGISTRY_PORT

printf '\n%b\n' "${GREEN}========== 测试 2: 多线程 (4) ==========${NC}"
"$BIN_DIR/benchmark_client_json" multi add $TEST_REQUESTS 4 0 $USE_DISCOVER 127.0.0.1 $SERVER_PORT $REGISTRY_PORT

printf '\n%b\n' "${GREEN}========== 测试 3: echo 10万 ==========${NC}"
"$BIN_DIR/benchmark_client_json" single echo 100000 0 0 $USE_DISCOVER 127.0.0.1 $SERVER_PORT $REGISTRY_PORT

printf '\n%b\n' "${GREEN}========== 测试 4: 吞吐量 10秒 ==========${NC}"
"$BIN_DIR/benchmark_client_json" throughput add 0 0 10 $USE_DISCOVER 127.0.0.1 $SERVER_PORT $REGISTRY_PORT

printf '\n%b\n' "${GREEN}========== 测试 5: 大 payload (echo 100KB x 1000 次) ==========${NC}"
"$BIN_DIR/benchmark_client_json" single echo 1000 0 0 $USE_DISCOVER 127.0.0.1 $SERVER_PORT $REGISTRY_PORT 100000

# steady 忽略 threads（单线程），逐秒输出 qps/latency，与 Protobuf 脚本的稳态档对齐
printf '\n%b\n' "${GREEN}========== 测试 6: 稳态 16B (${STEADY_SECS}s, 逐秒输出) ==========${NC}"
"$BIN_DIR/benchmark_client_json" steady echo 0 1 $STEADY_SECS $USE_DISCOVER 127.0.0.1 $SERVER_PORT $REGISTRY_PORT 16

printf '\n%b\n' "${GREEN}========== 测试 7: 稳态 64KB (${STEADY_SECS}s) ==========${NC}"
"$BIN_DIR/benchmark_client_json" steady echo 0 1 $STEADY_SECS $USE_DISCOVER 127.0.0.1 $SERVER_PORT $REGISTRY_PORT 65536

printf '\n%b\n' "${GREEN}========== 测试 8: 4 线程稳态 16B (${STEADY_SECS}s) ==========${NC}"
"$BIN_DIR/benchmark_client_json" multi echo 0 4 $STEADY_SECS $USE_DISCOVER 127.0.0.1 $SERVER_PORT $REGISTRY_PORT 16

printf '\n%b\n' "${GREEN}停止服务端...${NC}"
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null
printf '%b\n' "${GREEN}测试完成！${NC}"
