#!/usr/bin/env bash
# ============================================================
# RPC 框架 — 完整性能测试脚本
# 功能: 多并发梯度 QPS + Latency 分位数 (P50/P99/P999)
#       JSON vs Protobuf 对比 + 大 payload 测试
# 用法: bash run_full_benchmark.sh
# 输出: 终端彩色表格 + /tmp/rpc_bench_result.txt
# ============================================================
set -e

# ====== 配置 ======
SERVER_PORT=8889
REGISTRY_PORT=8080
USE_DISCOVER=0
RESULT_FILE="/tmp/rpc_bench_result.txt"

# 颜色
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ====== 自动检测 bin 目录 ======
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
find_bin() {
    local dir="$SCRIPT_DIR"
    while [ "$dir" != "/" ]; do
        if [ -f "$dir/CMakeCache.txt" ] && [ -d "$dir/bin" ]; then echo "$dir/bin"; return 0; fi
        if [ -d "$dir/build/bin" ]; then echo "$dir/build/bin"; return 0; fi
        dir="$(dirname "$dir")"
    done
    return 1
}
BIN_DIR="$(find_bin)"
if [ -z "$BIN_DIR" ]; then
    echo -e "${YELLOW}[ERROR] 找不到 build/bin 目录，请先编译项目${NC}"
    exit 1
fi

SERVER_BIN="$BIN_DIR/benchmark_server"
SERVER_JSON_BIN="$BIN_DIR/benchmark_server_json"
CLIENT_BIN="$BIN_DIR/benchmark_client"
CLIENT_JSON_BIN="$BIN_DIR/benchmark_client_json"

# ====== 检查可执行文件 ======
for bin in "$SERVER_BIN" "$SERVER_JSON_BIN" "$CLIENT_BIN" "$CLIENT_JSON_BIN"; do
    if [ ! -f "$bin" ]; then
        echo -e "${YELLOW}[ERROR] 找不到 $bin，请先编译${NC}"
        exit 1
    fi
done

# ====== 工具函数 ======
start_server() {
    local bin="$1"
    "$bin" $SERVER_PORT $USE_DISCOVER $REGISTRY_PORT 0 &
    SERVER_PID=$!
    sleep 2
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo -e "${YELLOW}[ERROR] 服务端启动失败${NC}"
        exit 1
    fi
}

stop_server() {
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

# 跑一轮测试，返回 QPS 和 P50/P99/P999
run_test() {
    local type="$1" method="$2" requests="$3" threads="$4" duration="$5" payload="$6"
    local output
    output=$("$CLIENT_BIN" "$type" "$method" "$requests" "$threads" "$duration" "$USE_DISCOVER" 127.0.0.1 "$SERVER_PORT" "$REGISTRY_PORT" "$payload" 2>/dev/null || true)
    echo "$output"
}

run_test_json() {
    local type="$1" method="$2" requests="$3" threads="$4" duration="$5" payload="$6"
    local output
    output=$("$CLIENT_JSON_BIN" "$type" "$method" "$requests" "$threads" "$duration" "$USE_DISCOVER" 127.0.0.1 "$SERVER_PORT" "$REGISTRY_PORT" "$payload" 2>/dev/null || true)
    echo "$output"
}

# 从测试输出中提取指标
extract() {
    local output="$1" field="$2"
    echo "$output" | grep "$field" | head -1 | sed 's/.*: *//' | tr -d ' us'
}

# ====== 清理 ======
trap stop_server EXIT
> "$RESULT_FILE"

echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║       RPC 框架 — 完整性能测试                            ║"
echo "║       环境: $(nproc 2>/dev/null || echo '?')C  $(free -h | awk '/^Mem:/{print $2}')  $(uname -r)                    ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ==========================================
# 1. Protobuf 并发梯度测试
# ==========================================
echo -e "\n${BOLD}${GREEN}[1/4] Protobuf — 并发梯度 (小 payload add)${NC}"
start_server "$SERVER_BIN"

echo "" | tee -a "$RESULT_FILE"
echo "=== Protobuf 小 payload (add) 并发梯度 ===" | tee -a "$RESULT_FILE"
printf "%-8s %-12s %-12s %-12s %-12s %-8s\n" "线程" "QPS" "P50(us)" "P99(us)" "P999(us)" "成功率" | tee -a "$RESULT_FILE"
printf "%-8s %-12s %-12s %-12s %-12s %-8s\n" "----" "----" "-------" "-------" "--------" "-----" | tee -a "$RESULT_FILE"

for threads in 1 4 8 16; do
    output=$(run_test multi add 50000 "$threads" 0 0)
    qps=$(extract "$output" "QPS")
    p50=$(extract "$output" "P50")
    p99=$(extract "$output" "P99")
    p999=$(extract "$output" "P999")
    success=$(extract "$output" "成功率")
    printf "%-8s %-12s %-12s %-12s %-12s %-8s\n" "$threads" "$qps" "$p50" "$p99" "$p999" "$success" | tee -a "$RESULT_FILE"
done
stop_server

# ==========================================
# 2. JSON 并发梯度测试
# ==========================================
echo -e "\n${BOLD}${GREEN}[2/4] JSON — 并发梯度 (小 payload add)${NC}"
start_server "$SERVER_JSON_BIN"

echo "" | tee -a "$RESULT_FILE"
echo "=== JSON 小 payload (add) 并发梯度 ===" | tee -a "$RESULT_FILE"
printf "%-8s %-12s %-12s %-12s %-12s %-8s\n" "线程" "QPS" "P50(us)" "P99(us)" "P999(us)" "成功率" | tee -a "$RESULT_FILE"
printf "%-8s %-12s %-12s %-12s %-12s %-8s\n" "----" "----" "-------" "-------" "--------" "-----" | tee -a "$RESULT_FILE"

for threads in 1 4 8 16; do
    output=$(run_test_json multi add 50000 "$threads" 0 0)
    qps=$(extract "$output" "QPS")
    p50=$(extract "$output" "P50")
    p99=$(extract "$output" "P99")
    p999=$(extract "$output" "P999")
    success=$(extract "$output" "成功率")
    printf "%-8s %-12s %-12s %-12s %-12s %-8s\n" "$threads" "$qps" "$p50" "$p99" "$p999" "$success" | tee -a "$RESULT_FILE"
done
stop_server

# ==========================================
# 3. 大 payload 对比 (100KB echo)
# ==========================================
echo -e "\n${BOLD}${GREEN}[3/4] 大 payload 对比 — echo 100KB x 1000 次${NC}"

echo "" | tee -a "$RESULT_FILE"
echo "=== 大 payload (echo 100KB x 1000) 对比 ===" | tee -a "$RESULT_FILE"
printf "%-12s %-12s %-12s %-12s %-12s %-12s\n" "序列化" "QPS" "P50(us)" "P99(us)" "P999(us)" "P99(ms)" | tee -a "$RESULT_FILE"
printf "%-12s %-12s %-12s %-12s %-12s %-12s\n" "------" "----" "-------" "-------" "--------" "------" | tee -a "$RESULT_FILE"

# Protobuf 大payload
start_server "$SERVER_BIN"
output=$(run_test single echo 1000 0 0 100000)
qps=$(extract "$output" "QPS")
p50=$(extract "$output" "P50")
p99=$(extract "$output" "P99")
p999=$(extract "$output" "P999")
p99ms=$(awk "BEGIN {printf \"%.2f\", $p99/1000}")
printf "%-12s %-12s %-12s %-12s %-12s %-12s\n" "Protobuf" "$qps" "$p50" "$p99" "$p999" "${p99ms}ms" | tee -a "$RESULT_FILE"
stop_server

# JSON 大payload
start_server "$SERVER_JSON_BIN"
output=$(run_test_json single echo 1000 0 0 100000)
qps=$(extract "$output" "QPS")
p50=$(extract "$output" "P50")
p99=$(extract "$output" "P99")
p999=$(extract "$output" "P999")
p99ms=$(awk "BEGIN {printf \"%.2f\", $p99/1000}")
printf "%-12s %-12s %-12s %-12s %-12s %-12s\n" "JSON" "$qps" "$p50" "$p99" "$p999" "${p99ms}ms" | tee -a "$RESULT_FILE"
stop_server

# ==========================================
# 4. 吞吐量测试 (10s 持续压测)
# ==========================================
echo -e "\n${BOLD}${GREEN}[4/4] 吞吐量 — 10 秒持续压测 (add)${NC}"

echo "" | tee -a "$RESULT_FILE"
echo "=== 吞吐量 10s 持续压测 ===" | tee -a "$RESULT_FILE"
printf "%-12s %-12s %-12s %-12s %-12s\n" "序列化" "QPS" "P50(us)" "P99(us)" "P999(us)" | tee -a "$RESULT_FILE"
printf "%-12s %-12s %-12s %-12s %-12s\n" "------" "----" "-------" "-------" "--------" | tee -a "$RESULT_FILE"

start_server "$SERVER_BIN"
output=$(run_test throughput add 0 0 10 0)
qps=$(extract "$output" "QPS")
p50=$(extract "$output" "P50")
p99=$(extract "$output" "P99")
p999=$(extract "$output" "P999")
printf "%-12s %-12s %-12s %-12s %-12s\n" "Protobuf" "$qps" "$p50" "$p99" "$p999" | tee -a "$RESULT_FILE"
stop_server

start_server "$SERVER_JSON_BIN"
output=$(run_test_json throughput add 0 0 10 0)
qps=$(extract "$output" "QPS")
p50=$(extract "$output" "P50")
p99=$(extract "$output" "P99")
p999=$(extract "$output" "P999")
printf "%-12s %-12s %-12s %-12s %-12s\n" "JSON" "$qps" "$p50" "$p99" "$p999" | tee -a "$RESULT_FILE"
stop_server

# ====== 完成 ======
echo ""
echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${CYAN}║  测试完成！结果已保存到: ${RESULT_FILE}${NC}"
echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}提示: 将以上 QPS 和 P50/P99/P999 数据填入简历${NC}"
echo -e "${YELLOW}      建议取 4 线程并发下的 Protobuf 数据作为简历数值${NC}"
