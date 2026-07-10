#!/usr/bin/env bash
# ============================================================
# SHM (共享内存) RPC 性能测试 — JSON + FlatBuffers 零拷贝对比
# 前置: 已编译项目
# 用法: bash run_shm_benchmark.sh          # 运行全部（JSON + FlatBuf）
#       bash run_shm_benchmark.sh json      # 仅 JSON
#       bash run_shm_benchmark.sh flat      # 仅 FlatBuffers 零拷贝
# ============================================================
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

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
    echo -e "${YELLOW}[ERROR] 找不到 build/bin 目录，请先编译${NC}"
    exit 1
fi

TEST_REQUESTS=20000
MODE="${1:-all}"  # all / json / flat

# 启动服务端 + 等待就绪
# 注意: 不能通过 $() 返回 PID（子 shell 退出会杀后台进程），用全局变量 _SRV_PID
_SRV_PID=""
start_server() {
    local srv="$1" shm="$2"
    rm -f /dev/shm/$shm "$shm"*_notify 2>/dev/null || true
    "$srv" >/dev/null 2>&1 &
    _SRV_PID=$!
    for i in $(seq 1 30); do
        if [ -f /dev/shm/$shm ] 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    kill $_SRV_PID 2>/dev/null || true
    echo -e "${YELLOW}[ERROR] 服务端启动超时 (30×0.1s): SHM /dev/shm/$shm 未创建${NC}" >&2
    _SRV_PID=""
    return 1
}

# 停止服务端 + 清理 SHM
stop_server() {
    local shm="$1"
    if [ -n "$_SRV_PID" ]; then
        kill $_SRV_PID 2>/dev/null || true
        wait $_SRV_PID 2>/dev/null || true
        _SRV_PID=""
    fi
    rm -f /dev/shm/$shm "$shm"*_notify 2>/dev/null || true
}

# 单轮测试: 启动服务端 → 跑测试 → 停止服务端
run_one_round() {
    local label="$1" srv="$2" cli="$3" shm="$4" mode="$5" method="$6" req="$7"
    shift 7

    if ! start_server "$srv" "$shm"; then return 1; fi

    echo -e "\n${GREEN}${label}: ${mode} (${method}, ${req}次)${NC}"
    "$cli" $mode $method $req "$@" 2>&1 || true

    stop_server "$shm"
}

# ========== JSON 路径测试 ==========
run_json_bench() {
    local SRV="$BIN_DIR/shm_benchmark_server"
    local CLI="$BIN_DIR/shm_benchmark_client"
    local SHM_NAME="lcz_shm_bench"

    for bin in "$SRV" "$CLI"; do
        if [ ! -f "$bin" ]; then
            echo -e "${YELLOW}[SKIP] 找不到 $bin${NC}"; return
        fi
    done

    echo -e "\n${BOLD}${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}${CYAN}║    SHM JSON 序列化 RPC 性能测试                          ║${NC}"
    echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"

    run_one_round "[单线程]" "$SRV" "$CLI" "$SHM_NAME" single   add $TEST_REQUESTS
    run_one_round "[多线程]" "$SRV" "$CLI" "$SHM_NAME" multi    add $TEST_REQUESTS 4
    run_one_round "[吞吐量]" "$SRV" "$CLI" "$SHM_NAME" throughput add 0 0 10
}

# ========== FlatBuffers 零拷贝路径测试 ==========
run_flat_bench() {
    local SRV="$BIN_DIR/shm_benchmark_server_zc"
    local CLI="$BIN_DIR/shm_benchmark_client_zc"
    local SHM_NAME="lcz_shm_bench_zc"

    for bin in "$SRV" "$CLI"; do
        if [ ! -f "$bin" ]; then
            echo -e "${YELLOW}[SKIP] 找不到 $bin（FlatBuffers 未编译或不可用）${NC}"; return
        fi
    done

    echo -e "\n${BOLD}${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}${CYAN}║  SHM FlatBuffers 零拷贝 RPC 性能测试                     ║${NC}"
    echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"

    run_one_round "[单线程]" "$SRV" "$CLI" "$SHM_NAME" single   add $TEST_REQUESTS
    run_one_round "[多线程]" "$SRV" "$CLI" "$SHM_NAME" multi    add $TEST_REQUESTS 4
    run_one_round "[吞吐量]" "$SRV" "$CLI" "$SHM_NAME" throughput add 0 0 10
}

# ====== 执行 ======
case "$MODE" in
    json)  run_json_bench ;;
    flat)  run_flat_bench ;;
    all)
        run_json_bench
        run_flat_bench
        echo ""
        echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
        echo -e "${BOLD}${CYAN}║  JSON vs FlatBuffers 零拷贝 对比完成！                   ║${NC}"
        echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
        ;;
    *) echo "用法: bash run_shm_benchmark.sh [json|flat|all]" ;;
esac

echo ""
echo -e "${YELLOW}对比参考:${NC}"
echo -e "  TCP Proto 4线程: QPS ~39,246  P50 ~92μs"
echo -e "  SHM JSON:         QPS > TCP  P50 3-15μs"
echo -e "  SHM FlatBuf ZC:   QPS > JSON P50 <5μs (零拷贝)"
