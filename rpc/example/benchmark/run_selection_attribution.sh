#!/usr/bin/env bash
# ============================================================
# 选主机耗时占比：在真实 RpcClient 请求路径上采样
#
# 背景：benchmark_consistent_hash 的 86.6 ns/op 测的是
#   MethodHost::selectHost（单个函数、单独压）。
#   真实请求走的是 RpcClient::getCandidates → Discover::serviceDiscover
#   （锁 + map 查找 + selectHosts 建/排序/去重候选向量），
#   两者不是同一段代码，单函数微基准不能外推成 P99 占比。
#
# 用法:
#   bash run_selection_attribution.sh           # 跑一遍，打印选主机 P50/P99 与占比
#   bash run_selection_attribution.sh --check   # 断言采样点确实生效、且发现模式
#                                               # 的选主机延迟显著高于直连模式
#
# 注意：SHM 路径的 benchmark（example/shm）直接用 ShmClient，不经过
#   RpcClient/getCandidates，选主机占比恒为 0，不是本脚本的测量对象。
#   README 里 52µs 的 P99 正是 SHM 4 线程那一格。
# ============================================================
set -euo pipefail

CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

REGISTRY_PORT=7070      # test4_registry_server 固定监听 7070
SERVER_PORT=8889
SERVER_METRICS_PORT=9091
SINGLE_REQUESTS=${SINGLE_REQUESTS:-20000}
STEADY_SECS=${STEADY_SECS:-10}

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
    echo -e "${YELLOW}[ERROR] 找不到 build/bin，请先编译${NC}" >&2
    exit 1
fi
for bin in benchmark_server benchmark_client test4_registry_server; do
    if [ ! -x "$BIN_DIR/$bin" ]; then
        echo -e "${YELLOW}[ERROR] 找不到 $BIN_DIR/$bin，请先编译${NC}" >&2
        exit 1
    fi
done

OUT_DIR="$(mktemp -d)"
REG_PID=""
SRV_PID=""
cleanup() {
    [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null || true
    [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
    [ -n "$SRV_PID" ] && wait "$SRV_PID" 2>/dev/null || true
    [ -n "$REG_PID" ] && wait "$REG_PID" 2>/dev/null || true
    if [ "$CHECK_ONLY" -eq 0 ]; then
        echo -e "\n原始输出: $OUT_DIR"
    else
        rm -rf "$OUT_DIR"
    fi
}
trap cleanup EXIT

echo -e "${GREEN}启动注册中心 (port $REGISTRY_PORT)...${NC}"
"$BIN_DIR/test4_registry_server" >"$OUT_DIR/registry.log" 2>&1 &
REG_PID=$!
sleep 1
if ! kill -0 "$REG_PID" 2>/dev/null; then
    echo -e "${RED}[ERROR] 注册中心启动失败${NC}" >&2
    exit 1
fi

echo -e "${GREEN}启动服务端 (port $SERVER_PORT, 开启服务发现)...${NC}"
"$BIN_DIR/benchmark_server" "$SERVER_PORT" 1 "$REGISTRY_PORT" 0 "$SERVER_METRICS_PORT" \
    >"$OUT_DIR/server.log" 2>&1 &
SRV_PID=$!
sleep 2
if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo -e "${RED}[ERROR] 服务端启动失败${NC}" >&2
    exit 1
fi

run_client() { # $1=输出文件 $2=负载均衡策略（空=默认） 其余=client 参数
    local out="$1" strategy="$2"; shift 2
    if [ -n "$strategy" ]; then
        LCZ_PROFILE_SELECTION=1 LCZ_LB_STRATEGY="$strategy" \
            "$BIN_DIR/benchmark_client" "$@" >"$out" 2>&1
    else
        LCZ_PROFILE_SELECTION=1 "$BIN_DIR/benchmark_client" "$@" >"$out" 2>&1
    fi
}

# 86.6ns 的微基准测的是一致性哈希，所以这条路径单独跑一遍
echo -e "\n${GREEN}[1] 服务发现 + 一致性哈希：单线程 add x $SINGLE_REQUESTS${NC}"
run_client "$OUT_DIR/disc_consistent.txt" consistent_hash \
    single add "$SINGLE_REQUESTS" 0 0 1 127.0.0.1 "$SERVER_PORT" "$REGISTRY_PORT"
grep -E "负载均衡策略|采样次数|选主机延迟|占总耗时" "$OUT_DIR/disc_consistent.txt" || true

echo -e "\n${GREEN}[2] 服务发现 + 轮询：单线程 add x $SINGLE_REQUESTS${NC}"
run_client "$OUT_DIR/disc_single.txt" "" \
    single add "$SINGLE_REQUESTS" 0 0 1 127.0.0.1 "$SERVER_PORT" "$REGISTRY_PORT"
grep -E "负载均衡策略|采样次数|选主机延迟|占总耗时" "$OUT_DIR/disc_single.txt" || true

echo -e "\n${GREEN}[3] 服务发现 + 轮询：4 线程 echo 稳态 ${STEADY_SECS}s（16B）${NC}"
run_client "$OUT_DIR/disc_multi.txt" "" \
    multi echo 0 4 "$STEADY_SECS" 1 127.0.0.1 "$SERVER_PORT" "$REGISTRY_PORT" 16
grep -E "采样次数|选主机延迟|占总耗时" "$OUT_DIR/disc_multi.txt" || true

echo -e "\n${GREEN}[4] 直连模式对照：单线程 add x $SINGLE_REQUESTS${NC}"
run_client "$OUT_DIR/direct_single.txt" "" \
    single add "$SINGLE_REQUESTS" 0 0 0 127.0.0.1 "$SERVER_PORT" "$REGISTRY_PORT"
grep -E "采样次数|选主机延迟|占总耗时" "$OUT_DIR/direct_single.txt" || true

echo -e "\n${GREEN}[5] 观察者开销对照：服务发现单线程，不开采样${NC}"
"$BIN_DIR/benchmark_client" single add "$SINGLE_REQUESTS" 0 0 1 127.0.0.1 "$SERVER_PORT" "$REGISTRY_PORT" \
    >"$OUT_DIR/disc_single_noprof.txt" 2>&1
grep -E "平均值|P99:" "$OUT_DIR/disc_single_noprof.txt" || true

if [ "$CHECK_ONLY" -eq 0 ]; then
    echo -e "\n${GREEN}完成。占比口径：${NC}"
    echo "  sum(selection)/sum(total) 更稳（对长尾不敏感）"
    echo "  P99(selection)/P99(total) 是同一分位上的比值"
    exit 0
fi

# ==================== --check：只断言测量链路，不断言性能绝对值 ====================
fail() { echo -e "${RED}[FAIL] $*${NC}" >&2; exit 1; }

# 解析不到时返回空串而不是让 pipefail 把脚本直接带走，这样才能打印 FAIL 原因
samples_of()   { { sed -n 's/.*采样次数: \([0-9][0-9]*\).*/\1/p' "$1" | head -1; } || true; }
p50_of()       { { grep -oE 'P50=[0-9.]+' "$1" | head -1 | cut -d= -f2; } || true; }
p99_share_of() { { sed -n 's/.*P99(selection)\/P99(total)=\([0-9.][0-9.]*\)%.*/\1/p' "$1" | head -1; } || true; }

DISC_SAMPLES="$(samples_of "$OUT_DIR/disc_single.txt")"
CH_SAMPLES="$(samples_of "$OUT_DIR/disc_consistent.txt")"
MULTI_SAMPLES="$(samples_of "$OUT_DIR/disc_multi.txt")"
DIRECT_SAMPLES="$(samples_of "$OUT_DIR/direct_single.txt")"
DISC_P50="$(p50_of "$OUT_DIR/disc_single.txt")"
CH_P50="$(p50_of "$OUT_DIR/disc_consistent.txt")"
DIRECT_P50="$(p50_of "$OUT_DIR/direct_single.txt")"
P99_SHARE="$(p99_share_of "$OUT_DIR/disc_single.txt")"
CH_P99_SHARE="$(p99_share_of "$OUT_DIR/disc_consistent.txt")"

for pair in "disc_consistent:$CH_SAMPLES" "disc_single:$DISC_SAMPLES" "disc_multi:$MULTI_SAMPLES" "direct_single:$DIRECT_SAMPLES"; do
    name="${pair%%:*}"; val="${pair#*:}"
    [ -n "$val" ] || fail "$name 没解析到采样次数（getCandidates 采样点失效？）"
    [ "$val" -gt 0 ] || fail "$name 采样次数为 0（getCandidates 采样点失效？）"
done

# 单线程发现模式：每次逻辑调用一次 getCandidates，另有 1 次建连前的自检调用
[ "$DISC_SAMPLES" -ge "$SINGLE_REQUESTS" ] \
    || fail "发现模式采样次数 $DISC_SAMPLES 少于请求数 $SINGLE_REQUESTS"
[ "$CH_SAMPLES" -ge "$SINGLE_REQUESTS" ] \
    || fail "一致性哈希采样次数 $CH_SAMPLES 少于请求数 $SINGLE_REQUESTS"
[ "$DIRECT_SAMPLES" -ge "$SINGLE_REQUESTS" ] \
    || fail "直连模式采样次数 $DIRECT_SAMPLES 少于请求数 $SINGLE_REQUESTS"

# 发现模式每请求要持锁查 map + selectHosts；直连只 push 一个候选。
# 若两者相同，说明采样窗口落在错误的代码段（比如把整个 call 计时了）。
[ -n "$DISC_P50" ] && [ -n "$CH_P50" ] && [ -n "$DIRECT_P50" ] \
    || fail "没解析到选主机 P50（采样点没生效？）"
awk -v d="$DISC_P50" -v s="$DIRECT_P50" 'BEGIN { exit !(d > s) }' \
    || fail "发现模式 P50($DISC_P50 ns) 未高于直连($DIRECT_P50 ns)，采样窗口可能不在 getCandidates"
awk -v d="$CH_P50" -v s="$DIRECT_P50" 'BEGIN { exit !(d > s) }' \
    || fail "一致性哈希 P50($CH_P50 ns) 未高于直连($DIRECT_P50 ns)，采样窗口可能不在 getCandidates"

# 选主机是端到端的一部分，同分位比值必须 <100%；为 0 说明 P99 没采到
[ -n "$P99_SHARE" ] && [ -n "$CH_P99_SHARE" ] || fail "没解析到 P99(selection)/P99(total)"
awk -v r="$P99_SHARE" 'BEGIN { exit !(r > 0 && r < 100) }' \
    || fail "轮询 P99 占比 $P99_SHARE% 不在 (0,100) 内"
awk -v r="$CH_P99_SHARE" 'BEGIN { exit !(r > 0 && r < 100) }' \
    || fail "一致性哈希 P99 占比 $CH_P99_SHARE% 不在 (0,100) 内"

echo -e "${GREEN}[PASS] 采样次数 一致性哈希=$CH_SAMPLES 轮询=$DISC_SAMPLES 轮询4线程=$MULTI_SAMPLES 直连=$DIRECT_SAMPLES${NC}"
echo -e "${GREEN}[PASS] 发现模式 P50 轮询=${DISC_P50}ns 一致性哈希=${CH_P50}ns > 直连=${DIRECT_P50}ns${NC}"
echo -e "${GREEN}[PASS] P99 占比 轮询=${P99_SHARE}% 一致性哈希=${CH_P99_SHARE}% (0,100)${NC}"
