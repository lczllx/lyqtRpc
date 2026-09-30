#!/usr/bin/env bash
# ============================================================
# 一致性哈希负载均衡微基准
#
# 用法:
#   bash run_consistent_hash_bench.sh           # 跑一遍，结果存到 bench_result/
#   bash run_consistent_hash_bench.sh --check   # 只断言 [3]/[4] 的确定性指标（CI 用）
#
# 说明: 二进制不接受参数，主机数/key 数在源码里写死
#       （benchmark_consistent_hash.cc 的 HOSTS=10、KEYS=100000）。
#       [1] 单次延迟、[2] 哈希环构建耗时是计时项，跨机/跨次会抖，
#       只适合同机 before/after 对比；[3] 分布均衡性、[4] 增删节点
#       重映射率是纯逻辑，两次运行字节一致，故断言只挂在这两项上。
# ============================================================
set -euo pipefail

CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

# ====== 自动检测 build/bin 路径 ======
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
BIN="$BIN_DIR/benchmark_consistent_hash"
if [ -z "$BIN_DIR" ] || [ ! -x "$BIN" ]; then
    echo -e "${YELLOW}[ERROR] 找不到 benchmark_consistent_hash，请先编译项目${NC}" >&2
    exit 1
fi

# 分布均衡性阈值: 标准差/均值 上限。160 虚拟节点下实测约 7.6%；
# 退化成「每台一个点」会跳到 90% 量级，15% 既能拦住退化又留了 2 倍余量。
MAX_CV=0.15
# max:min 上限（实测 1.34）
MAX_SKEW=2.0
# 增删节点重映射率上下限。一致性哈希理论值 1/(n+1)、1/n，即 20% / 25%；
# 若策略被误改回取模，该值会跳到 75%~80%。取模列反向断言 ≥60%。
MAX_REMAP=35
MIN_MODULO_REMAP=60

OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT
"$BIN" > "$OUT" 2>&1

# ====== 提取 [3] 分布均衡性 ======
DIST_LINE="$(grep '每台均值=' "$OUT" || true)"
MEAN="$(echo "$DIST_LINE" | grep -oE '每台均值=[0-9.]+' | cut -d= -f2)"
STD="$(echo "$DIST_LINE"  | grep -oE '标准差=[0-9.]+'   | cut -d= -f2)"
SKEW="$(echo "$DIST_LINE" | grep -oE 'max:min=[0-9.]+'  | cut -d= -f2)"

# ====== 提取 [4] 增删节点重映射率 ======
# 每行形如: 加 1 台(4→5): 一致性哈希 1928/10000 (19.3%)  vs  取模哈希 8008/10000 (80.1%)
# 取该行前两个百分数，依次是一致性哈希、取模哈希
remap_pct() {  # $1=行的匹配串 $2=第几个百分数
    grep "$1" "$OUT" | grep -oE '\([0-9.]+%\)' | tr -d '()%' | sed -n "$2p" || true
}
ADD_CH="$(remap_pct '加 1 台' 1)"
ADD_MOD="$(remap_pct '加 1 台' 2)"
DEL_CH="$(remap_pct '删 1 台' 1)"
DEL_MOD="$(remap_pct '删 1 台' 2)"

# 解析不到就先报错退出，否则下面 awk 会拿空串算出语法错误，
# 报出来的是一堆 awk 报错而不是「输出格式变了」
for var in MEAN STD SKEW ADD_CH ADD_MOD DEL_CH DEL_MOD; do
    if [ -z "${!var}" ]; then
        echo -e "${RED}[ERROR] 无法从输出中解析 $var，二进制输出格式可能已变：${NC}" >&2
        cat "$OUT" >&2
        exit 1
    fi
done

fails=0
check() {  # $1=描述 $2=awk 条件(1=通过) $3=实测值 $4=阈值说明
    if [ "$(awk "BEGIN{print ($2) ? 1 : 0}")" = "1" ]; then
        echo -e "  ${GREEN}[OK]${NC} $1 = $3 （要求 $4）"
    else
        echo -e "  ${RED}[X]${NC} $1 = $3 （要求 $4）"
        fails=$((fails + 1))
    fi
}

echo -e "${BOLD}一致性哈希确定性指标断言${NC}"
check "分布标准差/均值"  "$STD/$MEAN <= $MAX_CV"            "$(awk "BEGIN{printf \"%.1f%%\", $STD/$MEAN*100}")" "≤ $(awk "BEGIN{printf \"%.0f%%\", $MAX_CV*100}")"
check "分布 max:min"     "$SKEW <= $MAX_SKEW"               "$SKEW"     "≤ $MAX_SKEW"
check "加1台 一致性哈希重映射率" "$ADD_CH <= $MAX_REMAP"     "$ADD_CH%"  "≤ $MAX_REMAP%"
check "加1台 取模哈希重映射率"   "$ADD_MOD >= $MIN_MODULO_REMAP" "$ADD_MOD%" "≥ $MIN_MODULO_REMAP%"
check "删1台 一致性哈希重映射率" "$DEL_CH <= $MAX_REMAP"     "$DEL_CH%"  "≤ $MAX_REMAP%"
check "删1台 取模哈希重映射率"   "$DEL_MOD >= $MIN_MODULO_REMAP" "$DEL_MOD%" "≥ $MIN_MODULO_REMAP%"

if [ "$fails" -ne 0 ]; then
    echo -e "\n${RED}${BOLD}$fails 项断言失败${NC}" >&2
    cat "$OUT" >&2
    exit 1
fi
echo -e "${GREEN}${BOLD}全部通过${NC}"

# --check 到此为止；本地跑则保存完整输出，附 git 上下文便于溯源
[ "$CHECK_ONLY" = "1" ] && exit 0

OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/bench_result}"
mkdir -p "$OUT_DIR"
TS="$(date +%Y%m%d_%H%M%S)"
RESULT="$OUT_DIR/consistent_hash_${TS}.txt"
{
    cat "$OUT"
    echo "--- 运行上下文 ---"
    echo "timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "git_commit: $(git -C "$SCRIPT_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "git_dirty: $([ -z "$(git -C "$SCRIPT_DIR" status --porcelain 2>/dev/null)" ] && echo no || echo yes)"
    echo "cpu: $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | xargs || true)"
    echo "cores: $(nproc 2>/dev/null || echo '?')"
} > "$RESULT"

echo ""
cat "$OUT"
echo -e "${GREEN}结果已保存: $RESULT${NC}"
