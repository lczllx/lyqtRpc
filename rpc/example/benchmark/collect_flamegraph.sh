#!/usr/bin/env bash
# ============================================================
# collect_flamegraph.sh — 起服务端 → perf 采样 → 跑压测 → 生成火焰图
#
# 用法（在 rpc/build 目录下执行：脚本在 example/benchmark/，二进制在 build/bin/）：
#   ../example/benchmark/collect_flamegraph.sh <名字> <服务端命令...> -- <压测命令...>
#
# 例（rpc/build 下）：
#   ../example/benchmark/collect_flamegraph.sh tcp_proto_small \
#     ./bin/benchmark_server 8889 0 8080 0 \
#     -- ./bin/benchmark_client throughput add 0 0 120 0 127.0.0.1 8889 8080 0
#
# 前置：
#   1. 已装 perf：sudo apt-get install -y linux-tools-$(uname -r)
#   2. 已装 FlameGraph：git clone https://github.com/brendangregg/FlameGraph ~/FlameGraph
#   3. 编译已带 -fno-omit-frame-pointer（rpc/CMakeLists.txt 已开），默认 --call-graph fp 即可；
#      要内核+libc 一起全解析（无 [unknown]）用 CALL_GRAPH="dwarf,16384"（慢）
#
# 输出：bench_result/<名字>_<时间戳>.svg（火焰图）+ 同名 .meta（git commit / 采样参数等上下文元数据）
# ============================================================
set -euo pipefail

FG_DIR="${FLAMEGRAPH_DIR:-$HOME/FlameGraph}"   # FlameGraph 脚本目录
OUT_DIR="${OUT_DIR:-bench_result}"             # 输出目录
FREQ="${PERF_FREQ:-99}"                        # 采样频率，短跑可 PERF_FREQ=999
CALL_GRAPH="${CALL_GRAPH:-fp}"                 # 回溯方式：fp（快，需 -fno-omit-frame-pointer）| dwarf,16384（慢但内核+libc 全解析、无 [unknown]）

# perf 权限：paranoid<=1 时普通用户即可采样（本机已配），否则退回 sudo
if [ "$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 3)" -le 1 ]; then
    SUDO=""
else
    SUDO="sudo"
fi

NAME="$1"; shift

# 以 "--" 分隔「服务端命令」和「压测命令」
SRV=(); CLI=(); CUR=srv
for a in "$@"; do
    [ "$a" = "--" ] && { CUR=cli; continue; }
    if [ "$CUR" = srv ]; then SRV+=("$a"); else CLI+=("$a"); fi
done

[ ${#SRV[@]} -gt 0 ] || { echo "错误：缺少服务端命令（-- 之前）"; exit 1; }
[ ${#CLI[@]} -gt 0 ] || { echo "错误：缺少压测命令（-- 之后）"; exit 1; }

mkdir -p "$OUT_DIR"

# 0. 采集上下文：时间戳 + git 信息，随火焰图一起落盘（before/after 对比要能溯源到 commit）
TS="$(date +%Y%m%d_%H%M%S)"
START_EPOCH="$(date +%s)"
GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
if [ -z "$(git status --porcelain 2>/dev/null)" ]; then GIT_DIRTY=no; else GIT_DIRTY=yes; fi

# 1. 起服务端。用 $! 直接拿服务端二进制 PID —— 别用 $() 捕获，
#    否则子 shell 退出会连带杀掉后台进程（SHM 踩过这个坑）。
"${SRV[@]}" >/dev/null 2>&1 &
SRV_PID=$!
trap 'kill $SRV_PID 2>/dev/null || true' EXIT
sleep 2

# 2. perf 采样整个服务端进程（含 worker 线程），-g + fp 取调用栈
$SUDO perf record -F "$FREQ" -g --call-graph "$CALL_GRAPH" -p "$SRV_PID" \
    -o "/tmp/perf_${NAME}.data" &
PERF_PID=$!

# 3. 跑压测（前台，持续到结束 → 采样窗口 = 压测时长）
"${CLI[@]}"

# 4. 停采样、停服务端
$SUDO kill -INT "$PERF_PID" 2>/dev/null || true
sleep 1
kill "$SRV_PID" 2>/dev/null || true
trap - EXIT

# 5. 折叠 + 出图
$SUDO perf script -i "/tmp/perf_${NAME}.data" > "/tmp/perf_${NAME}.script"
"$FG_DIR/stackcollapse-perf.pl" "/tmp/perf_${NAME}.script" > "/tmp/perf_${NAME}.folded"

# 可选：DROP_KERNEL=1 时过滤掉 [unknown] 帧。非 root 采样时，内核地址(ffffffff...)
# 和 libc 内部符号都解析不了、显示为 [unknown]；过滤后只留用户态。
# 改造只动用户态，对比火焰图反而更清晰；想连内核一起看就用 sudo 跑，不用过滤。
if [ "${DROP_KERNEL:-0}" = "1" ]; then
    awk '{ n=split($1,f,";"); out=""; for(i=1;i<=n;i++){ if(f[i]!="[unknown]"){ out=(out==""?f[i]:out";"f[i]); } } if(out!="") print out, $2; }' \
        "/tmp/perf_${NAME}.folded" > "/tmp/perf_${NAME}.filtered"
    mv "/tmp/perf_${NAME}.filtered" "/tmp/perf_${NAME}.folded"
fi

"$FG_DIR/flamegraph.pl" "/tmp/perf_${NAME}.folded" > "$OUT_DIR/${NAME}_${TS}.svg"

# 6. 落盘元数据（同名 .meta）：硬件 / git / 采样参数，供 before/after 对比溯源
END_EPOCH="$(date +%s)"
DURATION=$((END_EPOCH - START_EPOCH))
CPU_MODEL="$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | xargs || true)"
PERF_VER="$(perf --version 2>/dev/null | head -1 || true)"

cat > "$OUT_DIR/${NAME}_${TS}.meta" <<EOF
name: ${NAME}
timestamp: $(date '+%Y-%m-%d %H:%M:%S')
duration_sec: ${DURATION}
git_commit: ${GIT_COMMIT}
git_branch: ${GIT_BRANCH}
git_dirty: ${GIT_DIRTY}
kernel: $(uname -r)
cpu: ${CPU_MODEL}
cores: $(nproc)
perf: ${PERF_VER}
perf_freq: ${FREQ}
call_graph: ${CALL_GRAPH}
drop_kernel: ${DROP_KERNEL:-0}
server_pid: ${SRV_PID}
server_cmd: ${SRV[*]}
client_cmd: ${CLI[*]}
output: ${OUT_DIR}/${NAME}_${TS}.svg
EOF

echo "✅ $OUT_DIR/${NAME}_${TS}.svg"
echo "📋 $OUT_DIR/${NAME}_${TS}.meta"
