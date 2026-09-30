#!/usr/bin/env bash
set -euo pipefail

# ====== 自动检测项目根目录 ======
find_root() {
    local dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    while [ "$dir" != "/" ]; do
        if [ -f "$dir/rpc/CMakeLists.txt" ] || [ -f "$dir/.git" ]; then
            echo "$dir"; return 0
        fi
        dir="$(dirname "$dir")"
    done
    return 1
}
ROOT_DIR="$(find_root)"
if [ -z "$ROOT_DIR" ]; then
    echo "[ERROR] 找不到项目根目录" >&2
    exit 1
fi

BIN_DIR="${ROOT_DIR}/rpc/build/bin"

REG_PORT="${REG_PORT:-7070}"
PROVIDER_PORT="${PROVIDER_PORT:-8889}"
AUTO_KILL_PROVIDER="${AUTO_KILL_PROVIDER:-0}"

REG_BIN="${BIN_DIR}/test4_registry_server"
PROVIDER_BIN="${BIN_DIR}/test4_provider_server"
CONSUMER_BIN="${BIN_DIR}/test4_consumer_client"

if [[ ! -x "${REG_BIN}" || ! -x "${PROVIDER_BIN}" || ! -x "${CONSUMER_BIN}" ]]; then
  echo "[ERROR] 缺少服务发现 demo 可执行文件。请先编译："
  echo "  cd ${ROOT_DIR}/rpc && cmake --preset system -B build && cmake --build build -j\$(nproc)"
  exit 1
fi

mkdir -p "${ROOT_DIR}/demo-logs"
REG_LOG="${ROOT_DIR}/demo-logs/registry_${REG_PORT}.log"
PROVIDER_LOG="${ROOT_DIR}/demo-logs/provider_${PROVIDER_PORT}.log"
CONSUMER_LOG="${ROOT_DIR}/demo-logs/consumer.log"

cleanup() {
  set +e
  [[ -n "${CONSUMER_PID:-}" ]] && kill "${CONSUMER_PID}" >/dev/null 2>&1 || true
  [[ -n "${PROVIDER_PID:-}" ]] && kill "${PROVIDER_PID}" >/dev/null 2>&1 || true
  [[ -n "${REG_PID:-}" ]] && kill "${REG_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "== Demo: Registry + Discovery =="
echo "BIN_DIR: ${BIN_DIR}"
echo "日志目录：${ROOT_DIR}/demo-logs"
echo
echo ">>> 1) 启动注册中心（test4_registry_server）"
stdbuf -oL -eL "${REG_BIN}" >"${REG_LOG}" 2>&1 &
REG_PID=$!
echo "registry pid=${REG_PID}, log=${REG_LOG}"
sleep 1
echo

echo ">>> 2) 启动 Provider（test4_provider_server，自动注册到 registry）"
stdbuf -oL -eL "${PROVIDER_BIN}" >"${PROVIDER_LOG}" 2>&1 &
PROVIDER_PID=$!
echo "provider pid=${PROVIDER_PID}, log=${PROVIDER_LOG}"
sleep 2
echo

echo ">>> 3) 启动 Consumer（test4_consumer_client，发现服务并调用 add）"
stdbuf -oL -eL "${CONSUMER_BIN}" >"${CONSUMER_LOG}" 2>&1 &
CONSUMER_PID=$!
echo "consumer pid=${CONSUMER_PID}, log=${CONSUMER_LOG}"
echo

echo ">>> 4) 打印关键日志"
echo "----- registry (tail 30) -----"
tail -n 30 "${REG_LOG}" || true
echo "----- provider (tail 30) -----"
tail -n 30 "${PROVIDER_LOG}" || true
echo "----- consumer (tail 30) -----"
tail -n 30 "${CONSUMER_LOG}" || true
echo

if [[ "${AUTO_KILL_PROVIDER}" == "1" ]]; then
  echo ">>> 5) 自动演示：关闭 Provider -> 等待注册中心剔除（15s 级别）"
  echo "等待 Consumer 首次调用成功后再关闭 Provider（最多等待 12s）..."
  success=0
  for _ in {1..12}; do
    if tail -n 50 "${CONSUMER_LOG}" 2>/dev/null | grep -q "调用成功"; then
      success=1
      break
    fi
    sleep 1
  done
  if [[ "${success}" == "1" ]]; then
    kill "${PROVIDER_PID}" >/dev/null 2>&1 || true
    PROVIDER_PID=""
    echo "已关闭 Provider，等待 20s 观察 registry sweep/下线通知..."
    sleep 20
    echo
    echo "----- registry (tail 60) -----"
    tail -n 60 "${REG_LOG}" || true
    echo
  else
    echo "[WARN] 12s 内未检测到'调用成功'，为避免误杀 Provider，本次不自动下线。"
  fi
fi

echo "Demo 运行中。按 Ctrl+C 结束（脚本会自动清理进程）。"
wait
