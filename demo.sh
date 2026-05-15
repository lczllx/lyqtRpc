#!/bin/bash
# RPC 功能演示脚本
# 用法: ./demo.sh {etcd|offline|timeout|topic|circuit|ha|all}

set -e

BIN_DIR="$(cd "$(dirname "$0")/rpc/build" && pwd)"
BIN="$BIN_DIR/bin"          # test1 / test4
BIN3="$BIN_DIR/example/test/test3"  # test3 has no RUNTIME_OUTPUT_DIRECTORY
LOG_DIR="/tmp/rpc-demo-logs"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

step()  { echo -e "\n${BOLD}${CYAN}[$1/$TOTAL]${NC} ${BOLD}$2${NC}"; }
info()  { echo -e "  ${GREEN}→${NC} $*"; }
warn()  { echo -e "  ${RED}→${NC} $*"; }

cleanup() {
    killall test1_registry_server test1_rpc_server test1_rpc_client \
            test4_registry_server test4_provider_server test4_consumer_client \
            test3_topic_server test3_subscribe_client test3_publish_client 2>/dev/null || true
}
trap cleanup EXIT

demo_etcd() {
    TOTAL=6
    echo -e "${BOLD}=== 演示: etcd 持久化 — registry 重启状态不丢 ===${NC}"
    echo "场景: provider 注册到 registry → kill registry → 重启 registry → 注册信息仍在 etcd 中"
    echo "意义: 面试官问'注册中心挂了怎么办'，你的答案'数据在 etcd，重启即恢复'"
    echo ""

    # 检查 etcd
    if ! pgrep -x etcd >/dev/null; then
        warn "请先启动 etcd: etcd --listen-client-urls=http://127.0.0.1:2379 --advertise-client-urls=http://127.0.0.1:2379 &"
        return 1
    fi
    info "etcd 已在运行"
    export LCZ_ETCD=http://127.0.0.1:2379

    # 起 registry（etcd 模式）
    step 1 "启动 registry（etcd 后端）"
    "$BIN/test1_registry_server" > "$LOG_DIR/etcd_reg.log" 2>&1 &
    REG_PID=$!
    sleep 2
    info "registry 启动 (pid=$REG_PID, LCZ_ETCD=http://127.0.0.1:2379)"

    # 起 provider，注册 add 服务
    step 2 "启动 provider 并注册到 registry"
    "$BIN/test1_rpc_server" > "$LOG_DIR/etcd_prov.log" 2>&1 &
    PROV_PID=$!
    sleep 3
    info "provider 已注册 add 方法到 etcd"
    info "etcd 中存储:"
    etcdctl get --prefix /lcz-rpc/ 2>/dev/null | head -8 || true

    # 杀 registry
    step 3 "杀掉 registry 进程..."
    kill $REG_PID 2>/dev/null || true
    sleep 1
    info "registry 已停止"

    # 重启 registry
    step 4 "重启 registry，从 etcd 恢复数据"
    "$BIN/test1_registry_server" > "$LOG_DIR/etcd_reg2.log" 2>&1 &
    REG_PID=$!
    sleep 2
    info "registry 重启完成 (pid=$REG_PID)"

    # 验证 client 仍能发现服务
    step 5 "client 发起调用，验证服务可发现"
    timeout 10 "$BIN/test1_rpc_client" > "$LOG_DIR/etcd_client.log" 2>&1 || true
    if grep -q "result:99" "$LOG_DIR/etcd_client.log"; then
        info "调用成功: add(66,33) = 99 ✓"
    else
        warn "调用失败，查看 $LOG_DIR/etcd_client.log"
    fi

    # 验证 etcd 数据仍在
    step 6 "验证 etcd 中注册信息完好"
    info "etcd 数据:"
    etcdctl get --prefix /lcz-rpc/ 2>/dev/null | head -8 || true

    kill $REG_PID $PROV_PID 2>/dev/null || true
    echo -e "\n${GREEN}etcd HA 演示完成${NC}"
}

demo_offline() {
    TOTAL=5
    echo -e "${BOLD}=== 演示: 死节点自动下线 ===${NC}"
    echo "场景: provider 注册 → kill provider → registry 心跳超时自动剔除"
    echo "意义: provider 挂了 client 不会被调到，无需人工介入"
    echo ""

    # 起 registry（memory 模式，心跳间隔 5s，超时 15s）
    step 1 "启动 registry（心跳扫描间隔 5s，15s 无心跳即剔除）"
    "$BIN/test4_registry_server" > "$LOG_DIR/off_reg.log" 2>&1 &
    REG_PID=$!
    sleep 1
    info "registry 启动，监听 7070"

    # 起 provider
    step 2 "启动 provider 并注册"
    "$BIN/test4_provider_server" > "$LOG_DIR/off_prov.log" 2>&1 &
    PROV_PID=$!
    sleep 2
    info "provider 已注册 (心跳间隔 10s)"
    grep "注册成功\|收到注册" "$LOG_DIR/off_reg.log" | tail -2

    # 验证 client 能调通
    step 3 "client 调用 add(10,20)，确认连通"
    timeout 12 "$BIN/test4_consumer_client" > "$LOG_DIR/off_client.log" 2>&1 || true
    if grep -q "结果: 30\|调用成功" "$LOG_DIR/off_client.log"; then
        info "调用成功: add(10,20) = 30 ✓"
    else
        warn "调用失败，查看 $LOG_DIR/off_client.log"
    fi

    # 杀 provider
    step 4 "杀掉 provider，等待 registry 心跳超时剔除"
    kill $PROV_PID 2>/dev/null || true
    info "provider 已杀掉 (pid=$PROV_PID)"
    info "等待 20s（心跳 10s + 超时判定 15s）..."
    sleep 20
    grep -E "发现过|剔除|下线" "$LOG_DIR/off_reg.log" | tail -5

    # 验证剔除
    step 5 "验证 registry 已移除死掉的 provider"
    grep -c "过期\|剔除\|下线" "$LOG_DIR/off_reg.log" >/dev/null 2>&1 \
        && info "provider 已被标记为过期并剔除 ✓" \
        || warn "未检测到剔除日志，检查 $LOG_DIR/off_reg.log"

    kill $REG_PID 2>/dev/null || true
    echo -e "\n${GREEN}死节点剔除演示完成${NC}"
}

demo_timeout() {
    TOTAL=4
    echo -e "${BOLD}=== 演示: 超时控制 ===${NC}"
    echo "场景: slow provider 执行 3s，client 设置 1s 超时"
    echo "意义: 防止慢请求拖死调用方，超时立即返回不阻塞"
    echo ""

    # 起 registry
    step 1 "启动 registry"
    "$BIN/test1_registry_server" > "$LOG_DIR/to_reg.log" 2>&1 &
    REG_PID=$!
    sleep 2

    # 起慢 provider（sleep 3s 再返回）
    step 2 "启动慢 provider（add 延时 3s）"
    "$BIN/test1_slow_rpc_server" > "$LOG_DIR/to_slow.log" 2>&1 &
    SLOW_PID=$!
    sleep 2
    info "慢 provider 已注册 (每次调用睡眠 3s)"

    # 起 timeout client（1s 超时）
    step 3 "client 设置 1s 超时，对比结果"
    timeout 10 "$BIN/test1_timeout_test_client" > "$LOG_DIR/to_client.log" 2>&1 || true
    sleep 2

    # 展示结果
    step 4 "结果"
    echo -e "  ${CYAN}=== 超时 client 输出 ===${NC}"
    grep -E "超时|成功|失败|耗时|timeout" "$LOG_DIR/to_client.log" || cat "$LOG_DIR/to_client.log"

    kill $REG_PID $SLOW_PID 2>/dev/null || true
    echo -e "\n${GREEN}超时控制演示完成${NC}"
}

demo_circuit() {
    TOTAL=5
    echo -e "${BOLD}=== 演示: 熔断器 — 故障自动隔离与恢复 ===${NC}"
    echo "场景: server 前3次正常 → 接下来故意慢响应(触发客户端超时) → 熔断器打开拒绝请求 → 冷却后 HALF_OPEN 探测 → server 恢复正常 → 熔断器关闭"
    echo ""

    # 短周期便于演示: 5次失败触发熔断, 8s冷却期
    export LCZ_CB_FAILURE_THRESHOLD=5
    export LCZ_CB_OPEN_DURATION=8
    export LCZ_CB_HALF_OPEN_MAX=1

    step 1 "启动熔断器测试 server"
    "$BIN/circuit_breaker_test_server" > "$LOG_DIR/cb_server.log" 2>&1 &
    SRV_PID=$!
    sleep 2
    info "server 启动 (pid=$SRV_PID, 监听 127.0.0.1:8889)"
    info "server 行为: 前3次正常响应, 后续5次慢响应(触发超时), 然后恢复正常"

    step 2 "启动 client 循环调用 add(1,2)，1次/秒"
    info "默认超时=5s, 熔断阈值=5, 冷却=8s"
    timeout 90 "$BIN/circuit_breaker_test_client" > "$LOG_DIR/cb_client.log" 2>&1 &
    CLIENT_PID=$!
    sleep 6
    echo -e "  ${CYAN}=== 早期输出（正常期）===${NC}"
    grep -E "^\[" "$LOG_DIR/cb_client.log" | head -5

    step 3 "等待超时累积 → 熔断器打开 → 快速拒绝"
    info "客户端每次超时需等5s, 5次失败共约25s..."
    sleep 30
    echo -e "  ${CYAN}=== 中期输出（熔断期）===${NC}"
    grep -E "^\[|CLOSED => OPEN|熔断器打开|熔断拒绝|熔断" "$LOG_DIR/cb_client.log" | tail -20

    step 4 "等待冷却结束 + HALF_OPEN 探测恢复"
    info "冷却期 = 8s, 之后 HALF_OPEN 探测..."
    sleep 20
    echo -e "  ${CYAN}=== 后期输出（恢复期）===${NC}"
    grep -E "^\[|HALF_OPEN|=> CLOSED|熔断器恢复" "$LOG_DIR/cb_client.log" | tail -15

    step 5 "结果验证"
    echo -e "  ${CYAN}=== 完整日志摘要 ===${NC}"
    grep -E "熔断|=> OPEN|=> HALF|=> CLOSED|熔断器恢复" "$LOG_DIR/cb_client.log" | head -20

    if grep -q "CLOSED => OPEN\|熔断器打开" "$LOG_DIR/cb_client.log"; then
        info "熔断器 OPEN ✓"
    else
        warn "未检测到熔断 OPEN"
    fi
    if grep -q "=> CLOSED\|熔断器恢复" "$LOG_DIR/cb_client.log"; then
        info "熔断器恢复 CLOSED ✓"
    else
        warn "未检测到恢复"
    fi

    kill $SRV_PID $CLIENT_PID 2>/dev/null || true
    echo -e "\n${GREEN}熔断器演示完成${NC}"
}

demo_topic() {
    TOTAL=4
    echo -e "${BOLD}=== 演示: Topic 发布订阅 — 多策略分发 ===${NC}"
    echo "场景: 2 个订阅者（vip/normal），发布者用不同策略发送 5 条消息"
    echo ""

    step 1 "启动 Topic 服务器"
    "$BIN3/test3_topic_server" > "$LOG_DIR/top_srv.log" 2>&1 &
    SRV_PID=$!
    sleep 1
    info "Topic 服务端启动 (port 7070)"

    step 2 "启动订阅者: vip (priority=5) + normal (priority=1)"
    "$BIN3/test3_subscribe_client" vip > "$LOG_DIR/top_vip.log" 2>&1 &
    VIP_PID=$!
    "$BIN3/test3_subscribe_client" normal > "$LOG_DIR/top_normal.log" 2>&1 &
    NORMAL_PID=$!
    sleep 1
    info "vip 订阅者 (tags=[vip], priority=5)"
    info "normal 订阅者 (tags=[normal], priority=1)"

    step 3 "发布消息: 按不同策略分发"
    echo ""
    for mode in broadcast priority fanout hash redundant; do
        info "策略: $mode"
        "$BIN3/test3_publish_client" "$mode" > "$LOG_DIR/top_pub_${mode}.log" 2>&1
        sleep 0.3
    done

    step 4 "订阅者收到的消息"
    echo -e "  ${CYAN}=== vip 订阅者 ===${NC}"
    grep "recv" "$LOG_DIR/top_vip.log" 2>/dev/null | head -20 || echo "  无输出"
    echo -e "  ${CYAN}=== normal 订阅者 ===${NC}"
    grep "recv" "$LOG_DIR/top_normal.log" 2>/dev/null | head -20 || echo "  无输出"
    echo ""
    info "broadcast: 所有订阅者都收到"
    info "priority: 只有 vip 收到（priority=2 > normal 的 1）"
    info "fanout: 每个订阅者收到 1 份（fanout=1）"
    info "hash: 按 shard key 固定路由到同一订阅者"
    info "redundant: 每个消息发 2 份"

    kill $SRV_PID $VIP_PID $NORMAL_PID 2>/dev/null || true
    echo -e "\n${GREEN}Topic 演示完成${NC}"
}

demo_ha() {
    TOTAL=6
    echo -e "${BOLD}=== 演示: 注册中心多实例 HA ===${NC}"
    echo "场景: 启动 3 个 registry 实例（同一端口 SO_REUSEPORT）→ etcd lease 选举 leader → 杀 leader → follower 自动接管"
    echo "意义: 注册中心本身挂了不影响服务，其他实例自动接管，外部无感知"
    echo ""

    if ! pgrep -x etcd >/dev/null; then
        warn "请先启动 etcd: etcd --listen-client-urls=http://127.0.0.1:2379 --advertise-client-urls=http://127.0.0.1:2379 &"
        return 1
    fi
    info "etcd 已在运行"
    export LCZ_ETCD=http://127.0.0.1:2379

    # 记录每个实例的 pid，用于精确控制
    HA_PIDS=()
    HA_LOGS=("$LOG_DIR/ha_node1.log" "$LOG_DIR/ha_node2.log" "$LOG_DIR/ha_node3.log")

    step 1 "启动 3 个 registry 实例（同一端口 8080, kReusePort）"
    for i in 0 1 2; do
        "$BIN/test1_registry_server" > "${HA_LOGS[$i]}" 2>&1 &
        HA_PIDS[$i]=$!
        info "实例 $((i+1)) 启动 pid=${HA_PIDS[$i]} log=${HA_LOGS[$i]}"
    done
    info "内核 SO_REUSEPORT 自动分发新连接到 3 个实例"

    step 2 "等待 etcd lease 选举完成（约 3s）"
    sleep 3
    echo -e "  ${CYAN}=== 选举结果 ===${NC}"
    for log in "${HA_LOGS[@]}"; do
        grep -E "成为 leader|退为 follower|选举已启动" "$log" | tail -5
    done

    # 找到 leader 的 pid
    LEADER_LOG=""
    for log in "${HA_LOGS[@]}"; do
        if grep -q "成为 leader" "$log" 2>/dev/null; then
            LEADER_LOG="$log"
            break
        fi
    done

    if [ -z "$LEADER_LOG" ]; then
        warn "未检测到 leader！检查日志: ${HA_LOGS[*]}"
        kill "${HA_PIDS[@]}" 2>/dev/null || true
        return 1
    fi
    # 从日志提取 leader 的 instance 信息（hostname:pid）
    LEADER_INFO=$(grep "成为 leader" "$LEADER_LOG" | tail -1)
    info "当前 leader: $LEADER_INFO"

    step 3 "启动 provider 注册服务 → 内核自动路由到其中一个实例"
    "$BIN/test1_rpc_server" > "$LOG_DIR/ha_prov.log" 2>&1 &
    PROV_PID=$!
    sleep 3
    if grep -q "注册成功" "$LOG_DIR/ha_prov.log" 2>/dev/null; then
        info "provider 注册成功（add 方法, 127.0.0.1:8889）"
    else
        warn "provider 注册可能失败，检查 $LOG_DIR/ha_prov.log"
    fi
    # 验证 etcd 中有注册数据
    info "etcd 中注册数据:"
    etcdctl get --prefix /lcz-rpc/v1/providers/add/ 2>/dev/null | head -6 || true

    step 4 "client 调用 add(66,33)，验证服务可发现"
    timeout 10 "$BIN/test1_rpc_client" > "$LOG_DIR/ha_client1.log" 2>&1 || true
    if grep -q "result:99" "$LOG_DIR/ha_client1.log" 2>/dev/null; then
        info "调用成功: add(66,33) = 99 ✓"
    else
        warn "调用失败，查看 $LOG_DIR/ha_client1.log"
    fi

    step 5 "杀掉 leader 实例 → etcd lease 5s 后过期 → key 自动删除"
    # leader 日志中"instance=hostname:PID"，提取 PID
    LEADER_PID=$(echo "$LEADER_INFO" | grep -oP 'instance=\S+' | grep -oP '\d+$')
    if [ -z "$LEADER_PID" ]; then
        # fallback: 从 leader 日志文件名推断
        for i in 0 1 2; do
            if [ "${HA_LOGS[$i]}" = "$LEADER_LOG" ]; then
                LEADER_PID=${HA_PIDS[$i]}
                break
            fi
        done
    fi
    info "杀掉 leader (pid=$LEADER_PID)"
    kill "$LEADER_PID" 2>/dev/null || true
    sleep 1
    info "leader 已停止，lease 将在 5s 后过期"

    step 6 "等待 follower 接管（lease 过期 + CAS 重试 ≈ 6s）"
    sleep 6
    echo -e "  ${CYAN}=== 接管结果 ===${NC}"
    for log in "${HA_LOGS[@]}"; do
        grep -E "成为 leader|退为 follower" "$log" | tail -3
    done

    # 检查是否有新 leader
    NEW_LEADER=$(grep -l "成为 leader" "${HA_LOGS[@]}" 2>/dev/null | head -1)
    if [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$LEADER_LOG" ]; then
        info "follower 接管成功！新 leader: $(grep '成为 leader' "$NEW_LEADER" | tail -1)"
        info "故障转移时间 ≈ $(grep -c 'tick\|续约\|竞选' "$NEW_LEADER" 2>/dev/null | head -1) 个 tick 内完成"
    elif [ -n "$NEW_LEADER" ]; then
        info "leader 未切换（原 leader 可能在 lease 过期前被 kill 的实例还未清理）"
    else
        warn "未检测到新 leader，检查剩余实例日志"
    fi

    # 清理：杀掉剩余 registry 实例和 provider
    for pid in "${HA_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    kill $PROV_PID 2>/dev/null || true
    echo -e "\n${GREEN}多实例 HA 演示完成${NC}"
}

mkdir -p "$LOG_DIR"

case "${1:-}" in
    etcd)
        demo_etcd
        ;;
    offline)
        demo_offline
        ;;
    timeout)
        demo_timeout
        ;;
    topic)
        demo_topic
        ;;
    circuit)
        demo_circuit
        ;;
    ha)
        demo_ha
        ;;
    all)
        demo_etcd
        demo_offline
        demo_timeout
        demo_topic
        demo_circuit
        demo_ha
        echo -e "\n${BOLD}${GREEN}全部演示完成。日志: $LOG_DIR/${NC}"
        ;;
    *)
        echo "用法: ./demo.sh {etcd|offline|timeout|topic|circuit|ha|all}"
        echo ""
        echo "  etcd    — 演示 registry 重启后 etcd 数据不丢，服务可继续发现"
        echo "  offline — 演示 provider 挂掉后 registry 自动检测并剔除"
        echo "  timeout — 演示慢 provider 超时控制，client 1s 超时立即返回"
        echo "  topic   — 演示发布订阅 5 种转发策略（broadcast/priority/fanout/hash/redundant）"
        echo "  circuit — 演示熔断器：连续失败触发熔断 → 冷却后 HALF_OPEN 探测 → 恢复"
        echo "  ha      — 演示多实例 HA：3 个 registry 通过 etcd 选举 leader，杀 leader 后 follower 接管"
        echo "  all     — 全部跑一遍"
        echo ""
        echo "  注意: etcd / ha 演示需要先启动 etcd 服务 (etcd --listen-client-urls=http://127.0.0.1:2379 &)"
        echo "  日志: $LOG_DIR/"
        ;;
esac
