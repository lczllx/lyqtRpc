#!/usr/bin/env bash
# =============================================================================
# Docker 构建与部署脚本
# =============================================================================
# 用法:（从项目根目录执行，或任意位置给出脚本绝对/相对路径均可）
#   bash autobuild/docker.sh doctor
#   bash autobuild/docker.sh setup
#   bash autobuild/docker.sh build
#   bash autobuild/docker.sh compose
#   bash autobuild/docker.sh demo
#   bash autobuild/docker.sh clean
# =============================================================================
set -e

# 自动定位项目根目录，从任何位置执行均可
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
# 计算从项目根到脚本的相对路径，所有提示用这个
_ME="bash ${BASH_SOURCE[0]}"

# 颜色要在第一次使用之前定义，否则第一行输出里 ${GREEN} 展开为空
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

echo -e "${GREEN}[docker] 工作目录: $PROJECT_ROOT${NC}"

ok()  { echo -e "  ${GREEN}[OK]${NC} $1"; }
warn(){ echo -e "  ${YELLOW}[!]${NC} $1"; }
err() { echo -e "  ${RED}[X]${NC} $1"; }

# ====== 封装 docker（无 sudo 则直接报错退出，不卡在密码输入） ======
_need_sudo() {
    docker info >/dev/null 2>&1 && return 1          # 已有权限，不需要
    sudo -n true >/dev/null 2>&1 && return 0          # 有无密码 sudo
    err "Docker 权限不足，请执行以下命令后重新登录终端："
    echo "  sudo usermod -aG docker \$USER"
    echo "  newgrp docker"
    echo "  然后重试本脚本（不再需要 sudo）"
    exit 1
}

_docker() {
    if _need_sudo; then sudo docker "$@"; else docker "$@"; fi
}

_compose() {
    if command -v docker >/dev/null && docker compose version >/dev/null 2>&1; then
        if _need_sudo; then sudo docker compose "$@"; else docker compose "$@"; fi
    elif command -v docker-compose >/dev/null 2>&1; then
        warn "使用旧版 docker-compose v1，建议: sudo apt install docker-compose-plugin"
        if _need_sudo; then sudo docker-compose "$@"; else docker-compose "$@"; fi
    else
        err "未找到 docker compose（v1/v2 均不可用）"
        echo "  安装 v2: sudo apt install docker-compose-plugin"
        exit 1
    fi
}

# ====== 改系统配置前的统一入口：无密码 sudo 才继续 ======
# 少了这一层，非交互 shell 里裸 sudo 会抛
# "sudo: a terminal is required to read the password"，再被 set -e 放大成
# exit 1 —— 分不清是权限问题还是脚本有 bug（实测 setup/mirror 都是这么挂的）。
_need_root() {
    sudo -n true >/dev/null 2>&1 && return 0
    err "本操作要写系统配置（/etc/docker 或 docker.service.d），需要 root 权限"
    echo "  当前 shell 是非交互式的，sudo 读不到密码"
    echo "  请在交互式终端里重跑，或以 root 手动执行上述命令"
    exit 1
}

# ====== Docker Hub 可达性判定（doctor 与 setup 共用） ======
# 抽成函数是因为早先两处各写各的：doctor 会看 daemon 的 proxy.conf，setup
# 只看裸 TCP —— 于是在已配 daemon 代理的机器上 doctor 报"可达"、setup 却
# 判"不可达"去写镜像源，两边说法打架。
hub_direct() { timeout 3 bash -c "echo >/dev/tcp/registry-1.docker.io/443" 2>/dev/null; }

# daemon 是否已配代理（proxy.conf 由本脚本的 proxy / setup 写入）
daemon_proxy_configured() {
    test -r /etc/systemd/system/docker.service.d/proxy.conf 2>/dev/null ||
        sudo -n test -f /etc/systemd/system/docker.service.d/proxy.conf 2>/dev/null
}

# 视为可达：直连成功，或 daemon 走代理（间接可达）
hub_reachable() { hub_direct || daemon_proxy_configured; }

# ====== 写入国内镜像源（setup 与 mirror 共用） ======
# daemon.json 里可能还有别的键（insecure-registries / data-root / log-driver
# …），早先的实现是 sudo tee 整个文件，会把这些一并清掉。故按情况处理：
#   文件不存在            → 新建
#   已有 registry-mirrors → 跳过（不覆盖用户既有的选择）
#   存在但无 mirrors      → 备份后 merge，保留原有键
write_registry_mirrors() {
    local f=/etc/docker/daemon.json
    sudo mkdir -p /etc/docker
    if [ -f "$f" ] && sudo grep -q 'registry-mirrors' "$f" 2>/dev/null; then
        warn "$f 已有 registry-mirrors，跳过"
        return 0
    fi
    if [ -f "$f" ]; then
        if ! command -v python3 >/dev/null 2>&1; then
            err "$f 已存在但没有 registry-mirrors，且无 python3 可安全合并"
            echo "  为免清掉已有配置，本次不写入。请手动在顶层加上:"
            echo "    sudo \${EDITOR:-vi} $f    #  \"registry-mirrors\": [ ... ]"
            return 1
        fi
        local bak="$f.bak.$(date +%Y%m%d%H%M%S)"
        sudo cp "$f" "$bak"
        ok "已备份原配置 → $bak"
        sudo python3 - "$f" <<'PY'
import json, sys
path = sys.argv[1]
with open(path) as fp:
    cfg = json.load(fp)
cfg["registry-mirrors"] = [
    "https://registry.cn-hangzhou.aliyuncs.com",
    "https://docker.mirrors.ustc.edu.cn",
]
with open(path, "w") as fp:
    json.dump(cfg, fp, indent=2, ensure_ascii=False)
    fp.write("\n")
PY
    else
        sudo tee "$f" >/dev/null <<'EOF'
{
  "registry-mirrors": [
    "https://registry.cn-hangzhou.aliyuncs.com",
    "https://docker.mirrors.ustc.edu.cn"
  ]
}
EOF
    fi
    sudo systemctl restart docker
    ok "镜像源配置完成"
}

# ====== 诊断 ======
doctor() {
    echo -e "${BOLD}Docker 环境诊断${NC}"
    echo ""

    # 1. Docker 是否安装
    if command -v docker >/dev/null 2>&1; then
        ok "Docker 已安装 ($(docker --version 2>/dev/null | head -1))"
    else
        err "Docker 未安装 → sudo apt install docker.io"
    fi

    # 2. 权限
    if docker info >/dev/null 2>&1; then
        ok "Docker 权限正常"
    elif sudo -n docker info >/dev/null 2>&1; then
        warn "需要 sudo 执行 Docker 命令（建议加入 docker 组一劳永逸）"
        echo "      执行: sudo usermod -aG docker \$USER && newgrp docker"
    else
        err "Docker 权限不足且无密码 sudo。请: sudo usermod -aG docker \$USER && newgrp docker"
    fi

    # 3. Docker Compose
    if command -v docker >/dev/null && docker compose version >/dev/null 2>&1; then
        ok "Docker Compose v2: $(docker compose version --short 2>/dev/null)"
    elif command -v docker-compose >/dev/null 2>&1; then
        warn "Docker Compose v1 (旧版建议升级)"
    else
        err "Docker Compose 未安装 → sudo apt install docker-compose-plugin"
    fi

    # 4. 网络可达性（Docker daemon 可能已通过代理连接 Docker Hub）
    if hub_direct; then
        ok "Docker Hub 可达（直连）"
    elif daemon_proxy_configured; then
        ok "Docker daemon 已配置代理（可间接访问 Docker Hub）"
    elif [ -n "${http_proxy:-}" ]; then
        warn "Docker Hub 不可达 — 执行 $_ME proxy 配置 Docker daemon 代理"
    else
        warn "Docker Hub 不可达 — 执行 $_ME mirror 配国内镜像源"
    fi

    # 5. 项目文件
    local dockerfile="$PROJECT_ROOT/Dockerfile"
    local compose="$PROJECT_ROOT/docker-compose.yml"
    [ -f "$dockerfile" ] && ok "Dockerfile 存在" || err "缺少 Dockerfile"
    [ -f "$compose" ]   && ok "docker-compose.yml 存在" || err "缺少 docker-compose.yml"

    echo ""
    echo -e "${YELLOW}推荐: $_ME setup  ← 自动检测环境并完成首次部署${NC}"
}

# ====== 自动 setup（诊断 → 配网 → 构建 → 启动） ======
setup() {
    echo -e "${BOLD}Docker 环境自动部署${NC}"
    echo ""

    # 1. 检查环境
    if ! command -v docker >/dev/null 2>&1; then
        err "请先安装 Docker: sudo apt install docker.io docker-compose-plugin"
        exit 1
    fi

    # 2. 配网络
    if [ -n "${http_proxy:-}" ] || [ -n "${HTTP_PROXY:-}" ]; then
        local proxy="${http_proxy:-$HTTP_PROXY}"
        if ! daemon_proxy_configured; then
            _need_root
            echo -e "${GREEN}[setup] 检测到代理 $proxy，配置 Docker daemon ...${NC}"
            sudo mkdir -p /etc/systemd/system/docker.service.d
            sudo tee /etc/systemd/system/docker.service.d/proxy.conf <<EOF
[Service]
Environment="HTTP_PROXY=$proxy"
Environment="HTTPS_PROXY=$proxy"
Environment="NO_PROXY=localhost,127.0.0.1,.local"
EOF
            sudo systemctl daemon-reload
            sudo systemctl restart docker
            ok "Docker daemon 代理已配置"
        else
            ok "Docker daemon 代理已配置，跳过"
        fi
    elif ! hub_reachable; then
        _need_root
        echo -e "${GREEN}[setup] Docker Hub 不可达，配置国内镜像源 ...${NC}"
        write_registry_mirrors
    else
        ok "Docker Hub 可达（直连或 daemon 已配代理），跳过镜像源配置"
    fi

    # 3. 构建
    build_image

    # 4. 启动
    compose_up
    echo ""
    echo -e "${BOLD}部署完成！${NC}"
    echo "  查看状态: docker compose ps"
    echo "  查看日志: docker compose logs -f"
    echo "  停  止: $_ME clean"
}

# ====== 构建 ======
build_image() {
    echo -e "${GREEN}[docker] 构建镜像 lcz-rpc:local ...${NC}"
    # --network=host 是必需的，不是优化：~/.docker/config.json 的
    # proxies.default 会把 http_proxy 作为预定义 build arg 注入每一次构建，
    # 而那个代理地址是宿主机的 127.0.0.1 —— 在非 host 网络的构建容器里它指向
    # 容器自己，apt 连不上直接 exit 100（实测：apt-get update 报
    # "Could not connect to 127.0.0.1:17899"）。docker-compose.yml 里 build
    # 也带 network: host，同一原因。加了之后容器里的 127.0.0.1 即宿主机，代理可达。
    DOCKER_BUILDKIT=1 _docker build --network=host -t lcz-rpc:local .
    ok "镜像构建完成"
}

run_container() {
    echo -e "${GREEN}[docker] 启动容器（交互式 shell） ...${NC}"
    _docker run --rm -it lcz-rpc:local
}

compose_up() {
    echo -e "${GREEN}[docker] docker compose up -d ...${NC}"
    _compose up -d --build
    ok "服务已启动"
    echo "  etcd     → localhost:2379"
    echo "  registry → localhost:8080"
    echo "  provider → localhost:8889"
    _compose ps
}

demo() {
    compose_up
    echo ""
    echo -e "${GREEN}[demo] 等待服务就绪，发送测试请求 ...${NC}"
    sleep 3
    if [ -f "$PROJECT_ROOT/rpc/build/bin/test1_rpc_client" ]; then
        "$PROJECT_ROOT/rpc/build/bin/test1_rpc_client" || true
    else
        _docker run --rm --network=host lcz-rpc:local \
            bash -c "/opt/rpc/bin/test1_rpc_client 2>&1 || true"
    fi
}

clean() {
    echo -e "${GREEN}[docker] 停止容器 + 删除镜像 ...${NC}"
    _compose down -v 2>/dev/null || true
    _docker rm -f lcz-rpc-local 2>/dev/null || true
    _docker rmi lcz-rpc:local 2>/dev/null || true
    ok "清理完成"
}

setup_proxy() {
    local proxy="${http_proxy:-${HTTP_PROXY:-}}"
    if [ -z "$proxy" ]; then
        err "未检测到系统代理（\$http_proxy 为空）"
        echo "  用法: export http_proxy=http://127.0.0.1:7890 && bash docker.sh proxy"
        exit 1
    fi
    echo -e "${GREEN}[proxy] 配置 Docker daemon 代理: $proxy${NC}"
    _need_root
    sudo mkdir -p /etc/systemd/system/docker.service.d
    sudo tee /etc/systemd/system/docker.service.d/proxy.conf <<EOF
[Service]
Environment="HTTP_PROXY=$proxy"
Environment="HTTPS_PROXY=$proxy"
Environment="NO_PROXY=localhost,127.0.0.1,.local"
EOF
    sudo systemctl daemon-reload
    sudo systemctl restart docker
    ok "Docker daemon 代理已配置，Docker 已重启"
}

setup_mirror() {
    echo -e "${GREEN}[mirror] 配置 Docker 国内镜像源（阿里云 + 中科大）...${NC}"
    _need_root
    write_registry_mirrors
}

case "${1:-doctor}" in
    doctor)  doctor ;;
    setup)   setup ;;
    build)   build_image ;;
    run)     run_container ;;
    compose) compose_up ;;
    demo)    demo ;;
    clean)   clean ;;
    proxy)   setup_proxy ;;
    mirror)  setup_mirror ;;
    *)
        echo "用法: $_ME [doctor|setup|build|compose|demo|clean|proxy|mirror]"
        echo ""
        echo "  首次部署: doctor  → 诊断环境（Docker/权限/网络/Compose）"
        echo "            setup   → 自动配网+构建+启动（推荐）"
        echo ""
        echo "  日常使用: compose → 构建并启动服务（etcd + registry + provider）"
        echo "            demo    → 启动 + 自动发送测试请求"
        echo "            clean   → 停止容器 + 删除镜像"
        echo ""
        echo "  网络配置（国内首次部署，二选一）:"
        echo "    有代理 → $_ME proxy  (自动读取 \$http_proxy)"
        echo "    无代理 → $_ME mirror (配置国内镜像源)"
        ;;
esac
