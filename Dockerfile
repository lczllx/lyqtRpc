# =============================================================================
# LCZ RPC — 多阶段镜像
# -----------------------------------------------------------------------------
# 构建：
#   docker build -t lcz-rpc:local .
#   docker compose up -d                     # 一键启动 etcd + registry + provider
#
# 无需手动初始化子模块 — muduo 缺失时自动从 GitHub 拉取
# =============================================================================

# ---------- 阶段 1：编译 ----------
FROM ubuntu:22.04 AS builder
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    g++ \
    make \
    libboost-dev \
    libjsoncpp-dev \
    libcurl4-openssl-dev \
    protobuf-compiler \
    libprotobuf-dev \
    flatbuffers-compiler \
    libflatbuffers-dev \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY rpc/ /src/rpc/

# 若 clone 时没用 --recursive，muduo 子模块目录为空 → 自动下载
RUN if [ ! -f /src/rpc/muduo/CMakeLists.txt ]; then \
        echo ">>> muduo 子模块未初始化，自动拉取..."; \
        mkdir -p /src/rpc/muduo && \
        wget -qO- https://github.com/chenshuo/muduo/archive/f1fc77e0c13b80e5086ff457362c8a86d1b609d4.tar.gz | \
        tar -xz --strip-components=1 -C /src/rpc/muduo && \
        echo ">>> muduo 拉取完成"; \
    fi

WORKDIR /src/rpc/build
RUN cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DLCZ_RPC_BUILD_EXAMPLES=ON \
    -DLCZ_RPC_BUILD_TESTS=OFF
RUN cmake --build . -j$(nproc)

# ---------- 阶段 2：运行（仅运行时库 + 可执行文件）----------
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

# 通过 -dev 元包拉取运行时库（避免硬编码 libjsoncpp25/libprotobuf23 版本号，
# -dev 包依赖正确的运行时 .so，兼容 22.04/24.04 等不同 Ubuntu 版本）
RUN apt-get update && apt-get install -y --no-install-recommends \
    libjsoncpp-dev \
    libprotobuf-dev \
    libcurl4 \
    zlib1g \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/rpc/build/bin /opt/rpc/bin

WORKDIR /opt/rpc
ENV PATH="/opt/rpc/bin:${PATH}"

# 默认进入 shell，便于你手动起 registry / server / client
CMD ["/bin/bash", "-lc", "echo 'LCZ RPC — 可执行文件位于 /opt/rpc/bin'; ls -la /opt/rpc/bin"]
