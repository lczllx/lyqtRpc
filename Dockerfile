# =============================================================================
# LCZ RPC — 多阶段镜像
# =============================================================================
# 构建（无需手动初始化子模块，muduo 缺失时自动从 GitHub 拉取）：
#   docker compose up -d
#   docker compose logs -f          # 查看日志
#   docker compose down -v          # 停止并清理数据卷
# =============================================================================

# ---------- 阶段 1：编译 ----------
FROM ubuntu:22.04 AS builder
ENV DEBIAN_FRONTEND=noninteractive

ARG http_proxy
ARG https_proxy

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
COPY gateway/ /src/gateway/

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
    -DLCZ_RPC_BUILD_GATEWAY=ON \
    -DLCZ_RPC_BUILD_TESTS=OFF
RUN cmake --build . -j$(nproc)

# ---------- 阶段 2：运行 ----------
FROM ubuntu:22.04

ARG http_proxy
ARG https_proxy

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
CMD ["/bin/bash", "-lc", "echo 'LCZ RPC — 可执行文件位于 /opt/rpc/bin'; ls -la /opt/rpc/bin"]
