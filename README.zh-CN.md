# lyqtRpc

[![CI](https://github.com/lczllx/lyqtRpc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/lczllx/lyqtRpc/actions/workflows/ci.yml)

[English](README.md)

> 基于 muduo + Protobuf 的轻量级 C++17 RPC 框架。支持 TCP / 共享内存零拷贝双传输模式，集成 etcd 注册中心、熔断器、令牌桶流控、分布式追踪、Prometheus 可观测性、HTTP→RPC API 网关。

作者：lczllx · 语言：C++17 · 网络：muduo · 传输：TCP & SHM 零拷贝 · 序列化：Protobuf, JSON, FlatBuffers · 构建：CMake

## 功能亮点

- **双传输模式**：TCP（LV 变长协议）和 SHM 零拷贝（per-client ring buffer + Protobuf `SerializeToArray` 直写）
- **服务治理**：etcd 注册中心、心跳保活、CAS 选主、负载上报、三态熔断器、令牌桶流控
- **调用方式**：同步、`std::future` 异步、回调三种模式
- **序列化可插拔**：`ISerializer` 抽象接口，默认 Protobuf，支持 JSON 调试、FlatBuffers 零拷贝读
- **分布式追踪**：`trace_id` + `span_id` 全链路透传
- **多客户端并发**：SHM 路径服务端 muduo `EventLoopThread` 线程池，per-client 独立 ring buffer
- **可观测性**：内建 Prometheus `/metrics` 端点（文本协议 0.0.4），暴露请求量/延迟直方图/并发度/错误数/连接数/熔断状态/令牌桶余量/进程级指标，覆盖 brpc `/vars` 核心项
- **API 网关**：基于同一套 muduo 基础设施的 HTTP→RPC 网关，支持路由匹配/限流/熔断/指标/全链路追踪，独立进程零侵入部署

## 性能：lyqtRpc vs brpc

测试环境：4C8G 云机, Ubuntu 22.04, g++ 12.3.0, 全部 Protobuf 序列化, echo 字符串回显。brpc 1.17.0。

### 单线程延迟与吞吐

| 载荷 | brpc TCP | lyqtRpc TCP Proto | lyqtRpc SHM Proto ZC |
|---|---:|---:|---:|
| 16B QPS | ~14,000 | 10,706 | **25,216** |
| 16B P50 | ~68μs | 90μs | **28μs** |
| 64KB QPS | ~4,300 | 2,059 | **11,552** |
| 64KB P50 | ~220μs | 459μs | **68μs** |

### 4 线程并发

| 指标 | brpc TCP | lyqtRpc TCP Proto | lyqtRpc SHM Proto ZC |
|---|---:|---:|---:|
| QPS | ~48,000 | 35,660 | **123,250** |
| P50 | ~82μs | 105μs | **17μs** |

SHM 单线程延迟是 brpc 的 41%，4 线程降至 21%。TCP 路径与 brpc 差距约 30%，根因是 bthread 协程、IOBuf 零拷贝、baidu_std 多路复用等架构差异。

## 快速开始

```bash
git clone https://github.com/lczllx/lyqtRpc.git
cd lyqtRpc
git submodule update --init --recursive
bash autobuild/quick_build.sh
```

### Docker 部署

```bash
bash autobuild/docker.sh doctor   # 诊断环境
bash autobuild/docker.sh setup    # 自动配网 + 构建镜像 + 启动
```

### 运行示例

```bash
# TCP RPC 示例（需先启动 etcd）
docker compose up -d etcd
bash demosh/demo.sh etcd

# SHM Proto 零拷贝示例（无需额外依赖）
cd rpc/build
./bin/shm_proto_server &
./bin/shm_proto_client

# 全路径压测
cd example/shm && bash run_shm_benchmark.sh all
```

### 查看 Prometheus 指标

```bash
cd rpc/build
./bin/benchmark_server 8889 0 8080 0 &
./bin/benchmark_client single echo 2000 1 1
curl localhost:9090/metrics    # 请求量/延迟直方图/错误数/连接数/process_*
```

接入 Prometheus 只需在 `prometheus.yml` 的 `scrape_configs` 里加上该地址；
P99 等分位数用查询侧计算：`histogram_quantile(0.99, rate(rpc_request_duration_us_bucket[1m]))`。

### API 网关

```bash
cd rpc/build
./bin/benchmark_server 8889 &      # 1. 起 RPC 后端
./bin/gateway_server &             # 2. 起网关（HTTP :8080, metrics :9091）

curl -d '{"data":"hello"}' localhost:8080/api/echo   # → RPC echo 后端
curl localhost:8080/api/health                        # → 网关健康检查
curl localhost:8080/diagnose                          # → 限流熔断诊断
curl localhost:9091/metrics | grep gateway            # → 网关专属指标
```

## 架构

```
Provider ──REGISTER/HEARTBEAT──> Registry(etcd) <──DISCOVER── Consumer
   │                                                          │
   └── RPC call ────────────────────────────────────────────>─┘
```

- **LV 协议帧**：`| 4B total_len | 4B msg_type | 4B id_len | id | body |`
- **SHM 零拷贝**：`SerializeToArray` 直写 ring buffer → `eventfd` → `ParseFromArray`，绕过 TCP 协议栈
- **注册存储**：`LCZ_ETCD` 环境变量切换 Memory/Etcd 后端，默认单机内存模式
- **熔断器**：三态（CLOSED→OPEN→HALF_OPEN），method×host 粒度，支持内存/etcd 持久化
- **流控**：`TokenBucket` + `BACKOFF` 自动退避重试
- **线程池**：muduo `EventLoopThread`，per-client 独立 ring buffer，无锁 SPSC

> 详细的流程图、注册发现、心跳摘除、超时控制见 [docs/cn/architecture.md](docs/cn/architecture.md)

## 目录

```text
lyqtRpc/
├── rpc/
│   ├── src/
│   │   ├── client/           # RpcClient, ClientDiscover, 熔断器, ShmClient 系列
│   │   ├── server/           # RpcServer, Registry, 选举, ShmServer 系列
│   │   └── general/          # ShmChannel, LVProtocol, 消息工厂, 序列化器, 日志
│   ├── tests/                # 76 个 GTest 单测
│   ├── example/              # 示例 + 压测
│   ├── proto/                # protobuf 定义
│   └── muduo/                # Git 子模块
├── gateway/
│   ├── src/                  # HttpServer, HttpRouter, GatewayHandler, DiagnoseHandler
│   └── example/              # gateway_server 启动入口
├── autobuild/                # 构建 + Docker 脚本
├── demosh/                   # 演示脚本
├── docs/                     # 设计文档（中英双语）
├── grafana/dashboards/       # Grafana 面板 JSON
├── Dockerfile
└── docker-compose.yml
```

## 构建

**依赖**：（`apt install` 一行搞定）
```bash
sudo apt-get install -y build-essential cmake g++ make \
  libboost-dev libjsoncpp-dev libcurl4-openssl-dev \
  protobuf-compiler libprotobuf-dev \
  flatbuffers-compiler libflatbuffers-dev    # SHM FlatBuf 路径可选
```

**编译**：
```bash
cd lyqtRpc/rpc && mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DLCZ_RPC_BUILD_EXAMPLES=ON
make -j$(nproc)
```

**运行单测**：`cmake .. -DLCZ_RPC_BUILD_TESTS=ON` 且已安装 `libgtest-dev`，构建后 `./bin/lcz_rpc_unit_tests`。

## 规划

- [x] Github Actions CI（编译 + 单测 + SHM smoke test）
- [x] Docker 镜像 + docker-compose 编排
- [x] etcd 注册存储（Lease + 环境变量切换 Memory/Etcd 后端）
- [x] 三态熔断器（method×host 粒度，环境变量可配）
- [x] 注册中心多实例 HA（etcd lease + CAS 选举）
- [x] 令牌桶流控 + BACKOFF 退避
- [x] 分布式追踪（trace_id/span_id 全链路透传）
- [x] 序列化器抽象（ISerializer 接口 + ProtobufSerializer）
- [x] 监控导出（Prometheus /metrics，QPS/延迟直方图/错误码/进程级指标）

## 已知缺陷

- TCP 路径单连接 `send()` 持锁，多线程竞争明显，计划引入连接池 + 协议多路复用
- etcd 心跳每次 keepalive 失败后重新注册（Lease TTL 偏短），高频场景有写放大
- SHM 大载荷（>64KB）ring buffer memcpy 两次，吞吐不如 TCP 零拷贝方案
- 无鉴权/加密、无流式 RPC、Topic 无持久化
- 单测仅覆盖核心模块，注册中心/熔断器/网络层无单测

## 代码统计

| 项目 | 数值 |
|---|---|
| RPC 框架总行数 | 8,545（仅 `rpc/src/`，不含 muduo 子模块和 proto 生成代码） |
| 单元测试 | 1,600+ 行，12 文件，76 用例（GTest） |
| 示例代码 | 1,589 行 |
| 源文件数 | 56（`rpc/src/` 下 .h/.hpp/.cc/.cpp） |
