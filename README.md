# LCZ RPC

[![CI](https://github.com/lczllx/RPC/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/lczllx/RPC/actions/workflows/ci.yml)

作者：lczllx  
邮箱：2181719471@qq.com  
GitHub：https://github.com/lczllx/RPC  
开发环境：Ubuntu，VS Code  
编译器：g++  
语言：C++11  
网络：muduo  
序列化：jsoncpp（JSON）/ Protobuf  
构建：CMake  

---

这是我用 C++11 + muduo 写的一个轻量 RPC，JSON、Protobuf 都能走，带注册中心、心跳、负载均衡，调用有同步、Future、回调几种。

网络层用 muduo，事件循环、非阻塞 IO、定时器都在这套里，多 IO 线程时用 `EventLoopThreadPool`。RPC 的帧格式、序列化、注册中心不在 muduo 里，在本仓库里实现。

JSON 用 jsoncpp，好调试。Protobuf 走 `call_proto` / `registerProtoHandler`，和压测里的二进制路径一致。**muduo 以 Git 子模块形式在 `rpc/muduo`，克隆后须先拉子模块再 CMake**，CMake 负责 proto 生成与编译 muduo。

TCP 要自己定帧，不然粘包不好拆。LV：`长度 + 类型 + id + body`，收齐一帧再反序列化，`msg_type` 给分发，`id` 对上请求和响应。

---

## 1. 项目简介

RPC 从发请求到收响应整条链路是齐的，benchmark 里对比了 JSON 和 Protobuf。注册中心、心跳、怎么选节点也写了，能跑通、能演示。

---

## 2. 压测

优化过程中 QPS 从约 20.2k（2C2G）到约 31k（4C8G）再到约 5 万（4C8G）。下面是一次跑出来的数，换机器会有偏差。

大 payload（`echo 100KB × 1000`）：P99 **1.96ms → 0.72ms**，QPS **702 → 1607**（约 2.29 倍），平均延迟也有下降。

小 payload（add、短 echo）：Protobuf 相对 JSON 的 QPS 大约高 **26%～75%**，延迟略低，以脚本输出为准。

测了：单线程 add、多线程 add、跑 10 秒吞吐、100KB echo。

环境：`4C8G` 云机、`Ubuntu 22.04`、`g++ 11`、`-O3`、本机回环。

| 场景 | JSON | Protobuf | 提升 |
|---|---:|---:|---:|
| 小 payload QPS（多线程 add） | 31,546 | 50,125 | +59% |
| 大 payload QPS（100KB echo） | 702.74 | 1,607.72 | 2.29x |
| 大 payload P99 | 1.96ms | 0.72ms | 2.72x |

---

## 3. 快速开始

**子模块（必做）**  
`muduo` 在 `rpc/muduo`，**先拉子模块再 `cmake`**，否则 Configure 会失败：

```bash
git submodule update --init --recursive
```

仅 `git clone` 或 **`git clone --depth 1` 浅克隆** 时，子模块不会自动就绪，务必执行上面命令。默认 `.gitmodules` 为 **HTTPS**（`https://github.com/chenshuo/muduo.git`），部分网络较慢或超时可在本地改为 SSH，例如：

```bash
git config submodule.rpc/muduo.url git@github.com:chenshuo/muduo.git
git submodule sync --recursive
git submodule update --init --recursive
```

**一键构建（推荐）**
```bash
cd RPC
bash autobuild/quick_build.sh
```

**Docker（可选）**  
在仓库根（与 `Dockerfile` 同级）执行：`git submodule update --init --recursive` 后 `docker build -t lcz-rpc:local .`。

完整构建（依赖检查、子模块等）：

```bash
cd RPC
bash autobuild/build.sh
```

**依赖（与 CI `.github/workflows/ci.yml` 对齐，Ubuntu 22.04）**  
编译示例与 muduo **至少需要 Boost**（`find_package(Boost)`），与仅装 json/protobuf 不够：

```bash
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  build-essential \
  cmake \
  pkg-config \
  g++ \
  make \
  libboost-dev \
  libjsoncpp-dev \
  libcurl4-openssl-dev \
  protobuf-compiler \
  libprotobuf-dev
```

若开启单元测试（`-DLCZ_RPC_BUILD_TESTS=ON`），还需 **GTest**（CI 使用系统包，非 CMake 联网拉取）：

```bash
sudo apt-get install -y --no-install-recommends libgtest-dev
```

**编译**
```bash
cd rpc
mkdir -p build && cd build
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DLCZ_RPC_BUILD_EXAMPLES=ON \
  -DLCZ_RPC_BUILD_TESTS=OFF
make -j
```

跑单测时把 `LCZ_RPC_BUILD_TESTS` 设为 `ON` 并确保已安装 `libgtest-dev`，构建后执行：`./tests/lcz_rpc_unit_tests`。

**示例**
项目内置示例位于 `rpc/example`，常用包括：
- 注册中心：`test/test1/registry_server.cc`
- Provider：`test/test1/rpc_server.cc`
- Consumer：`test/test1/rpc_client.cc`
- Benchmark：`benchmark/benchmark_server.cc`、`benchmark/benchmark_client.cc`

> 可执行文件默认输出到 `rpc/build/bin`（含 benchmark 系列）。

**仓库根目录演示脚本（需已编译出 `rpc/build/bin`）**

- `demo.sh {etcd|offline|timeout|topic|circuit|all}`：一键演示 5 个场景
  - `etcd`    — registry 重启，etcd 数据不丢，服务可继续发现
  - `offline` — provider 挂掉，registry 15s 自动剔除
  - `timeout` — 慢 provider 超时控制
  - `topic`   — 发布订阅 5 种转发策略
  - `circuit` — 熔断器：连续失败触发熔断 → 冷却后 HALF_OPEN 探测 → 恢复
- `demo_discovery.sh`：注册发现（test4 registry / provider / consumer）
- `demo_benchmark.sh`：JSON / Protobuf 压测入口

**压测脚本**

```bash
cd rpc/example/benchmark
./run_benchmark.sh
sh run_benchmark_json.sh
```

---

## 4. 流程图

### 调用链

![RPC 调用流程图](flowchat/flow-rpc-call.png)

`RpcClient` / `RpcCaller` 发出请求，服务端按 method 进入 `registerProtoHandler`，响应原路返回。

### 注册与发现

![服务注册发现流程图](flowchat/flow-registry.png)

服务方注册并定时心跳。调用方按 method 向注册中心取节点列表，再在本地选择实例。

### 心跳与实例摘除

![心跳保活和失效剔除](flowchat/flow-heartbeat.png)

心跳间隔 **10s**，**15s** 内无更新则从列表移除该实例。

### 客户端超时

![客户端超时控制](flowchat/flow-timeout.png)

按 `rid` 注册 muduo 定时器，超时先返回 `TIMEOUT`，响应先到达则取消定时器。迟到响应丢弃，避免与超时重复处理。

---

## 5. 架构

角色：

- **Provider**：对外提供服务，注册、处理 RPC。
- **Consumer**：发 RPC。
- **Registry**：记有哪些实例、心跳、上下线。

调用链：

```text
Consumer
  -> RpcClient / RpcCaller / Requestor
  -> 序列化（JSON 或 Protobuf）
  -> LV 帧封包
  -> muduo 发 TCP
  -> Provider 拆包、反序列化、进业务
  -> 响应往回走
```

注册与发现：

```text
Provider --注册/心跳--> Registry <--查列表-- Consumer
   |                         |
   +----- 超时未心跳则摘掉 --------+
```

---

## 6. 功能

**序列化**  
JSON（jsoncpp）好调试。Protobuf 走 `REQ_RPC_PROTO` 等，包一大和 JSON 差距就明显。两条路都留着，想用哪个用哪个。

**LV 协议**  
`LVProtocol` 帧格式：

```text
| 4B total_len | 4B msg_type | 4B id_len | id | body |
```

用 `total_len` 判断一帧是否收齐，再反序列化。`msg_type` 交给 `MessageFactory`，`id` 对应请求与响应。整型字段按网络字节序写。`total_len` 有上限，防止异常大包，具体数值见代码。

**注册、心跳、选节点**  
心跳 **10s**，Registry **5s** 扫一圈，**15s** 没心跳就当掉线。负载均衡：`ROUND_ROBIN`、`RANDOM`、`SOURCE_HASH`、`LOWEST_LOAD`。调用方式：同步、Future、回调。超时直接失败，不会自动帮你重试。

**注册存储后端**  
通过 `LCZ_ETCD` 环境变量切换。未设置时走内存存储（MemoryRegistryStore），适合单机/测试；设置为 etcd 地址后走 EtcdRegistryStore，注册信息持久化到 etcd，registry 重启不丢数据。

**注册中心多实例 HA**  
设 `LCZ_ETCD` 后启动多个 Registry 实例（同一端口，内核 `SO_REUSEPORT` 分发连接）。实例间通过 etcd lease + CAS 事务选举 leader（5s TTL，1s 续约）；仅 leader 执行过期 provider 扫描，follower 依赖客户端 10s 健康检查兜底。Leader 崩溃后 lease 5s 自动过期，follower 自动接管。

**熔断器**  
三态状态机（CLOSED → OPEN → HALF_OPEN → CLOSED），method×host 粒度，支持环境变量配置阈值（`LCZ_CB_FAILURE_THRESHOLD`、`LCZ_CB_OPEN_DURATION`、`LCZ_CB_HALF_OPEN_MAX`）。存储后端同样走 `LCZ_ETCD` 切换（MemoryCircuitStore / EtcdCircuitStore）。调用方在 `RpcCaller` 层自动检查熔断状态，拒绝请求直接返回 false 不等待网络超时。provider 下线时通过 `delClient()` 回调同步清理连接池和熔断器状态。

**日志系统**  
自研异步日志模块：双缓冲 + AsyncLooper 后台线程，支持 DEBUG/INFO/WARN/ERROR/FATAL 五级，输出 `[时间][线程][日志器][文件:行号][级别] 消息`。落地方式可选控制台/文件/滚动文件。

**Topic**  
发布订阅，转发策略有 `BROADCAST`、`ROUND_ROBIN`、`FANOUT`、`SOURCE_HASH`、`PRIORITY`、`REDUNDANT`，和 RPC 共用底层消息和网络。

---

## 7. 实现要点

**粘包**  
靠 `total_len` 判断一帧收没收齐，`canProcessed()` 不过就接着攒，不瞎反序列化，避免越界和脏数据。

**请求和响应对上号**  
每个请求一个 `rid`，客户端 `rid -> ReqDescribe`，回来按 `rid` 分到同步 / future / 回调。在途请求用 `unordered_map` + 一把锁，写得简单，并发高了会抢锁，以后要优化再说。

**超时**  
`runAfter` 挂定时器，响应先到就取消。超时先到就丢后面的包，同一个 `rid` 不会又超时又当成功。

**实例掉线**  
Registry 定时扫过期实例并通知。Consumer 收到就清连接池并同步清理熔断器状态，少往已经下线的节点打。

**熔断隔离**  
每个 provider 的每个方法独立一个 `NodeBreaker` 状态机。连续失败达到阈值自动熔断（OPEN），后续请求直接拒绝不消耗网络资源。冷却期结束后放行一个 HALF_OPEN 探测请求，成功则恢复（CLOSED），失败则继续熔断。状态通过 `ICircuitStateStore` 接口持久化，支持内存和 etcd 两种后端。

---

## 8. 目录

```text
RPC/
├── README.md
├── Dockerfile
├── docker-compose.yml     # etcd + registry + provider 编排
├── demo.sh                # 功能演示（etcd/offline/timeout/topic/circuit/ha）
├── demo_discovery.sh      # 服务发现演示
├── demo_benchmark.sh      # 压测演示
├── .github/workflows/     # CI / Release
├── autobuild/             # 一键构建脚本
├── flowchat/              # 流程图资源
└── rpc/
    ├── muduo/             # Git 子模块（chenshuo/muduo）
    ├── src/
    │   ├── client/             # caller.hpp, circuit_breaker.hpp/cpp, node_breaker.hpp/cpp
    │   ├── server/
    │   │   ├── etcd_registry_store.cpp/.hpp
    │   │   ├── memory_registry_store.cpp/.hpp
    │   │   ├── leader_election.hpp
    │   │   ├── memory_leader_election.hpp
    │   │   ├── etcd_leader_election.cpp/.hpp
    │   │   ├── etcd_circuit_store.cpp/.hpp
    │   │   └── memory_circuit_store.cpp/.hpp
    │   └── general/
    │       └── log_system/  # 异步日志模块
    ├── proto/
    │   └── rpc_envelope.proto
    ├── tests/             # 单元测试
    ├── example/
    │   ├── benchmark/
    │   └── test/
    └── CMakeLists.txt
```

---

## 9. 规划实现

- [x] 单测与回归（GitHub Actions 运行 `lcz_rpc_unit_tests`）
- [x] 部署入门：Dockerfile + CI 校验 `docker build`
- [x] etcd 注册存储：EtcdRegistryStore + MemoryRegistryStore，环境变量切换
- [x] 熔断：三态状态机，method×host 粒度，支持内存/etcd 持久化，环境变量可配
- [x] 注册中心多实例 HA：etcd lease + CAS 选举 leader，SO_REUSEPORT 多实例同端口部署
- [ ] 限流、重试
- [ ] 监控：QPS、延迟分位、错误码
