# lyqtRpc

[![CI](https://github.com/lczllx/lyqtRpc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/lczllx/lyqtRpc/actions/workflows/ci.yml)

[中文](README.zh-CN.md)

> A lightweight C++17 RPC framework built on muduo + Protobuf. Dual TCP / shared-memory zero-copy transport, etcd registry, circuit breaker, token bucket, distributed tracing, Prometheus metrics, and an HTTP-to-RPC API gateway — all in one repo.

Author: lczllx · Language: C++17 · Network: muduo · Transport: TCP & SHM zero-copy · Serialization: Protobuf, JSON, FlatBuffers · Build: CMake

## Highlights

- **Dual transport**: TCP (LV variable-length framing) and SHM zero-copy (per-client ring buffer + Protobuf `SerializeToArray` direct write)
- **Service governance**: etcd registry, heartbeat keep-alive, CAS leader election, load reporting, three-state circuit breaker, token bucket rate limiting
- **Call models**: synchronous, `std::future` async, and callback-based
- **Pluggable serialization**: `ISerializer` abstraction, default Protobuf, JSON for debugging, FlatBuffers for zero-copy reads
- **Distributed tracing**: `trace_id` + `span_id` end-to-end propagation
- **Multi-client concurrency**: SHM path with muduo `EventLoopThread` worker pool, per-client independent ring buffers
- **Observability**: built-in Prometheus `/metrics` endpoint (text format 0.0.4), covering request count / latency histogram / concurrency / error count / connection count / circuit breaker state / token bucket / process-level metrics — mirrors brpc `/vars`
- **API Gateway**: HTTP→RPC gateway on the same muduo foundation, with route matching, rate limiting, circuit breaker, Prometheus metrics, and distributed tracing — deployed as a separate process, zero changes to the RPC framework

## Performance: lyqtRpc vs brpc

Test environment: 4C8G cloud VM, Ubuntu 22.04, g++ 12.3.0, all Protobuf, echo payload. brpc 1.17.0.

### Single-thread latency & throughput

| Payload | brpc TCP | lyqtRpc TCP Proto | lyqtRpc SHM Proto ZC |
|---|---:|---:|---:|
| 16B QPS | ~14,000 | 10,706 | **25,216** |
| 16B P50 | ~68μs | 90μs | **28μs** |
| 64KB QPS | ~4,300 | 2,059 | **11,552** |
| 64KB P50 | ~220μs | 459μs | **68μs** |

### 4-thread concurrency

| Metric | brpc TCP | lyqtRpc TCP Proto | lyqtRpc SHM Proto ZC |
|---|---:|---:|---:|
| QPS | ~48,000 | 35,660 | **123,250** |
| P50 | ~82μs | 105μs | **17μs** |

SHM single-thread latency is 41% of brpc, dropping to 21% at 4 threads. The ~30% gap on the TCP path stems from bthread coroutines, IOBuf zero-copy chains, and baidu_std multiplexing in brpc.

## Quick Start

```bash
git clone https://github.com/lczllx/lyqtRpc.git
cd lyqtRpc
git submodule update --init --recursive
bash autobuild/quick_build.sh
```

### Docker

```bash
bash autobuild/docker.sh doctor
bash autobuild/docker.sh setup
```

### Examples

```bash
# TCP RPC (requires etcd)
docker compose up -d etcd
bash demosh/demo.sh etcd

# SHM Proto zero-copy (no extra dependencies)
cd rpc/build
./bin/shm_proto_server &
./bin/shm_proto_client

# Full-path benchmark
cd example/shm && bash run_shm_benchmark.sh all
```

### Prometheus metrics

```bash
cd rpc/build
./bin/benchmark_server 8889 0 8080 0 &
./bin/benchmark_client single echo 2000 1 1
curl localhost:9090/metrics
```

Add the address to `prometheus.yml` `scrape_configs`. Percentiles are computed query-side:
`histogram_quantile(0.99, rate(rpc_request_duration_us_bucket[1m]))`.

### API Gateway

```bash
cd rpc/build
./bin/benchmark_server 8889 &      # 1. start RPC backend
./bin/gateway_server &             # 2. start gateway (HTTP :8080, metrics :9091)

curl -d '{"data":"hello"}' localhost:8080/api/echo   # → RPC echo backend
curl localhost:8080/api/health                        # → health check
curl localhost:8080/diagnose                          # → rate limiter + breaker status
curl localhost:9091/metrics | grep gateway            # → gateway metrics
```

## Architecture

```
Provider ──REGISTER/HEARTBEAT──> Registry(etcd) <──DISCOVER── Consumer
   │                                                          │
   └── RPC call ────────────────────────────────────────────>─┘
```

- **LV framing**: `| 4B total_len | 4B msg_type | 4B id_len | id | body |`
- **SHM zero-copy**: `SerializeToArray` → ring buffer → `eventfd` → `ParseFromArray`, bypassing TCP
- **Registry backend**: `LCZ_ETCD` env var switches between Memory / Etcd; defaults to in-memory
- **Circuit breaker**: three-state (CLOSED→OPEN→HALF_OPEN), method×host granularity, memory/etcd persistence
- **Rate limiter**: `TokenBucket` + `BACKOFF` automatic backoff and retry
- **Thread pool**: muduo `EventLoopThread`, per-client ring buffer, lock-free SPSC

> See [docs/en/architecture-en.md](docs/en/architecture-en.md) for detailed flowcharts.

## Directory

```text
lyqtRpc/
├── rpc/
│   ├── src/
│   │   ├── client/           # RpcClient, ClientDiscover, CircuitBreaker, ShmClient
│   │   ├── server/           # RpcServer, Registry, LeaderElection, ShmServer
│   │   └── general/          # ShmChannel, LVProtocol, MessageFactory, Serializer, Logger
│   ├── tests/                # 76 GTest cases
│   ├── example/              # Examples + benchmarks
│   ├── proto/                # Protobuf definitions
│   └── muduo/                # Git submodule
├── gateway/
│   ├── src/                  # HttpServer, HttpRouter, GatewayHandler, DiagnoseHandler
│   └── example/              # gateway_server entry point
├── autobuild/                # Build + Docker scripts
├── demosh/                   # Demo scripts
├── docs/                     # Design docs (cn + en)
├── grafana/dashboards/       # Grafana dashboard JSON
├── Dockerfile
└── docker-compose.yml
```

## Build

**Dependencies** (one `apt install`):
```bash
sudo apt-get install -y build-essential cmake g++ make \
  libboost-dev libjsoncpp-dev libcurl4-openssl-dev \
  protobuf-compiler libprotobuf-dev \
  flatbuffers-compiler libflatbuffers-dev    # optional, for SHM FlatBuf path
```

**Compile**:
```bash
cd lyqtRpc/rpc && mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DLCZ_RPC_BUILD_EXAMPLES=ON
make -j$(nproc)
```

**Unit tests**: `cmake .. -DLCZ_RPC_BUILD_TESTS=ON` with `libgtest-dev` installed, then `./bin/lcz_rpc_unit_tests`.

## Roadmap

- [x] GitHub Actions CI (build + unit tests + SHM smoke test)
- [x] Docker image + docker-compose
- [x] etcd registry backend (Lease + LCZ_ETCD env-var switch)
- [x] Three-state circuit breaker (method×host, configurable)
- [x] Registry multi-instance HA (etcd lease + CAS election)
- [x] Token bucket rate limiting + BACKOFF
- [x] Distributed tracing (trace_id/span_id)
- [x] Pluggable serializer (ISerializer + ProtobufSerializer)
- [x] Prometheus /metrics (QPS / latency histogram / error codes / process metrics)

## Known Limitations

- TCP path single-connection `send()` holds a lock; connection pool + protocol multiplexing planned
- etcd heartbeat re-registers on every keepalive failure (lease TTL too short), write amplification under load
- SHM payloads >64KB copy twice through the ring buffer; throughput worse than TCP zero-copy equivalents
- No auth / encryption; no streaming RPC; Topic has no persistence
- Unit tests cover core modules only; registry / circuit breaker / network layer untested

## Code Stats

| Item | Count |
|---|---|
| RPC framework LoC | 8,545 (`rpc/src/` only, excluding muduo and proto generated) |
| Unit tests | 1,600+ lines, 12 files, 76 cases (GTest) |
| Example code | 1,589 lines |
| Source files | 56 (`rpc/src/` .h/.hpp/.cc/.cpp) |
