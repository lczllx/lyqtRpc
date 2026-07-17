# lyqtRpc Architecture

## Components

- **Provider**: Exposes RPC services, registers with Registry on startup, sends periodic heartbeats and load reports
- **Consumer**: Discovers Provider nodes via Registry, applies client-side load balancing, sends RPC calls
- **Registry**: Manages Provider lifecycle (register, heartbeat, deregister), backed by etcd with Lease-based auto-expiry

## RPC Call Chain

![RPC Call Flow](../flowchat/flow-rpc-call.png)

```
Consumer
  -> RpcClient / RpcCaller / Requestor
  -> CircuitBreaker（circuit check）
  -> ISerializer（serialize）
  -> LVProtocol framing
  -> muduo TCP send
  -> Provider unframe, deserialize
  -> TokenBucket（rate limiting）
  -> business handler
  -> response back
```

## Service Registry & Discovery

![Service Registry & Discovery](../flowchat/flow-registry.png)

```mermaid
sequenceDiagram
    participant P as Provider
    participant R as Registry
    participant E as etcd
    participant C as Consumer

    P->>R: REGISTER(method, host:port, load)
    R->>E: PUT /providers/add/127.0.0.1:8889 (Lease 15s)
    P->>R: HEARTBEAT (every 10s)
    R->>E: LeaseKeepAlive

    C->>R: DISCOVER(method=add)
    R->>E: GET prefix /providers/add/
    E-->>R: [127.0.0.1:8889, ...]
    R-->>C: host list + load

    C->>P: RPC call add(10, 20)
    P-->>C: result=30
```

The Provider binds an etcd **15s Lease** on registration, refreshed via `LeaseKeepAlive`. If the Provider crashes, the Lease expires and etcd auto-deletes the key. Registry sweep detects deletions via prefix scan + in-memory set diff.

## Heartbeat & Instance Eviction

![Heartbeat & Eviction](../flowchat/flow-heartbeat.png)

```mermaid
sequenceDiagram
    participant P as Provider
    participant R as Registry
    participant E as etcd

    loop every 10s
        P->>R: HEARTBEAT(method, host)
        R->>E: LeaseKeepAlive(lease_id)
        E-->>R: OK (TTL reset to 15s)
    end

    Note over P: Provider crashes

    loop every 5s
        R->>E: GET prefix /providers/
        R->>R: diff → detect missing keys
        R->>R: notify Consumers
    end
```

## Client-Side Timeout

![Client Timeout](../flowchat/flow-timeout.png)

```mermaid
sequenceDiagram
    participant C as Consumer
    participant S as Provider
    participant T as TimerWheel

    C->>T: runAfter(timeout_ms, callback)
    C->>S: RPC request(rid=xxx)

    alt normal response
        S-->>C: response(rid=xxx)
        C->>T: cancel(timer_id)
    else timeout
        T-->>C: timeout(rid=xxx)
        C->>C: return TIMEOUT, clean up context
        S-->>C: late response (discarded)
    end
```

A muduo timer is registered per `rid`. Timeout returns `TIMEOUT` first; a successful response cancels the timer. Late responses for the same `rid` are discarded to prevent double-processing.

---

## LV Protocol Frame Format

```text
| 4B total_len | 4B msg_type | 4B id_len | id (variable) | body (variable) |
```

- `total_len`: full frame length excluding these 4 bytes, network byte order
- `msg_type`: `MsgType` enum (e.g., REQ_RPC=0, RSP_RPC=1), used by `MessageFactory::create()`
- `id_len` + `id`: request ID (`_rid`) for matching requests to responses
- `body`: serialized body (JSON or Protobuf binary)

`canProcessed()` checks whether a complete frame has arrived before deserializing.

---

## SHM Zero-Copy Transport

### TCP Loopback Path

```
Client Process                         Server Process
┌──────────────┐                    ┌──────────────┐
│ serialize()  │  ① std::string     │ unserialize() │
│   ↓          │                    │   ↑          │
│ muduo Buffer │  ② memcpy          │ muduo Buffer │ ③ retrieveAsString
│   ↓          │                    │   ↑          │
│ Socket send  │  ③ copy_from_user  │ Socket recv  │ ④ copy_to_user
│   ↓          │    → sk_buff       │   ↑          │
│   TCP/IP     │  ④ stack overhead   │   TCP/IP     │ ⑤ stack overhead
│   ↓          │                    │   ↑          │
│   lo iface   │════ kernel copy ════│   lo iface   │
└──────────────┘                    └──────────────┘
```

5 data copies (serialize alloc → Buffer trim → kernel copy×2 → Buffer→string).

### SHM Zero-Copy Path

```
Client Process                         Server Process
┌─────────────────────┐             ┌─────────────────────┐
│ SerializeToArray()  │ ① direct    │ ParseFromArray()    │ ③ in-place parse
│   ↓                 │             │   ↑                 │
│ req_write_ptr() ────┼──→ ring buffer ←── read_request() │
│   (returns body ptr) │  mmap shared │   (returns body ref) │
│   ↓                 │             │   ↑                 │
│ req_commit() ───┼──→ write_idx.store(release)          │
│ notify_req()        │  eventfd    │ epoll_wait()        │ ② kernel notify
└─────────────────────┘             └─────────────────────┘
```

1 data copy (serialize directly into ring buffer), 0 data syscalls (eventfd notification only).

### Lock-Free SPSC Ring Buffer

Thread safety via `std::atomic` release/acquire semantics, no CAS needed:

```cpp
// Producer publishes
ch.write_idx.store(w + frame_len, std::memory_order_release);

// Consumer sees (paired acquire)
uint64_t w = ch.write_idx.load(std::memory_order_acquire);
```

`write_idx` and `read_idx` are each written by a single thread (SPSC). uint64 wraparound is automatically correct.

---

## Feature Modules

### Serialization

The `ISerializer` interface supports pluggable backends. Default `ProtobufSerializer` delegates to `msg->serialize()`/`msg->unserialize()`. JSON is used for debugging. FlatBuffers provides true zero-copy reads via `GetRoot<T>()`.

### Registry HA

Multiple Registry instances share an etcd backend. `SO_REUSEPORT` distributes connections on the same port. etcd lease + CAS transaction elects a leader (5s TTL, 1s renewal). Only the leader runs expiration sweep; followers rely on client-side 10s health checks.

### Circuit Breaker

Three-state state machine (CLOSED → OPEN → HALF_OPEN → CLOSED), per method×host granularity. State persists via the `ICircuitStateStore` interface with in-memory and etcd backends.

### Token Bucket Rate Limiting

Provider-side `TokenBucket` generates tokens at a fixed rate. Excess requests return `BACKOFF` with `retry_after_ms`. The Client auto-waits before retrying.

### Distributed Tracing

`trace_id` (UUID) + `span_id` propagated via JSON payload or Proto envelope fields. grep the same trace_id across Client/Registry/Provider logs to reconstruct full call chains.

### Async Logging

Double-buffered async logger with background `AsyncLooper` thread. Five levels (DEBUG~FATAL). Supports console, file, and rolling file sinks.

### Pub/Sub Topics

Six forwarding strategies: `BROADCAST`, `ROUND_ROBIN`, `FANOUT`, `SOURCE_HASH`, `PRIORITY`, `REDUNDANT`. Shares the underlying messaging and network layer with RPC.

### Observability (Prometheus /metrics)

Built-in Prometheus text-format (0.0.4) endpoint covering the core of brpc's `/vars`. The design is "inline instrumentation in business threads + on-demand export in a dedicated thread":

- **Instrumentation**: Counter/Gauge/Histogram are `std::atomic`-based (relaxed). Business threads bump values in place while handling requests — no collector thread, no queue. The Registry singleton lazily creates series on first use.
- **Export**: a single `MetricsServer` thread (muduo EventLoop + Channel) listens on `:9090`; on scrape it reads atomic snapshots and renders text. `process_*` metrics (CPU/RSS/fds/threads/loadavg) are read from `/proc` at scrape time — zero cost when nobody scrapes.
- **Coverage**: server-side `rpc_requests_total` / `rpc_request_duration_us` (histogram) / `rpc_concurrency` / `rpc_errors_total` / `rpc_connection_count`; client-side `rpc_client_*` (RTT/concurrency/errors); governance `circuit_breaker_state` / `token_bucket_available` / `rpc_rate_limited_total` / `registry_heartbeats_total`.
- **Percentile strategy**: the process only stores raw histogram buckets; P99 is computed query-side via `histogram_quantile()` (same as official client libraries; brpc computes sliding-window percentiles in-process — a different trade-off).
- **Convention details**: error counters are pre-registered at 0 on startup (distinguishing "zero errors" from "series absent"); monotonic gauges with a `_total` suffix export TYPE counter; 15-digit precision prevents large values degrading into scientific notation.
