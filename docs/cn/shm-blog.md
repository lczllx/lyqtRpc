
# 从 TCP 90μs 到 SHM 28μs：我的 RPC 框架零拷贝优化历程

> 同一个 echo 请求，本机 TCP 走完要 90μs，换成共享内存只要 28μs。记录一下踩的坑，也聊聊两种通信方式到底差在哪。

---

## 为什么会慢——TCP 本机通信其实绕了远路

先讲一个不严谨但好理解的类比。

你去器材室拿东西。SHM 是你自己走过去拿——一次搞定。TCP 是你打电话叫你朋友去拿，你朋友可能又要叫他的朋友去拿——中间多了好几次传话，每次传话都是一次开销。

对应到代码层面：本机 TCP 通信虽然不经过物理网线（走 loopback），但数据还是要走一遍内核协议栈：

1. `send()` 把数据从用户态拷进内核
2. 内核分配 sk_buff、可能克隆一份
3. loopback 设备把包从发送队列注回接收队列
4. `recv()` 把数据从内核拷回用户态

每一步都是实在的 CPU 拷贝。loopback 设备自己在注释里都写了——"The loopback device is special. There is no DMA."——没有 DMA 意味着数据搬运全靠 CPU，看着像经过了网卡，实际上只是在内核里兜了一圈。

我截了这四个拷贝点在内核源码里的位置：

| 拷贝 | 文件 | 函数 | 干了什么 |
|---|---|---|---|
| ① | `include/net/sock.h:2315` | `skb_do_copy_data_nocache` → `copy_from_iter_full` | 用户态数据拷进内核 skb |
| ② | `net/ipv4/tcp.c:1310` | `skb_copy_to_page_nocache` | 协议栈内部可能再分配 skb 并拷贝 |
| ③ | `drivers/net/loopback.c:70` | `loopback_xmit` → `__netif_rx` | loopback 回注，无 DMA |
| ④ | `net/ipv4/tcp.c:2525` | `copy_to_iter` | 内核 skb 拷回用户态 |

![拷贝①：skb_do_copy_data_nocache → copy_from_iter_full](../../flowchat/1.png)
![拷贝②：skb_copy_to_page_nocache](../../flowchat/2.png)
![拷贝③：loopback_xmit → __netif_rx](../../flowchat/3.png)
![拷贝④：copy_to_iter](../../flowchat/4.png)
![loopback 注释："The loopback device is special. There is no DMA."](../../flowchat/5.png)

用 strace 验证也很直观——TCP 客户端发 100 个 echo 请求，光 `write` 系统调用就 237 次：
![TCP strace：237 次 write，36 次 read](../../flowchat/tcp_strace.png)
```
$ strace -c -e trace=write,read ./benchmark_client single echo 100 1 1

write  237 次    read  36 次    → 273 syscall / 100 请求
                                  ~2.7 次系统调用/请求
```

每次 send/recv 意味着至少一次内核拷贝。每个请求 3~4 次 CPU 拷贝（send 拷入 + loopback 回注 + recv 拷出，skb 克隆不一定每次都触发），100 个请求数据在内存里被搬了三四百次。

---

## SHM 做了什么——把"传话"全干掉

共享内存说起来就一句话——Client 和 Server 映射同一块物理内存。数据写进去，对面直接读。不经过内核，没有 send/recv，协议栈完全不参与。

在我的项目数据路径里：Client 把 Proto 序列化后 memcpy 进 ring buffer，写完用 eventfd 通知 Server，Server 被 epoll 唤醒，直接从 mmap 内存读数据，`body.assign` 完事。

```
Client 用户 Buffer
  │  ① memcpy → ring buffer（用户态，唯一一次）
  ▼
ring buffer（mmap，Server 同一物理页 → 不需要 copy_to_user）
  │  ② body.assign（用户态，直接从 mmap 读）
  ▼
Server 用户 Buffer
```

shm_channel.cc 里写端就这三行 memcpy——写完 frame_len、msg_type、body，store(write_idx, release) 公布。读端更直接，`body.assign(data_base + offset + 8, frame_len - 8)`，一行从 mmap 读到 string。

![shm_channel.cc 写端：memcpy frame_len / msg_type / body 进 ring buffer](../../flowchat/6%20%20(1).png)
![shm_channel.cc 读端：body.assign 直接从 mmap 内存读到 string](../../flowchat/6%20%20(2).png)

同样 100 个请求 strace：
![SHM strace：2 次 write，7 次 read](../../flowchat/shm_strace.png)

```
$ strace -c -e trace=write,read ./shm_proto_client

write   2 次     read   7 次    → 9 syscall
                                 0 次数据路径 syscall
```

TCP 273 次 → SHM 9 次。那 9 次是 eventfd 通知——和实际数据读写没关系，真正的数据搬运一次系统调用都没触发。TCP 相当于你在内核里找人帮你搬东西——你得先打通他的电话（系统调用），他再帮你搬（内核拷贝），然后告诉你搬完了（返回用户态）。SHM 是你自己走过去搬——一步到位。

### ring buffer 上一帧怎么存的

每帧 8 字节帧头 + Protobuf body：

```
┌──────────┬──────────┬────────────────┐
│frame_len │ msg_type │     body       │
│  4B      │  4B      │   变长          │
└──────────┴──────────┴────────────────┘
```

frame_len = 8 + body_len，不含自身那 4B。读端先读 frame_len，不够一帧就等下次 epoll，够就把整个 frame 读走，read_idx 往前挪，那块空间就算释放了。

有个特殊情况——ring buffer 尾巴上剩的 bytes 不够写一整帧。这时候 Producer 写一个 frame_len=0 的标记（跳过帧），然后把 `write_idx` 加上这段尾部长度，自然绕回到 buffer 头部，Consumer 读到 frame_len=0 后同样把 `read_idx` 加上尾部长度，再从头部继续读。双方都不是"重置为 0"——是索引正常往前推进，取模后自然回到头部。

write_idx 和 read_idx 都是 uint64_t，一直往前加，用到溢出那天？其实 uint64 减法天然模 2^64——绕回了也无所谓，`w - r` 算出来的已用字节数还是对的。SPSC ring buffer 不需要额外处理回绕。

### eventfd 怎么通知对端

写完一帧不是让对端轮询 write_idx——那样 CPU 吃满没意义，用 eventfd，Linux 内核提供的轻量事件对象。

```
Producer：memcpy 到 ring buffer → write(eventfd, 8B)  ← 一次 syscall
Consumer：epoll 被唤醒 → read(eventfd, 8B) → 读 ring buffer
```

`EFD_SEMAPHORE` 模式：write 一次计数器 +1，read 一次返回值 1 且计数器 -1。Client 连续发 5 个请求（write 5 次），Server 的 epoll 醒一次、read 5 次才能把计数器清空——不会丢通知。

还有一个问题是 eventfd 怎么跨进程传？eventfd 创建出来是个匿名 fd，没有文件名，另一个进程不能 `open`，用 SCM_RIGHTS——Unix domain socket 的带外数据，内核直接把 sender 的 fd 复制一份到 receiver 的 fd 表，两个进程 fd 编号不一样（server fd=5, client fd=7），但指向内核里同一个 eventfd 对象。

### 内存序：先写数据，后公布

ring buffer 没锁，靠 `std::atomic` 的 release/acquire 语义保证顺序。

```
Producer：① memcpy 数据    ② store(write_idx, release)  ← 顺序绝不能反
Consumer：③ load(write_idx, acquire)  ④ memcpy 读数据
```

release 保证：① 在 ② 之前一定完成，对端 acquire 之后一定看到完整数据。如果 CPU 把这两步重排了——先公布 write_idx 再 memcpy——Consumer 就会读到半帧垃圾。帧头是旧数据、body 是新数据，解析直接出错。x86 上 release-store 生成的是普通 mov（TSO 内存模型天然保序），但编译器不能把 memcpy 挪到 store 后面——这就是 `std::memory_order_release` 的作用：约束编译器，不依赖 CPU 特性。

---

## 模块实现过程中的思考

### 两个 ring buffer 还是一个大 buffer

最初想过只用一块共享内存，双向复用——Client 写 req 和 Server 写 resp 都在同一块 buffer 上。但这样一来 req 和 resp 的 write_idx 会被两个方向竞争，要保证正确就得加锁。在本机微服务场景下锁竞争约 20ns 起，并发再高一点 pthread mutex 直接 futex 进内核——花了这么大力气省掉 TCP 协议栈，结果被一把锁拖回去。所以每个 Client 各分一对独立的 ring buffer（req 1MB + resp 1MB），每个方向只有一个 Producer，完全不用锁。

这就是典型的"用内存换锁"——100 个 Client 占 200MB 共享内存，但在本机微服务的场景下完全可以接受。

### placement new 不能省略

ShmControl 里有两个 `std::atomic<uint64_t>`（write_idx / read_idx）。对 atomic 来说，构造函数不仅是把初值写进内存——还要初始化锁标志位（用来做 `is_lock_free()` 的判断）和其他内部实现状态。

我第一次直接在栈上构造了一个 ShmControl，然后 `memcpy(&stack_obj, mmap_addr, sizeof)`。Link 过了，测试跑了，偶发挂。排查了半天才反应过来——`memcpy` 只搬了 bit pattern，atomic 的内部元数据还在栈上那坨已经被析构了的内存里。mmap 上的 atomic 处于"从未构造"的状态，后续 store/load 行为未定义。不崩溃是侥幸，崩溃或丢更新才是正常。placement new 直接在 mmap 上原地调构造函数，一步到位，atomic 的所有状态都在共享内存里。

### FlatBufferBuilder 为什么不能在 ring buffer 上原地构造

FlatBufferBuilder 构造时需要一块连续的 buffer。但 ring buffer 的“可用空间”是环状的——尾部一段、头部一段，不保证连续。FBB 的构造函数不关心你是不是环形，它只管 `buffer + size` 这块连续区域。当 ring buffer 的剩余连续空间小于 `initial_size + 16` 时，FBB 直接抛 `std::bad_alloc`。

我曾经尝试过写一个自定义 Allocator 让 FBB 能跨 buffer 尾部→头部环绕分配，但 FBB 内部大量用了相对偏移指针——跨段后指针计算全乱。结论是不值得。写端老老实实在堆上构造，一次 memcpy 进 ring buffer。读端用 FlatBuffers 的 `GetRoot<T>()` 可以直接从 mmap 内存上原地读取字段——读端做到了真正的零拷贝，这才是 FlatBuffers 的正确用法。

### `_rid` 没被序列化——TCP 帮忙掩盖的 bug

`BaseMessage::_rid` 是每次 RPC 调用生成的唯一 ID，Client 靠它把收到的响应和之前发出的请求对应起来。但这个字段在 JSON 和 Proto envelope 里从来没被写进去。TCP 路径上单连接按序收发，第一个响应一定先于第二个回来，隐式保证了顺序一致性，匹配从来没出错。

切换到 SHM 之后，ring buffer 是环形无锁的——Client 连着发 3 个请求，Server 处理完的响应写入顺序可能和请求顺序不完全一致。Client 收到一个 resp，取 `resp->rid()` 和 `expected_rid` 对比——永远不等，请求挂起。查了半天才发现 `_rid` 根本没出现在序列化数据里。后来在 `JsonMessage::serialize()` 和 `rpc_envelope.proto` 里加上了 id 字段，TCP 和 SHM 两端都修了。

---

## 性能数据和理解

```
单线程 echo 16B：

         QPS       P50
TCP      10,706    90μs
SHM      25,216    28μs

提升      2.4×     ↓69%
```


注意：同步 RPC 模式下，Client 发完一个请求就阻塞等响应，下一个请求必须等这次往返结束才发出，任何时候最多只有一个小包在飞。Nagle 算法在这种场景下根本没有积攒合并的机会，不会触发延迟发送，所以设不设 `TCP_NODELAY` 对 P50 数据基本没影响——这不是关了 Nagle，是同步调用模式天然绕开了它。

小 payload（≤4KB）的瓶颈不是 memcpy 带宽——拷贝 16 字节一瞬的事。真正耗时的是系统调用（~20μs）+ 协议栈逻辑（拥塞控制/Nagle ~30μs）+ 内核调度。SHM 把系统调用从每请求 2 次压到 1 次（只剩 eventfd 通知），协议栈全砍了——去掉这 ~50μs，就是 28μs 和 90μs 的差距。

跨机器的零拷贝是 RDMA（InfiniBand/RoCE 网卡，能做到个位数微秒）。但同机器不需要特殊硬件——`/dev/shm` 就是一台 Linux 服务器上离你最近的"RDMA"。

---

## 总结

在做这次 TCP → SHM 的迁移之前，我对进程间通信的理解就是"socket 一发一收"。做完之后才意识到——即使是本机 loopback，走 TCP 也要穿过整条内核网络栈，中间每一步拷贝和系统调用都是成本，SHM 本质上就是把"中间人"全去了，两个进程直接在同一块内存上干活。


> 严格来说，SHM 数据路径上从用户 Buffer memcpy 到 ring buffer 这一步仍然是 CPU 拷贝，不算"零拷贝"。我这里说的零拷贝，是指相比 TCP 的 3~4 次内核拷贝压到了 1 次用户态拷贝——而且该路径上已经没有 `copy_from_user` / `copy_to_user` 这类内核态拷贝了。真正全程零拷贝的场景是读端直接 `GetRoot<T>()` 原地读取 FlatBuffers，但这就要求 payload 必须按 FlatBuffers 格式构造，有一定适用条件。

三种通信方式的适用场景：

```
同机器进程之间 → SHM（零额外硬件，28μs）
同机房跨机器   → RDMA（需要 InfiniBand/RoCE 网卡，~5μs）
跨机房 / 广域网 → TCP（普适性最强，90μs+）
```

项目地址：[github.com/lczllx/lyqtRpc](https://github.com/lczllx/lyqtRpc)。
