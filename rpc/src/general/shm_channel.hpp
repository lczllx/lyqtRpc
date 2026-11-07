#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <span>
#include "log_system/lcz_log.h"

#include <sys/mman.h>    // shm_open, mmap, munmap, shm_unlink
#include <sys/stat.h>    // S_IRUSR, S_IWUSR（shm_open 权限）
#include <fcntl.h>       // O_CREAT, O_RDWR, O_EXCL
#include <unistd.h>      // ftruncate, close, unlink
#include <sys/eventfd.h> // eventfd, EFD_NONBLOCK, EFD_SEMAPHORE
#include <sys/socket.h>  // socket, bind, listen, accept, connect, sendmsg, recvmsg
#include <sys/un.h>      // sockaddr_un, AF_UNIX
#include <arpa/inet.h>   // htonl, ntohl
#include <cstring>       // memcpy, strcpy
#include <atomic>        // std::atomic, memory_order_*
#include <cassert>       // static_assert
#include <new>           // placement new
#include <string>        // std::string
#include <string_view>   // std::string_view
#include <memory>        // std::shared_ptr

#include "abstract.hpp"
#include "message.hpp"
#include "publicconfig.hpp"

namespace lcz_rpc
{
    static constexpr size_t SHM_CTRL_SIZE = 4096;
    static constexpr uint32_t SHM_MAGIC = 0x4C435A53; // "LCZS"
    static constexpr uint32_t SHM_VERSION = 1;
    static constexpr int SHM_READY_TIMEOUT_MS = 3000; // Client 等待 Server 初始化的上限

    // 每客户端每方向默认容量。旧默认是 64MB+64MB，按 max_clients=64 算要占 8GB
    // 地址空间（tmpfs 页在写入后才分配，但一旦被写到就不会退还）。
    // 8MB 足够普通 JSON/日志类消息；大载荷作为零拷贝场景由构造参数显式调大。
    static constexpr size_t SHM_DEFAULT_BUF_SIZE = 8 * 1024 * 1024;

    // ring 满时的有界等待预算（背压）：请求方向让生产者阻塞到有空位或超时；
    // 响应方向保持 0，避免单个慢客户端把 worker 线程拖住。
    static constexpr int SHM_REQUEST_WAIT_MS = 100;
    static constexpr int SHM_RESPONSE_WAIT_MS = 0;

    struct ShmRingBuf
    {
        std::atomic<uint64_t> write_idx{0}; // 生产者独占写（谁写谁 load relaxed）
        std::atomic<uint64_t> read_idx{0};  // 消费者独占写（对端 load acquire）
        uint64_t data_size = 0;
        uint32_t _pad;
    };
    // 跨进程共享的原子量必须无锁且与地址无关：libatomic 的锁表按本进程地址记录，
    // 两个进程会各锁各的，等于没有互斥。x86_64/aarch64 的整型原子满足该条件。
    static_assert(std::atomic<uint64_t>::is_always_lock_free,
                  "SHM ring index must be always lock-free to work across processes");
    static_assert(std::atomic<uint32_t>::is_always_lock_free,
                  "SHM control fields must be always lock-free to work across processes");
    static_assert(std::atomic<bool>::is_always_lock_free,
                  "SHM ready flag must be always lock-free to work across processes");

    /* 让 ShmControl 的起始地址一定是 64 的倍数，
    让 atomic 操作不和无关数据挤在同一个 cache line 上*/
    struct alignas(64) ShmControl
    {
        // Server create() 完成后 store(true, release)，Client 忙等 load(acquire)
        std::atomic<bool> ready{false};

        // 残留段/旧格式检测：Client 在 ready 之后校验，不匹配直接拒绝。
        // creator_pid 用于 Server 在 EEXIST 时判断同名段是否还活着。
        std::atomic<uint32_t> magic{0};
        std::atomic<uint32_t> version{0};
        std::atomic<int32_t> creator_pid{0};

        ShmRingBuf req_channel;  // Client→Server
        ShmRingBuf resp_channel; // Server→Client
    };
    // 编译期断言，确保ShmControl没有奇怪的东西
    static_assert(sizeof(ShmControl) <= 4096);
    static_assert(alignof(ShmControl) == 64);
    static_assert(sizeof(ShmRingBuf) == 32);

    class ShmChannel
    {
    public:
        using ptr = std::shared_ptr<ShmChannel>;

        // ====== Server 端 ======
        bool create(const std::string &name, size_t req_buf_size, size_t resp_buf_size);
        //   shm_open(O_CREAT|O_RDWR|O_EXCL, 0600)；EEXIST 时仅回收创建者已退出的残留段
        //   → ftruncate(CTRL_SIZE + req_size + resp_size)
        //   → mmap(MAP_SHARED)
        //   → placement new ShmControl(_addr)
        //   → ctrl->ready.store(true, release)

        // ====== Client 端 ======
        bool open(const std::string &name, int timeout_ms = SHM_READY_TIMEOUT_MS);
        //   shm_open → fstat → mmap → 等 ready（超时返回 false）→ 校验 magic/version

        // ====== 请求方向（Client → Server） ======
        // wait_ms > 0 时 ring 满会阻塞等待空位（有界背压），超时才返回 false
        // read_* 拷贝到 std::string；read_*_view 直接给出 ring 内的视图（见下）
        bool write_request(std::string_view body, MsgType type, int wait_ms = SHM_REQUEST_WAIT_MS);
        bool read_request(std::string &body, MsgType &type);

        // ====== 响应方向（Server → Client） ======
        bool write_response(std::string_view body, MsgType type, int wait_ms = SHM_RESPONSE_WAIT_MS);
        bool read_response(std::string &body, MsgType &type);

        // ====== 零拷贝读视图（与 write_ptr 对称） ======
        // body 直接指向 ring 内已提交的帧，不做拷贝；read_idx 在 release 前停在
        // 该帧起点，生产者据此认为这块内存仍被占用，不会覆盖它。
        // 契约：同一方向 read → release 必须成对；未 release 时再次 read 返回 false
        //       （推 read_idx 会放行生产者，让正在读的视图失效）。
        struct FrameView
        {
            std::span<const char> body;      // 不含 8B 帧头
            MsgType type = MsgType::REQ_RPC; // 帧头带出的消息类型

            std::string_view body_view() const { return {body.data(), body.size()}; }
        };
        bool read_request_view(FrameView &view);
        bool read_response_view(FrameView &view);
        void release_request_view();
        void release_response_view();

        // 每方向 ring 的容量（字节）；发送前可据此判断单帧是否必然放不下
        size_t req_capacity() const { return _ctrl ? _ctrl->req_channel.data_size : 0; }
        size_t resp_capacity() const { return _ctrl ? _ctrl->resp_channel.data_size : 0; }

        // ====== 初始化握手（单客户端模式，保留向后兼容） ======
        bool setup_notify_server(const std::string &notify_path);
        bool setup_notify_client(const std::string &notify_path);

        // ====== 初始化握手（多客户端模式） ======
        // Server 端：在已 accept 的 conn_fd 上交换 eventfd + 发送 SHM 名字
        static bool handshake_server(int conn_fd, int &req_fd, int &resp_fd,
                                     const std::string &shm_name);
        // Client 端：在已 connect 的 conn_fd 上交换 eventfd + 接收 SHM 名字
        static bool handshake_client(int conn_fd, int &req_fd, int &resp_fd,
                                     std::string &shm_name);

        int req_notify_fd() const { return _req_notify_fd; }   // Client→Server
        int resp_notify_fd() const { return _resp_notify_fd; } // Server→Client

        // 外部设置 eventfd（多客户端 handshake 由外部传入）
        void set_req_notify_fd(int fd) { _req_notify_fd = fd; }
        void set_resp_notify_fd(int fd) { _resp_notify_fd = fd; }

        // 写完数据后通知对端（Producer 调）
        void notify_req()
        {
            uint64_t one = 1;
            if (_req_notify_fd >= 0)
                ::write(_req_notify_fd, &one, 8);
        }
        void notify_resp()
        {
            uint64_t one = 1;
            if (_resp_notify_fd >= 0)
                ::write(_resp_notify_fd, &one, 8);
        }

        // ====== 缓冲指针（FlatBuffers/Protobuf 零拷贝写入用） ======
        // 返回可写 body 区 span（空 span = 无可写连续空间），8 字节 frame header 由内部预留
        std::span<char> req_write_ptr();
        std::span<char> resp_write_ptr();
        void req_commit(size_t body_len, MsgType type);  // 写完提交游标 + write(notify_fd)
        void resp_commit(size_t body_len, MsgType type); // 同上，响应方向

        // ====== 生命周期 ======
        ~ShmChannel();
        bool is_open() const { return _addr != nullptr; }
        // 对端（创建者）是否还活着：ready 仍置位且创建者进程存在。
        // 用于客户端后台线程发现 Server 退出/崩溃后把连接标记为断开。
        bool peerAlive() const;
        void destroy();
        // munmap → close → shm_unlink（仅 creator）

    private:
        // 内部读写，被四个公开方法委托
        bool write_to(ShmRingBuf &ch, char *data_base,
                      std::string_view body, MsgType type, int wait_ms);
        bool read_from(ShmRingBuf &ch, char *data_base,
                       std::string &body, MsgType &type);

        // 读取一帧视图：跳过帧就地推进 read_idx，真实帧只记录 pinned_end
        bool read_frame_view(ShmRingBuf &ch, char *data_base,
                             uint64_t &pinned_end, FrameView &view);
        void release_frame_view(ShmRingBuf &ch, uint64_t &pinned_end);

        // 未释放读视图的帧尾（0 = 无）。只由该方向的消费者线程读写。
        uint64_t _req_pin_end = 0;
        uint64_t _resp_pin_end = 0;

        int _shm_fd = -1;            // shm_open 返回
        void *_addr = nullptr;       // mmap 返回的虚拟地址
        ShmControl *_ctrl = nullptr; // = _addr（两个指针指向同一块内存的开头）
        char *_req_data = nullptr;   // = _addr + 4096
        char *_resp_data = nullptr;  // = _addr + 4096 + req_buf_size
        size_t _total_size;
        std::string _name;
        int _req_notify_fd = -1;
        int _resp_notify_fd = -1;
        bool _is_creator = false;
    };
}
