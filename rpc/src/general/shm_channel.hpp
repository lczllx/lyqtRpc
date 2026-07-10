#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
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
#include <memory>        // std::shared_ptr

#include "abstract.hpp"
#include "message.hpp"
#include "publicconfig.hpp"

namespace lcz_rpc
{
    static constexpr size_t SHM_CTRL_SIZE = 4096;

    struct ShmRingBuf
    {
        std::atomic<uint64_t> write_idx{0}; // 生产者独占写（谁写谁 load relaxed）
        std::atomic<uint64_t> read_idx{0};  // 消费者独占写（对端 load acquire）
        uint64_t data_size = 0;
        uint32_t _pad;
    };

    /* 让 ShmControl 的起始地址一定是 64 的倍数，
    让 atomic 操作不和无关数据挤在同一个 cache line 上*/
    struct alignas(64) ShmControl
    {
        // Server create() 完成后 store(true, release)，Client 忙等 load(acquire)
        std::atomic<bool> ready{false};

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
        //   shm_open(O_CREAT|O_RDWR, 0666)
        //   → ftruncate(CTRL_SIZE + req_size + resp_size)
        //   → mmap(MAP_SHARED)
        //   → placement new ShmControl(_addr)
        //   → ctrl->ready.store(true, release)

        // ====== Client 端 ======
        bool open(const std::string &name);
        //   shm_open → fstat → mmap → while(!ready.load(acquire)) pause

        // ====== 请求方向（Client → Server） ======
        bool write_request(const std::string &body, MsgType type, int max_retries = 3);
        bool read_request(std::string &body, MsgType &type);

        // ====== 响应方向（Server → Client） ======
        bool write_response(const std::string &body, MsgType type, int max_retries = 3);
        bool read_response(std::string &body, MsgType &type);

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
        void set_req_notify_fd(int fd)  { _req_notify_fd = fd; }
        void set_resp_notify_fd(int fd) { _resp_notify_fd = fd; }

        // 写完数据后通知对端（Producer 调）
        void notify_req()  { uint64_t one=1; if(_req_notify_fd>=0) ::write(_req_notify_fd,&one,8); }
        void notify_resp() { uint64_t one=1; if(_resp_notify_fd>=0) ::write(_resp_notify_fd,&one,8); }

        // ====== 缓冲指针（FlatBuffers 零拷贝写入用） ======
        char *req_write_ptr(size_t &contig_avail);  // 请求 buffer 可写区起始 + 连续可用
        char *resp_write_ptr(size_t &contig_avail); // 响应 buffer 可写区起始 + 连续可用
        void req_commit(size_t body_len, MsgType type);   // 写完提交游标 + write(notify_fd)
        void resp_commit(size_t body_len, MsgType type);  // 同上，响应方向

        // ====== 生命周期 ======
        ~ShmChannel();
        bool is_open() const { return _addr != nullptr; }
        void destroy();
        // munmap → close → shm_unlink（仅 creator）

    private:
        // 内部读写，被四个公开方法委托
        bool write_to(ShmRingBuf &ch, char *data_base,
                      const std::string &body, MsgType type, int max_retries);
        bool read_from(ShmRingBuf &ch, char *data_base,
                       std::string &body, MsgType &type);

        int _shm_fd = -1; // shm_open 返回
        void *_addr = nullptr; // mmap 返回的虚拟地址
        ShmControl *_ctrl = nullptr; // = _addr（两个指针指向同一块内存的开头）
        char *_req_data = nullptr;// = _addr + 4096
        char *_resp_data = nullptr;// = _addr + 4096 + req_buf_size
        size_t _total_size;
        std::string _name;
        int _req_notify_fd = -1;
        int _resp_notify_fd = -1;
        bool _is_creator = false;
    };
}
