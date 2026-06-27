#include "general/shm_channel.hpp"

namespace lcz_rpc
{
    // ====== Server 端 ======
    bool ShmChannel::create(const std::string &name, size_t req_buf_size, size_t resp_buf_size)
    {
        // 1. shm_open：在 /dev/shm/ 下创建共享内存对象（tmpfs，纯内存不落盘）
        //    O_CREAT | O_EXCL：新建，已存在则失败，防止两个 Server 进程冲突
        _shm_fd = shm_open(
            name.c_str(),
            O_CREAT | O_RDWR | O_EXCL,
            S_IRUSR | S_IWUSR           // 仅 owner 读写
        );
        if (_shm_fd < 0)
        {
            LCZ_ERROR("shm_open %s failed, errno=%d", name.c_str(), errno);
            return false;
        }

        // 2. ftruncate：设置大小。必须先调，否则 mmap 区域访问越界 → SIGBUS
        //    布局：控制区(4KB) + 请求数据区 + 响应数据区
        _total_size = SHM_CTRL_SIZE + req_buf_size + resp_buf_size;
        int ret = ftruncate(_shm_fd, _total_size);
        if (ret < 0)
        {
            LCZ_ERROR("ftruncate %s failed, errno=%d", name.c_str(), errno);
            close(_shm_fd);
            shm_unlink(name.c_str());
            return false;
        }

        // 3. mmap：把内核分配的物理内存映射到当前进程虚拟地址空间
        //    MAP_SHARED：Server 写入对 Client 立即可见（映射同一物理页）
        _addr = mmap(nullptr, _total_size, PROT_READ | PROT_WRITE, MAP_SHARED, _shm_fd, 0);
        if (_addr == MAP_FAILED)
        {
            LCZ_ERROR("mmap %s failed, errno=%d", name.c_str(), errno);
            close(_shm_fd);
            shm_unlink(name.c_str());
            return false;
        }

        // 4. placement new：在 mmap 原始内存上构造 ShmControl
        //    std::atomic 有内部状态，不能 memcpy，必须原地构造
        _ctrl = new (_addr) ShmControl();
        _ctrl->req_channel.data_size  = req_buf_size;
        _ctrl->resp_channel.data_size = resp_buf_size;

        // 5. 计算两个数据区的起始指针
        _req_data  = (char *)_addr + SHM_CTRL_SIZE;   // 控制区后面
        _resp_data = _req_data + req_buf_size;         // 请求区后面

        _is_creator = true;
        _name = name;

        // 6. 宣布就绪。release 保证以上所有初始化在 store 之前对 Client 可见
        _ctrl->ready.store(true, std::memory_order_release);
        return true;
    }

    // ====== Client 端 ======
    bool ShmChannel::open(const std::string &name)
    {
    }
    //   shm_open → fstat → mmap → while(!ready.load(acquire)) pause

    // ====== 请求方向（Client → Server） ======
    bool ShmChannel::write_request(const std::string &body, MsgType type, int max_retries = 3)
    {
    }
    bool ShmChannel::read_request(std::string &body, MsgType &type)
    {
    }

    // ====== 响应方向（Server → Client） ======
    bool ShmChannel::write_response(const std::string &body, MsgType type, int max_retries = 3)
    {
    }
    bool ShmChannel::read_response(std::string &body, MsgType &type)
    {
    }

    // ====== 初始化握手（fd 传递） ======
    bool ShmChannel::setup_notify_server(const std::string &notify_path)
    {
    }
    //   socket(AF_UNIX) → bind(listen_path) → listen → accept
    //   → eventfd(EFD_SEMAPHORE) → sendmsg(SCM_RIGHTS) → recvmsg(SCM_RIGHTS)
    //   → _req_notify_fd = 自己创建的, _resp_notify_fd = 收来的
    //   → close(conn_fd)

    bool ShmChannel::setup_notify_client(const std::string &notify_path)
    {
    }
    //   socket(AF_UNIX) → connect
    //   → recvmsg(SCM_RIGHTS) 拿到 req_fd
    //   → eventfd(EFD_SEMAPHORE) → sendmsg(SCM_RIGHTS)
    //   → _req_notify_fd = 收来的, _resp_notify_fd = 自己创建的
    //   → close(conn_fd)

    // ====== 缓冲指针（FlatBuffers 零拷贝写入用） ======
    char *ShmChannel::req_write_ptr(size_t &contig_avail)
    {

    } // 请求 buffer 可写区起始 + 连续可用
    char *ShmChannel::resp_write_ptr(size_t &contig_avail)
    {

    } // 响应 buffer 可写区起始 + 连续可用
    void ShmChannel::req_commit(size_t frame_len)
    {

    } // 写完提交游标 + write(notify_fd)

    // ====== 生命周期 ======
    void ShmChannel::destroy(const std::string &name)
    {
    }
    //   munmap → close(shm_fd) → close(req_notify_fd) → close(resp_notify_fd)
    //   → shm_unlink(name) + unlink(notify_path) (仅 creator)
} // namespace lcz_rpc
