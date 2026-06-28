#include "./shm_channel.hpp"

namespace lcz_rpc
{
    // ====== Server 端 ======
    bool ShmChannel::create(const std::string &name, size_t req_buf_size, size_t resp_buf_size)
    {
        // 1. shm_open：在 /dev/shm/ 下创建共享内存对象（tmpfs，纯内存不落盘）
        //    O_CREAT | O_EXCL：新建，已存在则失败，防止两个 Server 进程冲突
        int fd = shm_open(
            name.c_str(),
            O_CREAT | O_RDWR | O_EXCL,
            S_IRUSR | S_IWUSR // 仅 owner 读写
        );
        if (fd < 0)
        {
            LCZ_ERROR("shm_open %s failed, errno=%d", name.c_str(), errno);
            return false;
        }

        // 2. ftruncate：设置大小。必须先调，否则 mmap 区域访问越界 → SIGBUS
        //    布局：控制区(4KB) + 请求数据区 + 响应数据区
        _total_size = SHM_CTRL_SIZE + req_buf_size + resp_buf_size;
        int ret = ftruncate(fd, _total_size);
        if (ret < 0)
        {
            LCZ_ERROR("ftruncate %s failed, errno=%d", name.c_str(), errno);
            close(fd);
            shm_unlink(name.c_str());
            return false;
        }

        // 3. mmap：把内核分配的物理内存映射到当前进程虚拟地址空间
        //    MAP_SHARED：Server 写入对 Client 立即可见（映射同一物理页）
        _addr = mmap(nullptr, _total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (_addr == MAP_FAILED)
        {
            LCZ_ERROR("mmap %s failed, errno=%d", name.c_str(), errno);
            close(fd);
            shm_unlink(name.c_str());
            return false;
        }

        // 4. placement new：在 mmap 原始内存上构造 ShmControl
        //    std::atomic 有内部状态，不能 memcpy，必须原地构造
        _ctrl = new (_addr) ShmControl();
        _ctrl->req_channel.data_size = req_buf_size;
        _ctrl->resp_channel.data_size = resp_buf_size;

        // 5. 计算两个数据区的起始指针
        _req_data = (char *)_addr + SHM_CTRL_SIZE; // 控制区后面
        _resp_data = _req_data + req_buf_size;     // 请求区后面

        _shm_fd = fd;
        _is_creator = true;
        _name = name;

        // 6. 宣布就绪。release 保证以上所有初始化在 store 之前对 Client 可见
        _ctrl->ready.store(true, std::memory_order_release);
        return true;
    }

    // ====== Client 端 ======
    bool ShmChannel::open(const std::string &name)
    {
        if (_addr)
        {
            LCZ_ERROR("shm_open %s failed, already open", name.c_str());
            return false;
        }
        // 1. shm_open：打开已有共享内存，只读，不加 O_CREAT | O_EXCL
        int fd = shm_open(name.c_str(), O_RDWR, 0);
        if (fd < 0)
        {
            LCZ_ERROR("shm_open %s failed, errno=%d (Server 是否已启动?)",
                      name.c_str(), errno);
            return false;
        }

        // 2. fstat：拿 Server 用 ftruncate 设置的总大小
        struct stat st;
        if (fstat(fd, &st) < 0)
        {
            LCZ_ERROR("fstat %s failed, errno=%d", name.c_str(), errno);
            close(fd);
            return false;
        }
        _total_size = static_cast<size_t>(st.st_size);

        // 3. mmap：映射到本进程虚拟地址空间。MAP_SHARED → 和 Server 同一物理内存
        _addr = mmap(nullptr, _total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (_addr == MAP_FAILED)
        {
            LCZ_ERROR("mmap %s failed, errno=%d", name.c_str(), errno);
            close(fd);
            return false;
        }

        // 4. 取控制区指针 — Server 已用 placement new 原地构造好，直接转
        _ctrl = static_cast<ShmControl *>(_addr);

        // 5. 自旋等 Server 就绪。acquire 保证看到 data_size 等初始化值
        while (!_ctrl->ready.load(std::memory_order_acquire))
        {
            __asm__("pause");
            // Server 在 us 级就绪，不忙太久
        }

        // 6. 从控制区读 data_size，计算两个数据区的起始指针
        size_t req_size = _ctrl->req_channel.data_size;
        size_t resp_size = _ctrl->resp_channel.data_size;
        _req_data = static_cast<char *>(_addr) + SHM_CTRL_SIZE;
        _resp_data = _req_data + req_size;

        _shm_fd = fd;
        _name = name;
        return true;
    }

    // ====== 内部：写一帧到指定通道 ======
    // ch        — req_channel 或 resp_channel
    // data_base — 该通道数据区的起始地址
    // body      — 已序列化好的消息体
    // type      — 消息类型，告诉对端反序列化成什么
    // max_retries — ring buffer 满时最多重试几次（consumer 消费后空间释放）
    bool ShmChannel::write_to(ShmRingBuf &ch, char *data_base,
                              const std::string &body, MsgType type, int max_retries)
    {
        // 帧格式：| 4B frame_len | 4B msg_type | body |
        // frame_len = 8 + body_len，不含自身 4B
        uint32_t body_len = body.size();
        uint32_t frame_len = 8 + body_len;
        uint64_t ds = ch.data_size; // ring buffer 数据区总大小

        for (int retry = 0; retry < max_retries; ++retry)
        {
            // 1. 读生产者位置（自己写的，relaxed 够用）
            // 防一个场景：进程崩溃后重启，Producer 需要从共享内存里读出上次写到哪了。
            // 本地变量丢了，但 write_idx 还在 mmap 里。
            uint64_t w = ch.write_idx.load(std::memory_order_relaxed);
            //    读消费者位置（对端写的，acquire 才能看到对方的消费进度）
            uint64_t r = ch.read_idx.load(std::memory_order_acquire);

            // 2. 检查空间：总大小 - 已用 = data_size - (w - r)
            //    uint64 回绕自动正确：如 w=5 r=UINT64_MAX-10 → 5-(-11)=16 ✓
            if (ds - (w - r) < frame_len)
                continue; // 不够，等 consumer 消费后重试

            // 3. 计算写入位置
            uint64_t offset = w % ds;      // 环形偏移
            uint64_t contig = ds - offset; // 从 offset 到 buffer 末尾还剩多少连续字节

            // 4. 尾部连续空间不够容下整帧 → 写跳过帧，从 buffer 头重新开始
            if (contig < frame_len)
            {
                // 写一个 frame_len=0 的标记，consumer 读到 0 知道这是填充
                if (contig >= 4)
                {
                    // 正常写跳过帧
                    static const uint32_t kSkip = 0;
                    memcpy(data_base + offset, &kSkip, 4);
                }
                else
                {
                    // 尾部 1-3 字节残留，手动清零。否则 Consumer 读 frame_len 时
                    // 会跨 buffer 边界读到下一帧的数据，形成垃圾值
                    static const char kZero[3] = {0, 0, 0};
                    memcpy(data_base + offset, kZero, contig);
                }
                w += contig;
                offset = 0;
                // 跳过后重新检查
                contig = r % ds;
                if (ds - (w - r) < frame_len)
                    continue; // 跳过帧浪费了尾部空间，总量不够了，等 consumer 消费
            }

            // 5. 写 frame_len（4B，不含自身）
            memcpy(data_base + offset, &frame_len, 4);
            //    写 msg_type（4B，int32 格式）
            int32_t mt = static_cast<int32_t>(type);
            memcpy(data_base + offset + 4, &mt, 4);
            //    写 body（变长）
            memcpy(data_base + offset + 8, body.data(), body_len);

            // 6. 公布新位置
            //    release：保证上面三个 memcpy 全部完成，对端 acquire 才能看到
            ch.write_idx.store(w + frame_len, std::memory_order_release);
            return true;
        }
        return false; // 重试耗完，ring buffer 一直满
    }

    // ====== 内部：从指定通道读一帧 ======
    bool ShmChannel::read_from(ShmRingBuf &ch, char *data_base,
                               std::string &body, MsgType &type)
    {
        // 1. 读自己位置（relaxed）+ 读生产者位置（acquire）
        uint64_t r = ch.read_idx.load(std::memory_order_relaxed);
        uint64_t w = ch.write_idx.load(std::memory_order_acquire);
        if (w == r)
            return false; // 空了，没有新数据

        uint64_t ds = ch.data_size;
        uint64_t offset = r % ds; // 环形偏移

        // 2. 读帧头 4B → frame_len
        uint32_t frame_len;
        memcpy(&frame_len, data_base + offset, 4);

        // 3. frame_len == 0 → 生产者写的跳过帧
        //    直接跳到 buffer 头，重试一次
        if (frame_len == 0)
        {
            // ds - offset = 从当前位置到尾部的字节数，全部跳过
            ch.read_idx.store(r + (ds - offset), std::memory_order_release);
            return read_from(ch, data_base, body, type);
        }

        // 4. 读 msg_type（4B int32 → MsgType 枚举）
        int32_t mt;
        memcpy(&mt, data_base + offset + 4, 4);
        type = static_cast<MsgType>(mt);

        // 5. 读 body（frame_len - 8 字节，减去帧头）
        body.assign(data_base + offset + 8, frame_len - 8);

        // 6. 确认消费完成，挪 read_idx
        ch.read_idx.store(r + frame_len, std::memory_order_release);
        return true;
    }

    // ====== 请求方向（Client → Server） ======
    // 委托到 write_to / read_from，走 req_channel
    bool ShmChannel::write_request(const std::string &body, MsgType type, int max_retries)
    {
        return write_to(_ctrl->req_channel, _req_data, body, type, max_retries);
    }
    bool ShmChannel::read_request(std::string &body, MsgType &type)
    {
        return read_from(_ctrl->req_channel, _req_data, body, type);
    }

    // ====== 响应方向（Server → Client） ======
    // 委托到 write_to / read_from，走 resp_channel
    bool ShmChannel::write_response(const std::string &body, MsgType type, int max_retries)
    {
        return write_to(_ctrl->resp_channel, _resp_data, body, type, max_retries);
    }
    bool ShmChannel::read_response(std::string &body, MsgType &type)
    {
        return read_from(_ctrl->resp_channel, _resp_data, body, type);
    }

    // 析构自动清理，防止测试或异常路径泄漏 mmap / fd
    ShmChannel::~ShmChannel()
    {
        destroy();
    }

    // ====== 生命周期 ======
    void ShmChannel::destroy()
    {
        // 1. munmap：解除虚拟地址空间映射
        if (_addr)
        {
            munmap(_addr, _total_size);
            _addr = nullptr;
        }
        // 2. close：关闭共享内存文件描述符
        if (_shm_fd >= 0)
        {
            close(_shm_fd);
            _shm_fd = -1;
        }
        // 3. shm_unlink：从 /dev/shm 删除文件（仅创建者）
        if (_is_creator && !_name.empty())
        {
            shm_unlink(_name.c_str());
        }
        // 4. 清空指针，防止悬挂引用
        _ctrl = nullptr;
        _req_data = nullptr;
        _resp_data = nullptr;
    }
    // ====== SCM_RIGHTS 工具函数：跨进程传递文件描述符 ======
    // 内核做的事：在目标进程的 fd 表中创建新条目，指向同一个 struct file
    // 所以两个进程里的 fd 编号可以不同，但它们指向同一个 eventfd 内核对象

    // 通过 Unix 域 socket 连接发送一个 fd 给对端进程
    static void send_fd(int conn_fd, int fd_to_send)
    {
        char buf[1] = {'x'};                      // 至少 1 字节数据，否则 SCM_RIGHTS 不生效
        struct iovec io = {buf, sizeof(buf)};

        struct msghdr msg = {};
        msg.msg_iov    = &io;
        msg.msg_iovlen = 1;

        // CMSG_SPACE 含对齐 padding，必须用 SPACE 不能用 CMSG_LEN
        union {
            struct cmsghdr hdr;
            char buf[CMSG_SPACE(sizeof(int))];
        } cbuf;

        msg.msg_control    = &cbuf;
        msg.msg_controllen = sizeof(cbuf);

        struct cmsghdr *cmsg  = CMSG_FIRSTHDR(&msg);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type  = SCM_RIGHTS;
        cmsg->cmsg_len   = CMSG_LEN(sizeof(int));
        memcpy(CMSG_DATA(cmsg), &fd_to_send, sizeof(int));

        sendmsg(conn_fd, &msg, 0);
        // 发送方可以关闭 fd——对端已在内核拿到新 fd，指向同一对象
    }

    // 从 Unix 域 socket 连接接收对端发来的 fd
    static int recv_fd(int conn_fd)
    {
        char buf[1];
        struct iovec io = {buf, sizeof(buf)};

        struct msghdr msg = {};
        msg.msg_iov    = &io;
        msg.msg_iovlen = 1;

        union {
            struct cmsghdr hdr;
            char buf[CMSG_SPACE(sizeof(int))];
        } cbuf;

        msg.msg_control    = &cbuf;
        msg.msg_controllen = sizeof(cbuf);

        recvmsg(conn_fd, &msg, 0);
        int fd;
        memcpy(&fd, CMSG_DATA(CMSG_FIRSTHDR(&msg)), sizeof(int));
        return fd;
    }

    // ====== Server 端：bind + listen → accept → 交换 eventfd ======
    // Server 创建 req_notify_fd，通过 Unix socket 发给 Client，
    // 然后从同一连接收 Client 创建的 resp_notify_fd。
    // 握手完成后 conn_fd 关闭，后续通知直接读写 eventfd。
    bool ShmChannel::setup_notify_server(const std::string &notify_path)
    {
        // 1. 创建 Unix 域 socket，绑定到一个文件系统路径
        int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (listen_fd < 0) {
            LCZ_ERROR("setup_notify_server: socket failed errno=%d", errno);
            return false;
        }

        struct sockaddr_un addr = {};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, notify_path.c_str(), sizeof(addr.sun_path) - 1);
        unlink(notify_path.c_str());               // 清理上次残留的 socket 文件
        if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            LCZ_ERROR("setup_notify_server: bind %s failed errno=%d", notify_path.c_str(), errno);
            close(listen_fd);
            return false;
        }
        if (listen(listen_fd, 1) < 0) {
            LCZ_ERROR("setup_notify_server: listen failed errno=%d", errno);
            close(listen_fd);
            unlink(notify_path.c_str());
            return false;
        }

        // 2. 阻塞等 Client 连上来
        int conn_fd = accept(listen_fd, nullptr, nullptr);
        if (conn_fd < 0) {
            LCZ_ERROR("setup_notify_server: accept failed errno=%d", errno);
            close(listen_fd);
            unlink(notify_path.c_str());
            return false;
        }

        // 3. 创建自己的 eventfd（Client 写完请求后 write 这个 fd 通知 Server）
        _req_notify_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE);
        if (_req_notify_fd < 0) {
            LCZ_ERROR("setup_notify_server: eventfd failed errno=%d", errno);
            close(conn_fd); close(listen_fd);
            return false;
        }

        // 4. 把 req_notify_fd 发给 Client，收 Client 的 resp_notify_fd
        send_fd(conn_fd, _req_notify_fd);
        _resp_notify_fd = recv_fd(conn_fd);

        // 5. 握手完成，关闭 Unix socket（eventfd 已独立可用）
        close(conn_fd);
        close(listen_fd);

        LCZ_INFO("setup_notify_server: req_fd=%d resp_fd=%d", _req_notify_fd, _resp_notify_fd);
        return true;
    }

    // ====== Client 端：connect → 交换 eventfd ======
    bool ShmChannel::setup_notify_client(const std::string &notify_path)
    {
        // 1. 创建 Unix 域 socket 并连接 Server
        int conn_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (conn_fd < 0) {
            LCZ_ERROR("setup_notify_client: socket failed errno=%d", errno);
            return false;
        }

        struct sockaddr_un addr = {};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, notify_path.c_str(), sizeof(addr.sun_path) - 1);
        if (connect(conn_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            LCZ_ERROR("setup_notify_client: connect %s failed errno=%d (server 是否已启动?)",
                      notify_path.c_str(), errno);
            close(conn_fd);
            return false;
        }

        // 2. 收 Server 的 req_notify_fd
        _req_notify_fd = recv_fd(conn_fd);

        // 3. 创建自己的 eventfd（Server 写完响应后 write 这个 fd 通知 Client）
        _resp_notify_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE);
        if (_resp_notify_fd < 0) {
            LCZ_ERROR("setup_notify_client: eventfd failed errno=%d", errno);
            close(conn_fd);
            return false;
        }

        // 4. 把自己的 resp_notify_fd 发给 Server
        send_fd(conn_fd, _resp_notify_fd);

        // 5. 握手完成
        close(conn_fd);

        LCZ_INFO("setup_notify_client: req_fd=%d resp_fd=%d", _req_notify_fd, _resp_notify_fd);
        return true;
    }

} // namespace lcz_rpc

