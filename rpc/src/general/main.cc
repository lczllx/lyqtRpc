// 写在测试文件或 main() 里
#include "src/general/shm_channel.hpp"
#include <cassert>

int main() {
    using namespace lcz_rpc;

    // Server 端创建
    ShmChannel ch;
    assert(ch.create("/test_shm", 1024*1024, 1024*1024));

    // 写请求
    std::string msg = "hello from shm";
    assert(ch.write_request(msg, MsgType::REQ_RPC_PROTO));

    // 读请求 — 同一进程闭环验证
    std::string out; MsgType t;
    assert(ch.read_request(out, t));
    assert(out == msg && t == MsgType::REQ_RPC_PROTO);

    // 写响应
    std::string resp = "world";
    assert(ch.write_response(resp, MsgType::RSP_RPC_PROTO));

    // 读响应
    assert(ch.read_response(out, t));
    assert(out == resp && t == MsgType::RSP_RPC_PROTO);

    ch.destroy();
    return 0;
}
