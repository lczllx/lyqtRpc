#pragma once
// =============================================================================
// shm_zc_adaptor.hpp — SHM 零拷贝序列化适配层
// =============================================================================
// RingBufAllocator: FlatBufferBuilder 的自定义分配器，直接写入 ring buffer
// ShmZcReader:      零拷贝读取，从 body 字节流直接映射 FlatBuffers 对象
//
// 使用示例：
//   // 写端（零拷贝）：
//   size_t contig;
//   char* buf = channel.req_write_ptr(contig);
//   RingBufAllocator alloc(buf, contig);
//   flatbuffers::FlatBufferBuilder builder(contig, &alloc, false, contig);
//   // ... build FlatBuffer (分配发生在 ring buffer 上) ...
//   builder.Finish(root);
//   channel.req_commit(builder.GetSize(), MsgType::REQ_RPC_FLAT);
//
//   // 读端（零拷贝）：
//   ShmZcReader reader(body);
//   auto* req = reader.as<lcz_rpc::fb::RpcRequest>();
//   std::string id = req->id()->str();  // 无 deserialize
// =============================================================================

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <typeinfo>
#include "./publicconfig.hpp"
#include "./log_system/lcz_log.h"
#include "flatbuffers/flatbuffers.h"

namespace lcz_rpc {

// ====== Ring Buffer 分配器 =====================================================
// 注入 FlatBufferBuilder，使序列化输出直接写入 ring buffer 的 body 区域
class RingBufAllocator : public flatbuffers::Allocator {
public:
    RingBufAllocator(char* buf, size_t capacity)
        : _buf(reinterpret_cast<uint8_t*>(buf)), _capacity(capacity) {}

    uint8_t* allocate(size_t size) override {
        if (_offset + size > _capacity) {
            LCZ_ERROR("RingBufAllocator OOM: need=%zu offset=%zu capacity=%zu",
                      size, _offset, _capacity);
            return nullptr;
        }
        uint8_t* ptr = _buf + _offset;
        _offset += size;
        return ptr;
    }

    void deallocate(uint8_t* /*p*/, size_t /*size*/) override {
        // Ring buffer 不需要释放，写入后由 commit 提交
    }

    size_t used() const { return _offset; }

private:
    uint8_t* _buf;
    size_t   _capacity;
    size_t   _offset = 0;
};

// ====== 零拷贝读取辅助 ==========================================================
// 从 read_request/read_response 拿到的 body 数据构造，
// 直接 cast 为 FlatBuffers 对象，无 deserialize 开销
class ShmZcReader {
public:
    explicit ShmZcReader(const std::string& body)
        : _data(body.data()), _size(body.size()) {}

    explicit ShmZcReader(const char* data, size_t size)
        : _data(data), _size(size) {}

    // 零拷贝映射为 FlatBuffers Table —— 不拷贝、不解析
    template<typename T>
    const T* as() const {
        if (!_data || _size == 0) return nullptr;
        auto* root = flatbuffers::GetRoot<T>(_data);
        flatbuffers::Verifier verifier(reinterpret_cast<const uint8_t*>(_data), _size);
        if (!root || !root->Verify(verifier)) {
            LCZ_ERROR("ShmZcReader::as<%s> verification failed", typeid(T).name());
            return nullptr;
        }
        return root;
    }

    const char* data() const { return _data; }
    size_t      size() const { return _size; }

    static std::string strval(const flatbuffers::String* s) {
        return s ? s->str() : std::string();
    }

private:
    const char* _data = nullptr;
    size_t      _size = 0;
};

} // namespace lcz_rpc
