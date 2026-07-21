#pragma once
// =============================================================================
// http_server.hpp — HTTP/1.1 服务器（Phase 1：HTTP 接入层）
// =============================================================================
// 底层网络：muduo::net::TcpServer（accept / epoll / IO 线程池 / Buffer / 连接管理），
// 不手写 socket/epoll，不管理线程生命周期。和 RpcServer 用的是完全相同的 muduo 基础设施。
//
// 上层解析：手写 HTTP 请求行 + 头部 + Content-Length body 的拆解逻辑。
// 不用 muduo::net::HttpServer 的原因：它的 POST body 处理是 FIXME（HttpContext.cc
// 的 kExpectBody 状态体为空），无法读取 JSON 请求体，对 API 网关不可用。
//
// 线程模型：
//   外部传入一个 muduo::net::EventLoop&，HttpServer 不持有 loop 的所有权。
//   setThreadNum(N) 控制 muduo IO 线程池大小，每个连接收到的数据由 muduo 回调
//   drive，解析全程在 IO 线程栈上完成——不创建额外线程、不跨线程传递请求状态、
//   不做异步派发。响应组装使用 muduo::net::Buffer，复用 muduo 的 I/O 路径。
//
// 用法（和 RpcServer 同款接口风格）：
//   muduo::net::EventLoop loop;
//   lcz_gateway::HttpServer srv(&loop, 8080, 4);
//   srv.setCallback([](const HttpReq& req, HttpResp* resp) { ... });
//   srv.start();
//   loop.loop();
// =============================================================================
#include <muduo/net/TcpServer.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/Buffer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <string>
#include <map>
#include <cstring>
#include <functional>

namespace lcz_gateway
{

    // HTTP 请求——只存网关用到的字段，不实现完整 RFC 7230
    struct HttpReq
    {
        std::string method; // "GET" / "POST"
        std::string path;   // "/api/echo"
        std::map<std::string, std::string> headers;
        std::string body; // 按 Content-Length 读取的请求体
    };

    // HTTP 响应——回调内 fill，由 HttpServer 组装成字节流发送
    struct HttpResp
    {
        int status = 200;
        std::string status_msg; // 空则按 status 自动填 "OK"/"Not Found"/...
        std::map<std::string, std::string> headers;
        std::string body;

        void setContentType(const std::string &t) { headers["Content-Type"] = t; }
        void setBody(const std::string &b) { body = b; }
    };

    class HttpServer
    {
    public:
        using Callback = std::function<void(const HttpReq &, HttpResp *)>;

        // loop: 外部 EventLoop，所有权在调用方（通常在主线程栈上）
        // port: 监听端口
        // thread_num: muduo IO 线程数，默认 4，和 RpcServer 一致
        HttpServer(muduo::net::EventLoop *loop, uint16_t port, int thread_num = 4)
            : _server(loop,
                      muduo::net::InetAddress(static_cast<uint16_t>(port)),
                      "GatewayHttp",
                      muduo::net::TcpServer::kReusePort)
        {
            _server.setThreadNum(thread_num);
            // 消息回调绑定到本类的 onMessage，由 muduo IO 线程驱动
            _server.setMessageCallback(
                [this](const muduo::net::TcpConnectionPtr &conn,
                       muduo::net::Buffer *buf, muduo::Timestamp)
                {
                    onMessage(conn, buf);
                });
        }

        void setCallback(const Callback &cb) { _cb = cb; }
        void setThreadNum(int n) { _server.setThreadNum(n); }
        void start() { _server.start(); }

    private:
        // ---- HTTP 请求解析 ----
        // 在单个 onMessage 回调中完成"请求行→头部→body"的同步解析。
        // 粘包/半包由 muduo::net::Buffer 和返回语义处理：数据不够时直接 return，
        // muduo 下次触发 onMessage 时 buf 中已追加新到达的字节，继续从上次中断处解析。
        // 单个请求最大 body 上限 10 MB，防止恶意大包撑爆内存。
        void onMessage(const muduo::net::TcpConnectionPtr &conn,
                       muduo::net::Buffer *buf)
        {
            // ---- 请求行：METHOD SP PATH SP HTTP/1.x CRLF ----
            const char *crlf = buf->findCRLF();
            if (!crlf)
                return; // 行不完整，等下次 onMessage

            size_t line_len = static_cast<size_t>(crlf - buf->peek());
            std::string request_line(buf->peek(), line_len);
            buf->retrieve(line_len + 2); // 消费请求行 + CRLF

            HttpReq req;
            size_t sp1 = request_line.find(' ');
            size_t sp2 = request_line.find(' ', sp1 + 1);
            if (sp1 == std::string::npos || sp2 == std::string::npos)
            {
                conn->shutdown(); // 格式错误，直接关连接，不回复（防御恶意扫描）
                return;
            }
            req.method = request_line.substr(0, sp1);
            req.path = request_line.substr(sp1 + 1, sp2 - sp1 - 1);

            // ---- 头部行：Key: Value CRLF ... 空行 CRLF 表示头结束 ----
            int content_length = 0;
            for (;;)
            {
                const char *line_crlf = buf->findCRLF();
                if (!line_crlf)
                    return; // 不完整

                size_t len = static_cast<size_t>(line_crlf - buf->peek());
                if (len == 0)
                {
                    buf->retrieve(2); // 空行 = 头结束标志
                    break;
                }

                std::string line(buf->peek(), len);
                buf->retrieve(len + 2);

                size_t colon = line.find(':');
                if (colon == std::string::npos)
                    continue; // 非标准行，跳过

                std::string key = line.substr(0, colon);
                std::string val = line.substr(colon + 1);
                // 去掉冒号后的前导空格
                size_t nsp = 0;
                while (nsp < val.size() && val[nsp] == ' ')
                    ++nsp;
                if (nsp > 0)
                    val = val.substr(nsp);

                req.headers[key] = val;
                // 合并不区分大小写：Content-Length / content-length 均命中
                if (strcasecmp(key.c_str(), "Content-Length") == 0)
                    content_length = std::stoi(val);
            }

            // ---- Body：按 Content-Length 精确读取 ----
            // Content-Length 是唯一支持的 body 定界方式——不实现 chunked
            // (Transfer-Encoding: chunked) 和 Connection: keep-alive，
            // 理由：API 网关的典型上游是 curl / axios / Postman / 微服务，
            // 这些客户端发 JSON 时都会带 Content-Length，chunked 仅在流式上传场景出现，
            // 网关暂不覆盖。
            if (content_length > 0)
            {
                if (content_length > 10 * 1024 * 1024)
                { // 10 MB 硬上限
                    conn->shutdown();
                    return;
                }
                // muduo Buffer 可能分包到达——readableBytes() < content_length
                // 时直接 return，muduo 下次 onMessage 时 buf 里已有新字节
                if (buf->readableBytes() < static_cast<size_t>(content_length))
                    return; // 数据不完整，等下次
                req.body.assign(buf->peek(), static_cast<size_t>(content_length));
                buf->retrieve(content_length);
            }

            // ---- 调用户回调，填充响应 ----
            HttpResp resp;
            resp.setContentType("application/json");
            if (_cb)
                _cb(req, &resp);

            // ---- 组装 HTTP 响应并发送 ----
            sendResponse(conn, resp);
        }

        // 使用 muduo::net::Buffer 组装响应，复用 muduo 的 I/O 写路径。
        // 短连接模式：发完立刻 shutdown。
        // —— API 网关场景下，后端 RPC 调用已经占了连接（RpcClient 连接池），
        //    网关 HTTP 层再做 keep-alive 收益极小（同一客户端短时间内的后续请求
        //    仍需重新走鉴权/限流/路由，无法复用会话状态），徒增 TIME_WAIT 管理复杂度。
        void sendResponse(const muduo::net::TcpConnectionPtr &conn,
                          const HttpResp &resp)
        {
            muduo::net::Buffer buf;

            buf.append("HTTP/1.1 " + std::to_string(resp.status) + " " +
                       (resp.status_msg.empty() ? statusMsg(resp.status)
                                                : resp.status_msg) +
                       "\r\n");

            for (const auto &[k, v] : resp.headers)
                buf.append(k + ": " + v + "\r\n");

            buf.append("Content-Length: " + std::to_string(resp.body.size()) + "\r\n");
            buf.append("Connection: close\r\n");
            buf.append("\r\n");
            buf.append(resp.body);

            conn->send(&buf);
            conn->shutdown();
        }

        static const char *statusMsg(int code)
        {
            switch (code)
            {
            case 200:
                return "OK";
            case 400:
                return "Bad Request";
            case 404:
                return "Not Found";
            case 429:
                return "Too Many Requests";
            case 500:
                return "Internal Server Error";
            case 502:
                return "Bad Gateway";
            case 503:
                return "Service Unavailable";
            default:
                return "Unknown";
            }
        }

        muduo::net::TcpServer _server;
        Callback _cb;
    };

} // namespace lcz_gateway
