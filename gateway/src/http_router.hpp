#pragma once
// =============================================================================
// http_router.hpp — HTTP 路由表（Phase 2：路由匹配层）
// =============================================================================
// 设计思路和 ProtoRpcRouter::_handlers 完全同款（rpc_router.hpp:306）：
// unordered_map<string, HandlerFn>，key = "METHOD /path"，O(1) 精确匹配。
// 前缀匹配单独用 vector 存储，遍历 O(N)——网关路由量通常不超过二三十条，完全够用。
//
// 线程安全：所有路由注册必须在 start() 之前完成（单线程构造），运行时只读不写，
// 因此 dispatch() 不加锁——和 ProtoRpcRouter 的设计一致。
//
// 用法：
//   HttpRouter router;
//   router.addRoute("POST", "/api/echo", echo_handler);
//   router.addPrefixRoute("GET", "/api/user/", user_handler);
//   auto handler = router.dispatch("POST", "/api/user/123");  // 命中前缀
// =============================================================================
#include <unordered_map>
#include <vector>
#include <string>
#include <functional>

namespace lcz_gateway
{

    // 前向声明——实际类型在 http_server.hpp 中定义
    struct HttpReq;
    struct HttpResp;

    using RouteHandler = std::function<void(const HttpReq &, HttpResp *)>;

    // 路由条目：精确匹配或前缀匹配
    struct RouteRule
    {
        std::string method; // "GET" / "POST"
        std::string path;   // 精确路径 "/api/echo" 或前缀 "/api/user/"
        bool is_prefix;     // true 表示前缀匹配
        RouteHandler handler;
    };

    class HttpRouter
    {
    public:
        // 精确匹配：method + path 完全一致才命中。
        // 示例：addRoute("POST", "/api/echo", h) 只匹配 POST /api/echo
        void addRoute(const std::string &method, const std::string &path,
                      RouteHandler handler)
        {
            _exact[key(method, path)] = std::move(handler);
        }

        // 前缀匹配：path 以 prefix 开头即命中，先注册先匹配。
        // 示例：addPrefixRoute("GET", "/api/user/", h) 匹配 GET /api/user/42
        void addPrefixRoute(const std::string &method, const std::string &prefix,
                            RouteHandler handler)
        {
            _prefix.push_back({method, prefix, true, std::move(handler)});
        }

        // 路由查找：精确匹配优先，其次按注册顺序遍历前缀表。
        // 返回 nullptr 表示未命中，调用方应返回 404。
        RouteHandler dispatch(const std::string &method, const std::string &path)
        {
            // ① 精确匹配：O(1) 哈希查找
            auto it = _exact.find(key(method, path));
            if (it != _exact.end())
                return it->second;

            // ② 前缀匹配：顺序遍历，先注册先命中
            for (auto &r : _prefix)
            {
                if (r.method == method &&
                    path.size() >= r.path.size() &&
                    path.compare(0, r.path.size(), r.path) == 0)
                {
                    return r.handler;
                }
            }
            return nullptr;
        }

    private:
        static std::string key(const std::string &method, const std::string &path)
        {
            return method + " " + path; // key 格式 "POST /api/echo"，空格分隔，和 HTTP 请求行一致
        }

        // 精确匹配表：key="POST /api/echo" → handler，O(1) 哈希查找
        // 用 unordered_map 而非 map：路由表构造后只读不写，无需有序遍历，哈希更快
        std::unordered_map<std::string, RouteHandler> _exact;

        // 前缀匹配表：顺序存储，dispatch 时按注册顺序遍历。
        // 用 vector 而非 unordered_map 的原因：
        //   前缀无法哈希（"/api/user/" 需要匹配 "/api/user/123"），
        //   只能遍历逐一 compare。网关路由量一般不超过二三十条，O(N) 完全够用。
        //   先注册先匹配——允许 "/api/" 做兜底，"/api/user/" 优先精确命中。
        std::vector<RouteRule> _prefix;
    };

} // namespace lcz_gateway
