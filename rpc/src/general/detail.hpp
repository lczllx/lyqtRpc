#pragma once
#include<cstdio>
#include<time.h>

#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <memory>
#include <jsoncpp/json/json.h>
#include <chrono>
#include <random>
#include <atomic>
#include <iomanip>
#include "publicconfig.hpp"
#include "log_system/lcz_log.h"

// 旧日志宏已移除，统一使用 log_system/lcz_log.h 中的 LCZ_DEBUG/LCZ_INFO/LCZ_WARN/LCZ_ERROR

// JSON 工具类：Jsoncpp 的薄封装，用于 Json 与字符串的序列化/反序列化
class JSON{
    public:
    //json对象->字符串 data-要序列化的json对象 output-序列化后的字符串
    static bool serialize(const Json::Value &data,std::string &output)
    {
        Json::StreamWriterBuilder swb;
        std::unique_ptr<Json::StreamWriter> sw(swb.newStreamWriter());
        std::stringstream ss;
        int ret=sw->write(data,&ss);
        if (ret != 0) 
        {
            LCZ_ERROR("Serialize failed!");
            return false;
        }
        output=ss.str();
        return true;

    }
    //字符串->json对象 data-反序列化后的json对象 input需要反序列化的字符串
    static bool deserialize(std::string_view input, Json::Value &data)
    {
        // jsoncpp 对畸形输入（内嵌 NUL、类型错位等）可能抛 Json::LogicError/RuntimeError，
        // 而非仅返回 false，故必须 try-catch 兜底，否则异常会穿透到协议层导致进程 terminate。
        try
        {
            Json::CharReaderBuilder crb;
            std::string errs;
            std::unique_ptr<Json::CharReader> cr(crb.newCharReader());
            bool ret=cr->parse(input.data(),input.data()+input.size(),&data,&errs);
            if (!ret)
            {
                LCZ_ERROR("DeSerialize failed!,%s",errs.c_str());
                return false;
            }
            return true;
        }
        catch (const std::exception &e)
        {
            LCZ_ERROR("DeSerialize threw: %s", e.what());
            return false;
        }

    }
};
// 简单的 uuid 生成工具：前半随机数，后半自增序号
inline std::string uuid() {
    std::stringstream ss;
    //1. 构造⼀个机器随机数对象
    std::random_device rd;
    //2. 以机器随机数为种⼦构造伪随机数对象
    std::mt19937 generator (rd());
    //3. 构造限定数据范围的对象
    std::uniform_int_distribution<int> distribution(0, 255);
    //4. ⽣成8个随机数，按照特定格式组织成为16进制数字字符的字符串
    for (int i = 0; i < 8; i++) {
        if (i == 4 || i == 6) ss << "-";
        ss << std::setw(2) << std::setfill('0') <<std::hex <<
        distribution(generator);
    }
    ss << "-";
    //5. 定义⼀个8字节序号，逐字节组织成为16进制数字字符的字符串
    static std::atomic<size_t> seq(1); // 00 00 00 00 00 00 00 01
    size_t cur = seq.fetch_add(1);
    for (int i = 7; i >= 0; i--) {
        if (i == 5) ss << "-";
        ss << std::setw(2) << std::setfill('0') << std::hex << ((cur >> (i*8))&0xFF);
    }
    return ss.str();
}
