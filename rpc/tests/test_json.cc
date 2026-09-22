// detail.hpp JSON 工具 + uuid 生成单测
#include <gtest/gtest.h>
#include <string>
#include "src/general/detail.hpp"

// 序列化 → 反序列化 roundtrip，字段类型与值保持一致
TEST(JsonTest, SerializeRoundtrip)
{
    Json::Value obj(Json::objectValue);
    obj["name"] = "lyq";
    obj["n"] = 42;
    obj["ok"] = true;
    obj["arr"] = Json::Value(Json::arrayValue);
    obj["arr"].append(1);
    obj["arr"].append("two");

    std::string out;
    ASSERT_TRUE(JSON::serialize(obj, out));
    ASSERT_FALSE(out.empty());

    Json::Value back;
    ASSERT_TRUE(JSON::deserialize(out, back));
    EXPECT_EQ(back["name"].asString(), "lyq");
    EXPECT_EQ(back["n"].asInt(), 42);
    EXPECT_EQ(back["ok"].asBool(), true);
    EXPECT_EQ(back["arr"][0].asInt(), 1);
    EXPECT_EQ(back["arr"][1].asString(), "two");
}

// 畸形 / 空输入反序列化失败且不抛异常（try-catch 兜底）
TEST(JsonTest, DeserializeMalformedReturnsFalse)
{
    Json::Value v;
    EXPECT_FALSE(JSON::deserialize("{not valid json", v));
    EXPECT_FALSE(JSON::deserialize("", v));
    EXPECT_FALSE(JSON::deserialize("{\"a\": }", v));
}

// uuid 长度 36，短横线位于 8/13/18/23，其余为小写十六进制
TEST(JsonTest, UuidFormat)
{
    std::string u = uuid();
    EXPECT_EQ(u.size(), 36u);
    EXPECT_EQ(u[8], '-');
    EXPECT_EQ(u[13], '-');
    EXPECT_EQ(u[18], '-');
    EXPECT_EQ(u[23], '-');
    for (char c : u)
    {
        if (c == '-') continue;
        EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
    }
}

// uuid 两次调用不同（尾部自增序号保证唯一）
TEST(JsonTest, UuidUniqueAcrossCalls)
{
    EXPECT_NE(uuid(), uuid());
}
