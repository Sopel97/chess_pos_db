#include "MemoryAmount.h"

#include <cstdint>
#include <string_view>
#include <string>
#include <map>

#include "json/json.hpp"

using namespace std::literals;

MemoryAmount::MemoryAmount(const std::string& s)
{
    const char* begin = s.c_str();
    char* end;
    const char* actualEnd = begin + s.size();
    m_bytes = std::strtoll(begin, &end, 10);
    if (end != actualEnd)
    {
        while (*end == ' ') ++end;
        std::string_view unit(end, actualEnd - end);
        m_bytes *= m_units.at(unit);
    }
}

void to_json(nlohmann::json& j, const MemoryAmount& v)
{
    j = nlohmann::json{ v.m_bytes };
}

void from_json(const nlohmann::json& j, MemoryAmount& v)
{
    if (j.is_string())
    {
        auto str = j.get<std::string>();
        v = MemoryAmount(str);
    }
    else
    {
        j.get_to(v.m_bytes);
    }
}

const std::map<std::string_view, std::size_t> MemoryAmount::m_units = {
    { "B"sv, 1 },
    { "kB"sv, 1000 },
    { "MB"sv, 1000 * 1000 },
    { "GB"sv, 1000 * 1000 * 1000 },
    { "TB"sv, 1000ull * 1000ull * 1000ull * 1000ull },
    { "KiB"sv, 1024 },
    { "MiB"sv, 1024 * 1024 },
    { "GiB"sv, 1024 * 1024 * 1024 },
    { "TiB"sv, 1024ull * 1024ull * 1024ull * 1024ull }
};
