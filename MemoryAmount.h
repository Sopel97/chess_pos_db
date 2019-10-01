#pragma once

#include <cstdint>
#include <string_view>
#include <string>
#include <map>

#include "lib/json/json.hpp"

namespace
{
    using namespace std::literals;

    struct MemoryAmount
    {
        constexpr MemoryAmount() :
            m_bytes{}
        {
        }

        MemoryAmount(const std::string& s)
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

        constexpr static MemoryAmount bytes(std::size_t amount)
        {
            return MemoryAmount(amount);
        }

        constexpr static MemoryAmount kilobytes(std::size_t amount)
        {
            return MemoryAmount(amount * 1000);
        }

        constexpr static MemoryAmount megabytes(std::size_t amount)
        {
            return MemoryAmount(amount * 1000 * 1000);
        }

        constexpr static MemoryAmount gigabytes(std::size_t amount)
        {
            return MemoryAmount(amount * 1000 * 1000 * 1000);
        }

        constexpr static MemoryAmount terabytes(std::size_t amount)
        {
            return MemoryAmount(amount * 1000 * 1000 * 1000 * 1000);
        }

        constexpr static MemoryAmount kibibytes(std::size_t amount)
        {
            return MemoryAmount(amount * 1024);
        }

        constexpr static MemoryAmount mebibytes(std::size_t amount)
        {
            return MemoryAmount(amount * 1024 * 1024);
        }

        constexpr static MemoryAmount gibibytes(std::size_t amount)
        {
            return MemoryAmount(amount * 1024 * 1024 * 1024);
        }

        constexpr static MemoryAmount tebibytes(std::size_t amount)
        {
            return MemoryAmount(amount * 1024 * 1024 * 1024 * 1024);
        }

        [[nodiscard]] constexpr operator std::size_t() const
        {
            return m_bytes;
        }

        friend void to_json(nlohmann::json& j, const MemoryAmount& v)
        {
            j = nlohmann::json{ v.m_bytes };
        }

        friend void from_json(const nlohmann::json& j, MemoryAmount& v)
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

    private:
        static inline const std::map<std::string_view, std::size_t> m_units = {
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

        constexpr MemoryAmount(std::size_t volume) :
            m_bytes(volume)
        {
        }

    private:
        std::size_t m_bytes;
    };
}
