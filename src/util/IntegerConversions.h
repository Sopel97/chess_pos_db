#pragma once

#include <cstdint>
#include <string_view>
#include <limits>
#include <optional>

#include "util/Assert.h"

namespace detail
{
    [[nodiscard]] bool isDigit(char c)
    {
        return c >= '0' && c <= '9';
    }

    [[nodiscard]] std::uint16_t parseUInt16(std::string_view sv)
    {
        ASSERT(sv.size() > 0);
        ASSERT(sv.size() <= 5);

        std::uint16_t v = 0;

        std::size_t idx = 0;
        switch (sv.size())
        {
        case 5:
            v += (sv[idx++] - '0') * 10000;
        case 4:
            v += (sv[idx++] - '0') * 1000;
        case 3:
            v += (sv[idx++] - '0') * 100;
        case 2:
            v += (sv[idx++] - '0') * 10;
        case 1:
            v += sv[idx] - '0';
            break;

        default:
            ASSERT(false);
        }

        return v;
    }

    [[nodiscard]] std::optional<std::uint16_t> tryParseUInt16(std::string_view sv)
    {
        if (sv.size() == 0 || sv.size() > 5) return std::nullopt;

        std::uint32_t v = 0;

        std::size_t idx = 0;
        switch (sv.size())
        {
        case 5:
            v += (sv[idx++] - '0') * 10000;
        case 4:
            v += (sv[idx++] - '0') * 1000;
        case 3:
            v += (sv[idx++] - '0') * 100;
        case 2:
            v += (sv[idx++] - '0') * 10;
        case 1:
            v += sv[idx] - '0';
            break;

        default:
            ASSERT(false);
        }

        if (v > std::numeric_limits<std::uint16_t>::max())
        {
            return std::nullopt;
        }

        return static_cast<std::uint16_t>(v);
    }
}
