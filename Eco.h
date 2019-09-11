#pragma once

#include "Assert.h"

#include <cstdint>
#include <string_view>

struct Eco
{
    Eco(char category, std::uint8_t index) :
        m_category(category),
        m_index(index)
    {
        ASSERT(category >= 'A' && category <= 'E');
        ASSERT(index <= 99);
    }

    Eco(std::string_view sv) :
        Eco(sv[0], (sv[1] - '0') * 10 + (sv[2] - '0'))
    {
    }

private:
    char m_category;
    std::uint8_t m_index;
};
