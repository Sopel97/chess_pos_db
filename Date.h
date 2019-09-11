#pragma once

#include <cstdint>
#include <string_view>

struct Date 
{
    Date(std::uint16_t daysSinceEpoch) :
        m_daysSinceEpoch(daysSinceEpoch)
    {
    }

    Date(std::string_view sv) :
        m_daysSinceEpoch(0)
    {
        // TODO:
    }

private:
    // epoch is 1900-01-01
    std::uint16_t m_daysSinceEpoch;
};
