#pragma once

#include <cstdint>
#include <string_view>

struct Date 
{
    Date() :
        m_year(0),
        m_month(0),
        m_day(0)
    {
    }

    Date(std::uint16_t year, std::uint8_t month, std::uint8_t day) :
        m_year(year),
        m_month(month),
        m_day(day)
    {
        ASSERT(m_year <= 9999);
        ASSERT(m_month <= 12);
        ASSERT(m_day <= 31);
    }

    [[nodiscard]] friend bool operator<(const Date& lhs, const Date& rhs) noexcept
    {
        if (lhs.m_year < rhs.m_year) return true;
        else if (lhs.m_year > rhs.m_year) return false;

        if (lhs.m_month < rhs.m_month) return true;
        else if (lhs.m_month > rhs.m_month) return false;

        if (lhs.m_day < rhs.m_day) return true;
        else return false;
    }

    [[nodiscard]] std::string toString() const
    {
        char buf[10];

        if (m_year == 0)
        {
            buf[0] = buf[1] = buf[2] = buf[3] = '?';
        }
        else
        {
            auto year = m_year;
            buf[3] = static_cast<char>(year /= 10) + '0';
            buf[2] = static_cast<char>(year /= 10) + '0';
            buf[1] = static_cast<char>(year /= 10) + '0';
            buf[0] = static_cast<char>(year /= 10) + '0';
        }

        if (m_month == 0)
        {
            buf[5] = buf[6] = '?';
        }
        else
        {
            auto month = m_month;
            buf[6] = static_cast<char>(month /= 10) + '0';
            buf[5] = static_cast<char>(month /= 10) + '0';
        }
        buf[4] = '.';

        if (m_day == 0)
        {
            buf[8] = buf[9] = '?';
        }
        else
        {
            auto day = m_day;
            buf[9] = static_cast<char>(day /= 10) + '0';
            buf[8] = static_cast<char>(day /= 10) + '0';
        }
        buf[7] = '.';

        return std::string(buf, 10);
    }

private:
    // A value of 0 signifies an unknown
    std::uint16_t m_year;
    std::uint8_t m_month;
    std::uint8_t m_day;
};
