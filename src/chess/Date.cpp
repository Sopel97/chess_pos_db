#include "Date.h"

#include <cstdint>
#include <string>
#include <string_view>

#include "detail/ParserBits.h"

Date::Date() :
    m_year(0),
    m_month(0),
    m_day(0)
{
}

std::optional<Date> Date::tryParse(std::string_view sv)
{
    std::uint16_t year = 0;
    std::uint8_t month = 0;
    std::uint8_t day = 0;

    auto yearOpt = parser_bits::tryParseUInt16(sv.substr(0, 4));
    if (yearOpt.has_value())
    {
        year = *yearOpt;
    }
    else
    {
        return std::nullopt;
    }

    if (sv.size() >= 7)
    {
        if (sv[4] != '.') return std::nullopt;
        if (!parser_bits::isDigit(sv[5])) return std::nullopt;
        if (!parser_bits::isDigit(sv[6])) return std::nullopt;
        month = (sv[5] - '0') * 10 + (sv[6] - '0');
    }
    else if (sv.size() != 4) return std::nullopt;

    if (sv.size() >= 10)
    {
        if (sv[7] != '.') return std::nullopt;
        if (!parser_bits::isDigit(sv[8])) return std::nullopt;
        if (!parser_bits::isDigit(sv[9])) return std::nullopt;
        month = (sv[5] - '0') * 10 + (sv[6] - '0');
    }
    else if (sv.size() != 7) return std::nullopt;

    return Date(year, month, day);
}

Date::Date(std::string_view sv)
{
    ASSERT(sv.size() >= 4);

    m_year = 0;
    m_month = 0;
    m_day = 0;

    if (sv.substr(0, 4) != std::string_view("????"))
    {
        m_year = parser_bits::parseUInt16(sv.substr(0, 4));
    }

    if (sv.size() >= 7 && sv.substr(5, 2) != std::string_view("??"))
    {
        m_month = (sv[5] - '0') * 10 + (sv[6] - '0');
    }

    if (sv.size() >= 10 && sv.substr(8, 2) != std::string_view("??"))
    {
        m_day = (sv[8] - '0') * 10 + (sv[9] - '0');
    }
}

Date::Date(std::uint16_t year, std::uint8_t month, std::uint8_t day) :
    m_year(year),
    m_month(month),
    m_day(day)
{
    ASSERT(m_year <= 9999);
    ASSERT(m_month <= 12);
    ASSERT(m_day <= 31);
}

[[nodiscard]] std::string Date::toString() const
{
    char buf[10];

    if (m_year == 0)
    {
        buf[0] = buf[1] = buf[2] = buf[3] = '?';
    }
    else
    {
        auto year = m_year;
        buf[3] = static_cast<char>(year % 10) + '0'; year /= 10;
        buf[2] = static_cast<char>(year % 10) + '0'; year /= 10;
        buf[1] = static_cast<char>(year % 10) + '0'; year /= 10;
        buf[0] = static_cast<char>(year % 10) + '0';
    }

    if (m_month == 0)
    {
        buf[5] = buf[6] = '?';
    }
    else
    {
        auto month = m_month;
        buf[6] = static_cast<char>(month % 10) + '0'; month /= 10;
        buf[5] = static_cast<char>(month % 10) + '0';
    }
    buf[4] = '.';

    if (m_day == 0)
    {
        buf[8] = buf[9] = '?';
    }
    else
    {
        auto day = m_day;
        buf[9] = static_cast<char>(day % 10) + '0'; day /= 10;
        buf[8] = static_cast<char>(day % 10) + '0';
    }
    buf[7] = '.';

    return std::string(buf, 10);
}
