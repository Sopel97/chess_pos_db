#include "Date.h"

#include "detail/ParserBits.h"

#include "util/Assert.h"

#include <cstdint>
#include <string>
#include <string_view>

Date::Date() :
    m_year(0),
    m_month(0),
    m_day(0)
{
}

Date Date::min(const Date& lhs, const Date& rhs)
{
    // unknown dates are assumed to be before known

    if (lhs.m_year < rhs.m_year) return lhs;
    else if (lhs.m_year > rhs.m_year) return rhs;

    if (lhs.m_month < rhs.m_month) return lhs;
    else if (lhs.m_month > rhs.m_month) return rhs;

    if (lhs.m_day < rhs.m_day) return lhs;
    else return rhs;
}

Date Date::max(const Date& lhs, const Date& rhs)
{
    // unknown dates are assumed to be after known

    if (lhs.m_year > rhs.m_year) return lhs;
    else if (lhs.m_year < rhs.m_year) return rhs;

    if (lhs.m_month > rhs.m_month) return lhs;
    else if (lhs.m_month < rhs.m_month) return rhs;

    if (lhs.m_day > rhs.m_day) return lhs;
    else return rhs;
}

std::optional<Date> Date::tryParse(std::string_view sv, char sep)
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
        if (sv[4] != sep) return std::nullopt;
        if (sv[5] == '?' && sv[6] == '?')
        {
            month = 0;
        }
        else
        {
            if (!parser_bits::isDigit(sv[5])) return std::nullopt;
            if (!parser_bits::isDigit(sv[6])) return std::nullopt;
            month = (sv[5] - '0') * 10 + (sv[6] - '0');
        }
    }

    if (sv.size() >= 10)
    {
        if (sv[7] != sep) return std::nullopt;
        if (sv[8] == '?' && sv[9] == '?')
        {
            day = 0;
        }
        else
        {
            if (!parser_bits::isDigit(sv[8])) return std::nullopt;
            if (!parser_bits::isDigit(sv[9])) return std::nullopt;
            day = (sv[8] - '0') * 10 + (sv[9] - '0');
        }
    }

    return Date(year, month, day);
}

Date::Date(std::string_view sv)
{
    ASSERT(sv.size() >= 4);

    m_year = 0;
    m_month = 0;
    m_day = 0;

    // Lazily splits by '.'. Returns empty string views if at the end.
    auto nextPart = [sv, start = std::size_t{ 0 }]() mutable {
        std::size_t end = sv.find('.', start);
        if (end == std::string::npos)
        {
            std::string_view substr = sv.substr(start);
            start = sv.size();
            return substr;
        }
        else
        {
            std::string_view substr = sv.substr(start, end - start);
            start = end + 1; // to skip whitespace
            return substr;
        }
    };

    auto yearStr = nextPart();
    if (!yearStr.empty() && yearStr[0] != '?')
    {
        m_year = parser_bits::parseUInt16(yearStr);
    }

    auto monthStr = nextPart();
    if (!monthStr.empty() && monthStr[0] != '?')
    {
        m_month = parser_bits::parseUInt16(monthStr);
    }

    auto dayStr = nextPart();
    if (!dayStr.empty() && dayStr[0] != '?')
    {
        m_day = parser_bits::parseUInt16(dayStr);
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

[[nodiscard]] bool operator<(const Date& lhs, const Date& rhs) noexcept
{
    if (lhs.m_year < rhs.m_year) return true;
    else if (lhs.m_year > rhs.m_year) return false;

    if (lhs.m_month < rhs.m_month) return true;
    else if (lhs.m_month > rhs.m_month) return false;

    if (lhs.m_day < rhs.m_day) return true;
    else return false;
}

[[nodiscard]] bool operator==(const Date& lhs, const Date& rhs) noexcept
{
    return 
        lhs.m_year == rhs.m_year
        && lhs.m_month == rhs.m_month
        && lhs.m_day == rhs.m_day;
}

[[nodiscard]] std::string Date::toString(char sep) const
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
    buf[4] = sep;

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
    buf[7] = sep;

    return std::string(buf, 10);
}

[[nodiscard]] std::uint16_t Date::year() const
{
    return m_year;
}

[[nodiscard]] std::uint8_t Date::month() const
{
    return m_month;
}

[[nodiscard]] std::uint8_t Date::day() const
{
    return m_day;
}

[[nodiscard]] std::uint32_t Date::monthSinceYear0() const
{
    // 0 means unknown, month default to january if not present.
    auto month = m_month == 0 ? 1 : m_month;
    return m_year * month;
}

void Date::setUnknownToFirst()
{
    if (m_year == 0) m_year = 1;
    if (m_month == 0) m_month = 1;
    if (m_day == 0) m_day = 1;
}
