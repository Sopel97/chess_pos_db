#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <optional>

struct Date 
{
    Date();

    static std::optional<Date> tryParse(std::string_view sv);

    Date(std::string_view sv);

    Date(std::uint16_t year, std::uint8_t month, std::uint8_t day);

    [[nodiscard]] friend bool operator<(const Date& lhs, const Date& rhs) noexcept
    {
        if (lhs.m_year < rhs.m_year) return true;
        else if (lhs.m_year > rhs.m_year) return false;

        if (lhs.m_month < rhs.m_month) return true;
        else if (lhs.m_month > rhs.m_month) return false;

        if (lhs.m_day < rhs.m_day) return true;
        else return false;
    }

    [[nodiscard]] std::string toString() const;

private:
    // A value of 0 signifies an unknown
    std::uint16_t m_year;
    std::uint8_t m_month;
    std::uint8_t m_day;
};
