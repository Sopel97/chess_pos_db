#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

struct Date 
{
    Date();

    static Date min(const Date& lhs, const Date& rhs);
    static Date max(const Date& lhs, const Date& rhs);

    static std::optional<Date> tryParse(std::string_view sv, char sep = '.');

    Date(std::string_view sv);

    Date(std::uint16_t year, std::uint8_t month, std::uint8_t day);

    friend bool operator<(const Date& lhs, const Date& rhs) noexcept;
    friend bool operator==(const Date& lhs, const Date& rhs) noexcept;

    [[nodiscard]] std::string toString(char sep = '.') const;

    [[nodiscard]] std::uint16_t year() const;
    [[nodiscard]] std::uint8_t month() const;
    [[nodiscard]] std::uint8_t day() const;

    [[nodiscard]] std::uint32_t monthSinceYear0() const;

    void setUnknownToFirst();

private:
    // A value of 0 signifies an unknown
    std::uint16_t m_year;
    std::uint8_t m_month;
    std::uint8_t m_day;
};
