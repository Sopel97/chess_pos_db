#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <optional>

struct Eco
{
    Eco() = default;

    static std::optional<Eco> tryParse(std::string_view sv);

    Eco(char category, std::uint8_t index);

    Eco(std::string_view sv);

    [[nodiscard]] std::string toString() const;

private:
    char m_category;
    std::uint8_t m_index;
};
