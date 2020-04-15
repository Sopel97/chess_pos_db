#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

struct Eco
{
    Eco() = default;

    static std::optional<Eco> tryParse(std::string_view sv);

    Eco(char category, std::uint8_t index);

    Eco(std::string_view sv);

    [[nodiscard]] std::string toString() const;

    [[nodiscard]] char category() const;
    [[nodiscard]] std::uint8_t index() const;

private:
    char m_category;
    std::uint8_t m_index;
};
