#include "Eco.h"

#include "util/Assert.h"
#include "detail/ParserBits.h"

#include <cstdint>
#include <string>
#include <string_view>

std::optional<Eco> Eco::tryParse(std::string_view sv)
{
    if (sv.size() != 3) return std::nullopt;
    if (sv[0] < 'A' || sv[0] > 'E') return std::nullopt;
    if (!parser_bits::isDigit(sv[1])) return std::nullopt;
    if (!parser_bits::isDigit(sv[2])) return std::nullopt;

    return Eco(sv);
}

Eco::Eco(char category, std::uint8_t index) :
    m_category(category),
    m_index(index)
{
    ASSERT(category >= 'A' && category <= 'E');
    ASSERT(index <= 99);
}

Eco::Eco(std::string_view sv) :
    Eco(sv[0], (sv[1] - '0') * 10 + (sv[2] - '0'))
{
}

[[nodiscard]] std::string Eco::toString() const
{
    auto s = std::string(1, m_category);
    if (m_index < 10)
    {
        s += '0';
    }
    s += std::to_string(m_index);
    return s;
}
