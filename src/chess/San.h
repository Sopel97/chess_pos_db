#pragma once

#include <string_view>
#include <optional>

#include "chess/detail/ParserBits.h"

#include "util/Assert.h"
#include "Chess.h"
#include "Position.h"

namespace san
{
    using namespace std::literals;

    enum struct SanSpec : std::uint8_t
    {
        None = 0x0,
        Capture = 0x1,
        Check = 0x2,

        Compact = 0x8,

        // not yet supported
        // Mate = 0x4

        Full = Capture | Check | Compact
    };

    [[nodiscard]] constexpr SanSpec operator|(SanSpec lhs, SanSpec rhs)
    {
        return static_cast<SanSpec>(static_cast<std::uint8_t>(lhs) | static_cast<std::uint8_t>(rhs));
    }

    [[nodiscard]] constexpr SanSpec operator&(SanSpec lhs, SanSpec rhs)
    {
        return static_cast<SanSpec>(static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(rhs));
    }

    // checks whether lhs contains rhs
    [[nodiscard]] constexpr bool contains(SanSpec lhs, SanSpec rhs)
    {
        return (lhs & rhs) == rhs;
    }

    template <SanSpec SpecV>
    [[nodiscard]] std::string moveToSan(const Position& pos, Move move);

    [[nodiscard]] bool isValidSanMoveStart(char c);

    [[nodiscard]] Move sanToMove(const Position& pos, std::string_view san);

    [[nodiscard]] std::optional<Move> trySanToMove(const Position& pos, std::string_view san);
}
