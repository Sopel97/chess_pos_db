#pragma once

#include "Chess.h"
#include "Position.h"

#include <string>
#include <string_view>
#include <optional>

namespace uci
{
    [[nodiscard]] std::string moveToUci(const Position& pos, const Move& move);
    [[nodiscard]] Move uciToMove(const Position& pos, std::string_view sv);

    [[nodiscard]] std::optional<Move> tryUciToMove(const Position& pos, std::string_view sv);
}
