#pragma once

#include "chess/Chess.h"
#include "chess/GameClassification.h"
#include "chess/Position.h"

namespace persistence
{
    struct EntryConstructionParameters
    {
        PositionWithZobrist position;
        ReverseMove reverseMove;

        std::uint64_t gameIndexOrOffset;

        std::uint16_t whiteElo;
        std::uint16_t blackElo;
        std::uint16_t monthSinceYear0;

        GameLevel level;
        GameResult result;

        [[nodiscard]] std::int32_t eloDiff() const
        {
            return static_cast<std::int32_t>(whiteElo) - static_cast<std::int32_t>(blackElo);
        }
    };
}
