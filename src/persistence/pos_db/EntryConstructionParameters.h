#pragma once

#include "chess/Chess.h"
#include "chess/GameClassification.h"
#include "chess/Position.h"

namespace pos_db
{
    struct EntryConstructionParameters
    {
        PositionWithZobrist position;
        ReverseMove reverseMove;

        std::uint64_t firstGameIndexOrOffset;
        std::uint64_t lastGameIndexOrOffset;

        std::uint16_t whiteElo;
        std::uint16_t blackElo;
        std::uint16_t monthSinceYear0;

        GameLevel level;
        GameResult result;
    };
}
