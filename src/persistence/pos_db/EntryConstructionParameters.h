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

        // It's not possible to enforce elos to be present
        // so we cannot extract an average from this
        // without storing how many positions actually had elo.
        // But it's possible for some database formats
        // to provide this data.
        std::uint16_t whiteElo;
        std::uint16_t blackElo;

        // This is only to be used as keys of some sort
        // for filtering. (bucketing)
        std::uint16_t monthSinceYear0;

        GameLevel level;
        GameResult result;

        [[nodiscard]] std::int32_t eloDiff() const
        {
            return static_cast<std::int32_t>(whiteElo) - static_cast<std::int32_t>(blackElo);
        }
    };
}
