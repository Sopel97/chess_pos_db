#pragma once

#include "EnumMap.h"

#include <cstdint>

enum struct GameResult : std::uint8_t
{
    WhiteWin,
    BlackWin,
    Draw
};

template <>
struct EnumTraits<GameResult>
{
    using IdType = int;
    using EnumType = GameResult;

    static constexpr int cardinality = 3;
    static constexpr bool isNaturalIndex = true;

    static constexpr std::array<EnumType, cardinality> values{
        GameResult::WhiteWin,
        GameResult::BlackWin,
        GameResult::Draw
    };

    [[nodiscard]] static constexpr IdType ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        return static_cast<EnumType>(id);
    }
};

enum struct GameLevel : std::uint8_t
{
    Human,
    Engine,
    Server
};

template <>
struct EnumTraits<GameLevel>
{
    using IdType = int;
    using EnumType = GameLevel;

    static constexpr int cardinality = 3;
    static constexpr bool isNaturalIndex = true;

    static constexpr std::array<EnumType, cardinality> values{
        GameLevel::Human,
        GameLevel::Engine,
        GameLevel::Server
    };

    [[nodiscard]] static constexpr IdType ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        return static_cast<EnumType>(id);
    }
};
