#pragma once

#include "EnumMap.h"

#include <cstdint>
#include <string_view>

enum struct GameResult : std::uint8_t
{
    WhiteWin,
    BlackWin,
    Draw
};

struct GameResultPgnFormat {};
struct GameResultWordFormat {};

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

    [[nodiscard]] static std::string_view toString(GameResultWordFormat, GameResult result)
    {
        using namespace std::literals;

        switch (result)
        {
        case GameResult::WhiteWin:
            return "win"sv;
        case GameResult::BlackWin:
            return "loss"sv;
        case GameResult::Draw:
            return "draw"sv;
        }

        return ""sv;
    }

    [[nodiscard]] static std::string_view toString(GameResultPgnFormat, GameResult result)
    {
        using namespace std::literals;

        switch (result)
        {
        case GameResult::WhiteWin:
            return "1-0"sv;
        case GameResult::BlackWin:
            return "0-1"sv;
        case GameResult::Draw:
            return "1/2-1/2"sv;
        }

        return ""sv;
    }

    [[nodiscard]] static GameResult fromString(GameResultWordFormat, std::string_view sv)
    {
        using namespace std::literals;

        if (sv == "win"sv) return GameResult::WhiteWin;
        if (sv == "loss"sv) return GameResult::BlackWin;
        if (sv == "draw"sv) return GameResult::Draw;

        return GameResult::Draw;
    }

    [[nodiscard]] static GameResult fromString(GameResultPgnFormat, std::string_view sv)
    {
        using namespace std::literals;

        if (sv == "1-0"sv) return GameResult::WhiteWin;
        if (sv == "0-1"sv) return GameResult::BlackWin;
        if (sv == "1/2-1/2"sv) return GameResult::Draw;

        return GameResult::Draw;
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

    [[nodiscard]] static std::string_view toString(GameLevel level)
    {
        using namespace std::literals;

        switch (level)
        {
        case GameLevel::Human:
            return "human"sv;
        case GameLevel::Engine:
            return "engine"sv;
        case GameLevel::Server:
            return "server"sv;
        }

        return ""sv;
    }

    [[nodiscard]] static GameLevel fromString(std::string_view sv)
    {
        using namespace std::literals;

        if (sv == "human"sv) return GameLevel::Human;
        if (sv == "engine"sv) return GameLevel::Engine;
        if (sv == "server"sv) return GameLevel::Server;

        return GameLevel::Human;
    }
};
