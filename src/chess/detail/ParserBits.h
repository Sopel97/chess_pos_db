#pragma once

#include <string>
#include <string_view>
#include <optional>

#include "util/Assert.h"
#include "chess/Chess.h"
#include "intrin/Intrinsics.h"

namespace parser_bits
{
    [[nodiscard]] constexpr bool isFile(char c)
    {
        return c >= 'a' && c <= 'h';
    }

    [[nodiscard]] constexpr bool isRank(char c)
    {
        return c >= '1' && c <= '8';
    }

    [[nodiscard]] constexpr Rank parseRank(char c)
    {
        ASSERT(isRank(c));

        return fromOrdinal<Rank>(c - '1');
    }

    [[nodiscard]] constexpr File parseFile(char c)
    {
        ASSERT(isFile(c));

        return fromOrdinal<File>(c - 'a');
    }

    [[nodiscard]] constexpr bool isSquare(const char* s)
    {
        return isRank(s[0]) && isFile(s[1]);
    }

    [[nodiscard]] constexpr Square parseSquare(const char* s)
    {
        const File file = parseFile(s[0]);
        const Rank rank = parseRank(s[1]);
        return Square(file, rank);
    }

    [[nodiscard]] constexpr std::optional<Square> tryParseSquare(std::string_view s)
    {
        if (s.size() != 2) return {};
        if (!isSquare(s.data())) return {};
        return parseSquare(s.data());
    }

    [[nodiscard]] constexpr std::optional<Square> tryParseEpSquare(std::string_view s)
    {
        if (s == std::string_view("-")) return Square::none();
        return tryParseSquare(s);
    }

    [[nodiscard]] constexpr std::optional<CastlingRights> tryParseCastlingRights(std::string_view s)
    {
        if (s == std::string_view("-")) return CastlingRights::None;

        CastlingRights rights = CastlingRights::None;

        for (auto& c : s)
        {
            CastlingRights toAdd = CastlingRights::None;
            switch (c)
            {
            case 'K':
                toAdd = CastlingRights::WhiteKingSide;
                break;
            case 'Q':
                toAdd = CastlingRights::WhiteQueenSide;
                break;
            case 'k':
                toAdd = CastlingRights::BlackKingSide;
                break;
            case 'q':
                toAdd = CastlingRights::BlackQueenSide;
                break;
            }

            // If there are duplicated castling rights specification we bail.
            // If there is an invalid character we bail.
            // (It always contains None)
            if (contains(rights, toAdd)) return {};
            else rights |= toAdd;
        }

        return rights;
    }

    [[nodiscard]] constexpr CastlingRights readCastlingRights(const char*& s)
    {
        CastlingRights rights = CastlingRights::None;

        while (*s != ' ')
        {
            switch (*s)
            {
            case 'K':
                rights |= CastlingRights::WhiteKingSide;
                break;
            case 'Q':
                rights |= CastlingRights::WhiteQueenSide;
                break;
            case 'k':
                rights |= CastlingRights::BlackKingSide;
                break;
            case 'q':
                rights |= CastlingRights::BlackQueenSide;
                break;
            }

            ++s;
        }

        return rights;
    }

    FORCEINLINE inline void appendSquareToString(Square sq, std::string& str)
    {
        str += static_cast<char>('a' + ordinal(sq.file()));
        str += static_cast<char>('1' + ordinal(sq.rank()));
    }

    FORCEINLINE inline void appendRankToString(Rank r, std::string& str)
    {
        str += static_cast<char>('1' + ordinal(r));
    }

    FORCEINLINE inline void appendFileToString(File f, std::string& str)
    {
        str += static_cast<char>('a' + ordinal(f));
    }
}