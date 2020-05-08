#pragma once

#include "Bitboard.h"
#include "Chess.h"

#include "enum/Enum.h"
#include "enum/EnumArray.h"

struct CastlingTraits
{
    static constexpr EnumArray2<Color, CastleType, Square> rookCastleDestinations = { { {{ f1, d1 }}, {{ f8, d8 }} } };
    static constexpr EnumArray2<Color, CastleType, Square> kingCastleDestinations = { { {{ g1, c1 }}, {{ g8, c8 }} } };

    static constexpr EnumArray2<Color, CastleType, Square> rookCastleSources = { { {{ h1, a1 }}, {{ h8, a8 }} } };

    static constexpr EnumArray<Color, Square> kingStartSquare = { { e1, e8 } };

    static constexpr EnumArray2<Color, CastleType, Bitboard> castlingPaths = {
        {
            {{ Bitboard::square(f1) | g1, Bitboard::square(b1) | c1 | d1 }},
            {{ Bitboard::square(f8) | g8, Bitboard::square(b8) | c8 | d8 }}
        }
    };

    static constexpr EnumArray2<Color, CastleType, Square> squarePassedByKing = {
        {
            {{ f1, d1 }},
            {{ f8, d8 }}
        }
    };

    static constexpr EnumArray2<Color, CastleType, CastlingRights> castlingRights = {
        {
            {{ CastlingRights::WhiteKingSide, CastlingRights::WhiteQueenSide }},
            {{ CastlingRights::BlackKingSide, CastlingRights::BlackQueenSide }}
        }
    };

    // Move has to be a legal castling move.
    static constexpr CastleType moveCastlingType(const Move& move)
    {
        return (move.to.file() == fileH) ? CastleType::Short : CastleType::Long;
    }

    // Move must be a legal castling move.
    static constexpr CastlingRights moveCastlingRight(Move move)
    {
        if (move.to == h1) return CastlingRights::WhiteKingSide;
        if (move.to == a1) return CastlingRights::WhiteQueenSide;
        if (move.to == h8) return CastlingRights::WhiteKingSide;
        if (move.to == a8) return CastlingRights::WhiteQueenSide;
        return CastlingRights::None;
    }
};
