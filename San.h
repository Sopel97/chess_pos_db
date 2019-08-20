#pragma once

#include "Chess.h"
#include "Position.h"

namespace detail::san
{
    inline Rank parseRank(const char s)
    {
        return fromOrdinal<Rank>(s - '1');
    }

    inline File parseFile(const char s)
    {
        return fromOrdinal<File>(s - 'a');
    }

    inline Square parseSquare(const char* s)
    {
        const File file = parseFile(s[0]);
        const Rank rank = parseRank(s[1]);
        return Square(file, rank);
    }

    inline bool contains(const char* s, char c)
    {
        while (*s)
        {
            if (*s == c) return true;
            ++s;
        }

        return false;
    }

    inline bool isSanCapture(const char* san)
    {
        return contains(san, 'x');
    }

    inline void removeSanCapture(char* san)
    {
        for (;;)
        {
            if (*san == 'x') break;
            if (*san == '\0') return;
            ++san;
        }

        // x__
        // ^san
        while (*san) // the original '\0' after we copy it
        {
            *san = *(san + 1);
            ++san;
        }
    }

    inline void removeSanDecorations(char* san)
    {
        // removes capture designation
        // removes instances of the following characters:
        // # - mate
        // + - check
        // !
        // ?
        // N
        //
        // removal starts from the end of the san
        // and stops when any character not on the list above is found

        char* cur = san;
        while (*cur) ++cur;

        for (;;)
        {
            --cur;
            if (cur == san) break;

            switch (*cur)
            {
            case '#':
            case '+':
            case '!':
            case '?':
            case 'N':
                *cur = '\0';
                continue;
            }

            break;
        }

        removeSanCapture(san);
    }

    inline int len(const char* san)
    {
        // optimized for short strings
        const char* cur = san;
        while (*cur) ++cur;
        return static_cast<int>(cur - san);
    }

    inline Move sanToMove_Pawn(const Position& pos, const char* san)
    {
        // since we remove capture information it's either
        // 012345 idx
        // a1
        // aa1
        // a1=_
        // aa1=_

        // TODO: handle promotions

        if (san[2] == '\0')
        {
            // a1
            const Square toSq = parseSquare(san);
            Square fromSq = Square::none();

            if (pos.sideToMove() == Color::White)
            {
                if (pos.pieceAt(toSq + Offset{ 0, -1 }).type() == PieceType::Pawn)
                {
                    fromSq = toSq + Offset{ 0, -1 };
                }
                else if (pos.pieceAt(toSq + Offset{ 0, -2 }).type() == PieceType::Pawn)
                {
                    fromSq = toSq + Offset{ 0, -2 };
                }
            }
            else
            {
                if (pos.pieceAt(toSq + Offset{ 0, 1 }).type() == PieceType::Pawn)
                {
                    fromSq = toSq + Offset{ 0, 1 };
                }
                else if (pos.pieceAt(toSq + Offset{ 0, 2 }).type() == PieceType::Pawn)
                {
                    fromSq = toSq + Offset{ 0, 2 };
                }
            }

            return Move{ fromSq, toSq };
        }
        else if (san[3] == '\0')
        {
            // aa1

            const File fromFile = parseFile(san[0]);
            const File toFile = parseFile(san[1]);
            const Rank toRank = parseRank(san[2]);
            const Square toSq(toFile, toRank);

            if (pos.sideToMove() == Color::White)
            {
                const Square fromSq(fromFile, toRank - 1);
                return Move{ fromSq, toSq };
            }
            else
            {
                const Square fromSq(fromFile, toRank + 1);
                return Move{ fromSq, toSq };
            }
        }
        // else if () // promotions
    }

    template <PieceType PieceTypeV>
    inline Move sanToMove(const Position& pos, const char* san)
    {
        // either
        // 01234 - indices
        // a1
        // aa1
        // 1a1
        // a1a1

        const int sanLen = len(san);

        const Square toSq = parseSquare(san + sanLen - 2);
        const Bitboard candidates = pos.piecesBB(Piece(PieceTypeV, pos.sideToMove())) & pseudoAttacksBB(PieceTypeV, toSq);
        const Square fromSq = Square::none();

        if (sanLen == 2)
        {
            // we may be able to resolve it trivially no matter the piece

            if (candidates.exactlyOne())
            {
                const Square fromSq = candidates.first();
                return Move{ fromSq, toSq };
            }
        }
        else if (sanLen == 4)
        {
            // we have everything we need already in the san
            const Square fromSq = parseSquare(san);
            return Move{ fromSq, toSq };
        }

        // we have to fallback to specialized disambiguators

        if (san[2] == '\0')
        {
            // a1
            // we know that there is more than one candidate in candidates
        }
        else if (san[3] == '\0')
        {
            // aa1
            // 1a1
        }
    }

    inline Move sanToMove_King(const Position& pos, const char* san)
    {
        // since we remove captures the only possible case is 
        // a1

        const Square fromSq = pos.pieceSquare(Piece(PieceType::King, pos.sideToMove()));
        const Square toSq = parseSquare(san);
        return Move{ fromSq, toSq };
    }

    inline Move sanToMove_Castle(const Position& pos, const char* san)
    {

    }
}

// assumes that the the san is correct and the move
// described by it is legal
// NOT const char* because it removes signs of capture
inline Move sanToMove(const Position& pos, char* san)
{
    // ?[NBRQK]?[a-h]?[1-8]?x[a-h][1-8]
    // *above regex contains all valid SAN strings
    // (but also some invalid ones)

    detail::san::removeSanDecorations(san);

    switch (san[0])
    {
    case 'N':
        return detail::san::sanToMove<PieceType::Knight>(pos, san + 1);
    case 'B':
        return detail::san::sanToMove<PieceType::Bishop>(pos, san + 1);
    case 'R':
        return detail::san::sanToMove<PieceType::Rook>(pos, san + 1);
    case 'Q':
        return detail::san::sanToMove<PieceType::Queen>(pos, san + 1);
    case 'K':
        return detail::san::sanToMove_King(pos, san + 1);
    case 'O':
        return detail::san::sanToMove_Castle(pos, san);
    default:
        return detail::san::sanToMove_Pawn(pos, san);
    }
}
