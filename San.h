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

    inline bool isFile(const char s)
    {
        return s >= 'a' && s <= 'h';
    }

    inline bool isRank(const char s)
    {
        return s >= '1' && s <= '8';
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

    inline PieceType parsePromotedPieceType(char c)
    {
        switch (c)
        {
        case 'N':
            return PieceType::Knight;
        case 'B':
            return PieceType::Bishop;
        case 'R':
            return PieceType::Rook;
        case 'Q':
            return PieceType::Queen;
        }

        return PieceType::None;
    }

    inline Move sanToMove_Pawn(const Position& pos, const char* san)
    {
        // since we remove capture information it's either
        // 012345 idx
        // a1
        // aa1
        // a1=Q
        // aa1=Q

        const int sanLen = len(san);
        const Color color = pos.sideToMove();

        Move move{ Square::none(), Square::none(), MoveType::Normal, Piece::none() };

        if (sanLen == 2 || sanLen == 4)
        {
            // a1
            // a1=Q

            move.to = parseSquare(san);

            if (color == Color::White)
            {
                if (pos.pieceAt(move.to + Offset{ 0, -1 }).type() == PieceType::Pawn)
                {
                    move.from = move.to + Offset{ 0, -1 };
                }
                else if (pos.pieceAt(move.to + Offset{ 0, -2 }).type() == PieceType::Pawn)
                {
                    move.from = move.to + Offset{ 0, -2 };
                }
            }
            else
            {
                if (pos.pieceAt(move.to + Offset{ 0, 1 }).type() == PieceType::Pawn)
                {
                    move.from = move.to + Offset{ 0, 1 };
                }
                else if (pos.pieceAt(move.to + Offset{ 0, 2 }).type() == PieceType::Pawn)
                {
                    move.from = move.to + Offset{ 0, 2 };
                }
            }
        }
        else if (sanLen == 3 || sanLen == 5)
        {
            // aa1
            // aa1=Q

            const File fromFile = parseFile(san[0]);
            const File toFile = parseFile(san[1]);
            const Rank toRank = parseRank(san[2]);

            move.to = Square(toFile, toRank);
            if (pos.pieceAt(move.to) == Piece::none())
            {
                move.type = MoveType::EnPassant;
            }

            if (pos.sideToMove() == Color::White)
            {
                move.from = Square(fromFile, toRank - 1);
            }
            else
            {
                move.from = Square(fromFile, toRank + 1);
            }
        }

        if (sanLen >= 4)
        {
            // promotion

            const PieceType promotedPieceType = parsePromotedPieceType(san[sanLen - 1]);

            move.type = MoveType::Promotion;
            move.promotedPiece = Piece(promotedPieceType, color);
        }

        return move;
    }

    template <PieceType PieceTypeV>
    inline Move sanToMove(const Position& pos, const char* san)
    {
        static_assert(
            PieceTypeV == PieceType::Knight 
            || PieceTypeV == PieceType::Bishop 
            || PieceTypeV == PieceType::Rook 
            || PieceTypeV == PieceType::Queen);

        // either
        // 01234 - indices
        // a1
        // aa1
        // 1a1
        // a1a1

        const int sanLen = len(san); 

        const Square toSq = parseSquare(san + sanLen - 2);
        if (sanLen == 4)
        {
            // we have everything we need already in the san
            return Move{ parseSquare(san), toSq };
        }

        // first consider all candidates with ray attacks to the toSq
        Bitboard candidates = pos.piecesBB(Piece(PieceTypeV, pos.sideToMove()));
        candidates &= bb::pseudoAttacks<PieceTypeV>(toSq);

        if (candidates.exactlyOne())
        {
            const Square fromSq = candidates.first();
            return Move{ fromSq, toSq };
        }

        // if we have a knight then attacks==pseudoAttacks
        if (PieceTypeV != PieceType::Knight)
        {
            candidates &= bb::attacks<PieceTypeV>(toSq, pos.piecesBB());

            if (candidates.exactlyOne())
            {
                const Square fromSq = candidates.first();
                return Move{ fromSq, toSq };
            }
        }

        if (sanLen == 3)
        {
            // we have one of the following to disambiguate with:
            // aa1
            // 1a1

            if (isFile(san[0]))
            {
                const File fromFile = parseFile(san[0]);
                candidates &= bb::file(fromFile);
            }
            else // if (isRank(san[0]))
            {
                const Rank fromRank = parseRank(san[0]);
                candidates &= bb::rank(fromRank);
            }

            if (candidates.exactlyOne())
            {
                const Square fromSq = candidates.first();
                return Move{ fromSq, toSq };
            }
        }

        // if we are here then there are (should be) many pseudo-legal moves
        // but only one of them is legal

        for (Square sq : candidates)
        {
            const Move move{ sq, toSq };
            if (!pos.leavesKingInCheck(move))
            {
                return move;
            }
        }

        // shouldn't happen
        return Move::null();
    }

    inline Move sanToMove_King(const Position& pos, const char* san)
    {
        // since we remove captures the only possible case is 
        // a1

        const Square fromSq = pos.kingSquare(pos.sideToMove());
        const Square toSq = parseSquare(san);
        return Move{ fromSq, toSq };
    }

    inline Move sanToMove_Castle(const Position& pos, const char* san)
    {
        // either:
        // 012345 - idx
        // O-O-O
        // O-O

        const CastleType ct = san[3] == '\0' ? CastleType::Short : CastleType::Long;
        const Color c = pos.sideToMove();

        return Move::castle(ct, c);
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
