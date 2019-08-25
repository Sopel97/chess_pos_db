#pragma once

#include "Assert.h"
#include "Chess.h"
#include "Position.h"

namespace detail::san
{
    [[nodiscard]] constexpr bool isFile(const char s)
    {
        return s >= 'a' && s <= 'h';
    }

    [[nodiscard]] constexpr bool isRank(const char s)
    {
        return s >= '1' && s <= '8';
    }

    [[nodiscard]] constexpr Rank parseRank(const char s)
    {
        ASSERT(isRank(s));

        return fromOrdinal<Rank>(s - '1');
    }

    [[nodiscard]] constexpr File parseFile(const char s)
    {
        ASSERT(isFile(s));

        return fromOrdinal<File>(s - 'a');
    }

    [[nodiscard]] constexpr Square parseSquare(const char* s)
    {
        const File file = parseFile(s[0]);
        const Rank rank = parseRank(s[1]);
        return Square(file, rank);
    }

    [[nodiscard]] constexpr bool contains(const char* s, char c)
    {
        while (*s)
        {
            if (*s == c) return true;
            ++s;
        }

        return false;
    }

    [[nodiscard]] constexpr bool isSanCapture(const char* san)
    {
        return contains(san, 'x');
    }

    constexpr void removeSanCapture(char* san)
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

        ASSERT(!contains(san, 'x'));
    }

    constexpr void removeSanDecorations(char* san)
    {
        // removes capture designation
        // removes instances of the following characters:
        // # - mate
        // + - check
        // !
        // ?
        // N - novelty
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
                *cur = '\0';
                continue;

            case 'N':
                if (cur != san && *(cur - 1) != '=')* cur = '\0';
                continue;
            }

            break;
        }

        removeSanCapture(san);
    }

    [[nodiscard]] constexpr int strlen(const char* san)
    {
        // optimized for short strings
        const char* cur = san;
        while (*cur) ++cur;
        return static_cast<int>(cur - san);
    }
    
    constexpr void strcpy(char* out, const char* san)
    {
        while (*san) *out++ = *san++;
        *out = '\0';
    }

    [[nodiscard]] constexpr PieceType parsePromotedPieceType(char c)
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
        default:
            ASSERT(false);
        }

        return PieceType::None;
    }

    [[nodiscard]] constexpr Move sanToMove_Pawn(const Position& pos, const char* san)
    {
        // since we remove capture information it's either
        // 012345 idx
        // a1
        // aa1
        // a1=Q
        // aa1=Q

        const int sanLen = strlen(san);

        ASSERT(sanLen >= 2 && sanLen <= 5);

        const Color color = pos.sideToMove();

        Move move{ Square::none(), Square::none(), MoveType::Normal, Piece::none() };

        if (sanLen == 2 || sanLen == 4)
        {
            // a1
            // a1=Q

            move.to = parseSquare(san);

            if (color == Color::White)
            {
                ASSERT(move.to.rank() >= rank3);

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
                ASSERT(move.to.rank() <= rank6);

                if (pos.pieceAt(move.to + Offset{ 0, 1 }).type() == PieceType::Pawn)
                {
                    move.from = move.to + Offset{ 0, 1 };
                }
                else if (pos.pieceAt(move.to + Offset{ 0, 2 }).type() == PieceType::Pawn)
                {
                    move.from = move.to + Offset{ 0, 2 };
                }
            }

            ASSERT(pos.pieceAt(move.to) == Piece::none());
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
            else
            {
                ASSERT(pos.pieceAt(move.to).type() == PieceType::Pawn);
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

        ASSERT(pos.pieceAt(move.from).type() == PieceType::Pawn);

        return move;
    }

    template <PieceType PieceTypeV>
    [[nodiscard]] constexpr Move sanToMove(const Position& pos, const char* san)
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

        const int sanLen = strlen(san); 

        ASSERT(sanLen >= 2 && sanLen <= 4);

        const Square toSq = parseSquare(san + sanLen - 2);
        if (sanLen == 4)
        {
            // we have everything we need already in the san
            const Square fromSq = parseSquare(san);

            ASSERT(pos.pieceAt(fromSq).type() == PieceTypeV);

            return Move{ fromSq, toSq };
        }

        // first consider all candidates with ray attacks to the toSq
        Bitboard candidates = pos.piecesBB(Piece(PieceTypeV, pos.sideToMove()));
        candidates &= bb::pseudoAttacks<PieceTypeV>(toSq);

        if (candidates.exactlyOne())
        {
            const Square fromSq = candidates.first();

            ASSERT(pos.pieceAt(fromSq).type() == PieceTypeV);

            return Move{ fromSq, toSq };
        }

        // if we have a knight then attacks==pseudoAttacks
        if (PieceTypeV != PieceType::Knight)
        {
            candidates &= bb::attacks<PieceTypeV>(toSq, pos.piecesBB());

            if (candidates.exactlyOne())
            {
                const Square fromSq = candidates.first();

                ASSERT(pos.pieceAt(fromSq).type() == PieceTypeV);

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

                ASSERT(pos.pieceAt(fromSq).type() == PieceTypeV);

                return Move{ fromSq, toSq };
            }
        }

        // if we are here then there are (should be) many pseudo-legal moves
        // but only one of them is legal

        for (Square fromSq : candidates)
        {
            const Move move{ fromSq, toSq };
            if (!pos.leavesKingInCheck(move))
            {
                ASSERT(pos.pieceAt(fromSq).type() == PieceTypeV);

                return move;
            }
        }

        // shouldn't happen
        ASSERT(false);
        return Move::null();
    }

    [[nodiscard]] constexpr Move sanToMove_King(const Position& pos, const char* san)
    {
        // since we remove captures the only possible case is 
        // a1

        const Square fromSq = pos.kingSquare(pos.sideToMove());
        const Square toSq = parseSquare(san);

        ASSERT(pos.pieceAt(fromSq).type() == PieceType::King);

        return Move{ fromSq, toSq };
    }

    [[nodiscard]] constexpr Move sanToMove_Castle(const Position& pos, const char* san)
    {
        // either:
        // 012345 - idx
        // O-O-O
        // O-O

        const CastleType ct = san[3] == '\0' ? CastleType::Short : CastleType::Long;
        const Color c = pos.sideToMove();

        const Move move = Move::castle(ct, c);

        ASSERT(pos.pieceAt(move.from).type() == PieceType::King);
        ASSERT(pos.pieceAt(move.to).type() == PieceType::Rook);

        return move;
    }
}

// assumes that the the san is correct and the move
// described by it is legal
// NOT const char* because it removes signs of capture
[[nodiscard]] constexpr Move sanToMove(const Position& pos, char* san)
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

[[nodiscard]] constexpr Move sanToMove(const Position& pos, const char* san)
{
    constexpr int maxSanLength = 16; // a very generous upper bound

    char buffer[maxSanLength]{};

    detail::san::strcpy(buffer, san);

    return sanToMove(pos, buffer);
}

#if defined(USE_CONSTEXPR_INTRINSICS)
static_assert(sanToMove(Position::startPosition(), "a4") == Move{ A2, A4 });
static_assert(sanToMove(Position::startPosition(), "e3") == Move{ E2, E3 });
static_assert(sanToMove(Position::startPosition(), "Nf3") == Move{ G1, F3 });

static_assert(sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "a5") == Move{ A7, A5 });
static_assert(sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "e6") == Move{ E7, E6 });
static_assert(sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "Nf6") == Move{ G8, F6 });

static_assert(sanToMove(Position::fromFen("k7/8/8/4pP2/8/8/8/K7 w - e6 0 2"), "fxe6") == Move{ F5, E6, MoveType::EnPassant });

static_assert(sanToMove(Position::fromFen("k4q2/4p3/3Q1Q2/8/8/8/8/5K2 w - - 0 1"), "Qxe7") == Move{ D6, E7 });
static_assert(sanToMove(Position::fromFen("k2q4/4p3/3Q1Q2/8/8/8/8/3K4 w - - 0 1"), "Qxe7!?") == Move{ F6, E7 });

static_assert(sanToMove(Position::fromFen("k7/8/3Q1Q2/4r3/3Q1Q2/8/8/3K4 w - - 0 1"), "Qf6xe5") == Move{ F6, E5 });

static_assert(sanToMove(Position::fromFen("k7/8/3Q1Q2/4r3/8/8/8/3K4 w - - 0 1"), "Qfxe5??!") == Move{ F6, E5 });

static_assert(sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Nxd5") == Move{ B4, D5 });
static_assert(sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Kc1?") == Move{ D1, C1 });
static_assert(sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Nd4") == Move{ F5, D4 });

static_assert(sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Bh8") == Move{ E5, H8 });
static_assert(sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Beg7") == Move{ E5, G7 });
static_assert(sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Bhg7") == Move{ H6, G7 });
static_assert(sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Be4") == Move{ F3, E4 });

static_assert(sanToMove(Position::fromFen("8/2B5/7B/2B5/k1B5/2B5/8/K7 w - - 0 1"), "B7e5") == Move{ C7, E5 });

static_assert(sanToMove(Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "O-O!") == Move{ E1, H1, MoveType::Castle });
static_assert(sanToMove(Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "O-O-ON") == Move{ E1, A1, MoveType::Castle });

static_assert(sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=Q") == Move{ D7, D8, MoveType::Promotion, whiteQueen });
static_assert(sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=R") == Move{ D7, D8, MoveType::Promotion, whiteRook });
static_assert(sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=B") == Move{ D7, D8, MoveType::Promotion, whiteBishop });
static_assert(sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=N") == Move{ D7, D8, MoveType::Promotion, whiteKnight });

static_assert(sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=Q") == Move{ E2, E1, MoveType::Promotion, blackQueen });
static_assert(sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=R") == Move{ E2, E1, MoveType::Promotion, blackRook });
static_assert(sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=B") == Move{ E2, E1, MoveType::Promotion, blackBishop });
static_assert(sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=N") == Move{ E2, E1, MoveType::Promotion, blackKnight });
#endif
