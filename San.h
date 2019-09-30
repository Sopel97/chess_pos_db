#pragma once

#include <string_view>

#include "Assert.h"
#include "Chess.h"
#include "Position.h"

namespace san
{
    using namespace std::literals;

    namespace detail
    {
        [[nodiscard]] constexpr int strlen(const char* san)
        {
            ASSERT(san != nullptr);

            // optimized for short strings
            const char* cur = san;
            while (*cur) ++cur;
            return static_cast<int>(cur - san);
        }

        constexpr void strcpy(char* out, const char* san)
        {
            ASSERT(out != nullptr);
            ASSERT(san != nullptr);

            while (*san)* out++ = *san++;
            *out = '\0';
        }

        constexpr void strcpy(char* out, const char* san, std::size_t length)
        {
            ASSERT(out != nullptr);
            ASSERT(san != nullptr);

            const char* end = san + length;
            while (san != end)* out++ = *san++;
            *out = '\0';
        }

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

        [[nodiscard]] constexpr Square parseSquare(std::string_view s)
        {
            ASSERT(s.size() >= 2u);

            const File file = parseFile(s[0]);
            const Rank rank = parseRank(s[1]);
            return Square(file, rank);
        }

        inline void appendSquareToString(Square sq, std::string& str)
        {
            str += static_cast<char>('a' + ordinal(sq.file()));
            str += static_cast<char>('1' + ordinal(sq.rank()));
        }

        [[nodiscard]] constexpr bool contains(std::string_view s, char c)
        {
            return s.find(c) != std::string::npos;
        }

        [[nodiscard]] constexpr bool isSanCapture(std::string_view san)
        {
            return contains(san, 'x');
        }

        // returns new length
        constexpr std::size_t removeSanCapture(char* san, std::size_t length)
        {
            // There is no valid san with length less than 4
            // that has a capture
            if (length < 4)
            {
                return length;
            }

            for (;;)
            {
                if (*san == 'x') break;
                if (*san == '\0') return length;
                ++san;
            }

            ASSERT(san[0] == 'x');
            // x__
            // ^san
            while (*san)
            {
                *san = *(san + 1);
                ++san;
            }

            ASSERT(!contains(san, 'x'));

            return length - 1u;
        }

        namespace lookup::removeSanDecorations
        {
            constexpr std::array<bool, 256> isDecoration = []() {
                std::array<bool, 256> isDecoration{};

                isDecoration['#'] = true;
                isDecoration['+'] = true;
                isDecoration['!'] = true;
                isDecoration['?'] = true;

                return isDecoration;
            }();
        }

        constexpr std::size_t removeSanDecorations(char* san, std::size_t length)
        {
            // removes capture designation
            // removes instances of the following characters:
            // # - mate
            // + - check
            // !
            // ?
            //
            // removal starts from the end of the san
            // and stops when any character not on the list above is found
            //
            // returns the new length of the san

            ASSERT(length >= 2);

            char* cur = san + length - 1u;

            while (lookup::removeSanDecorations::isDecoration[static_cast<unsigned char>(*cur)])
            {
                *cur = '\0';
                --cur;
                if (cur == san)
                {
                    return 0;
                }
            }

            const std::size_t newLength = cur - san + 1u;
            return removeSanCapture(san, newLength);
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
            }

            ASSERT(false);
            return PieceType::None;
        }

        [[nodiscard]] constexpr char pieceTypeToChar(PieceType pt)
        {
            switch (pt)
            {
            case PieceType::Knight:
                return 'N';
            case PieceType::Bishop:
                return 'B';
            case PieceType::Rook:
                return 'R';
            case PieceType::Queen:
                return 'Q';
            case PieceType::King:
                return 'K';
            }

            ASSERT(false);
            return '\0';
        }

        [[nodiscard]] constexpr Move sanToMove_Pawn(const Position& pos, std::string_view san)
        {
            // since we remove capture information it's either
            // 012345 idx
            // a1
            // aa1
            // a1=Q
            // aa1=Q

            const std::size_t sanLen = san.size();

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

                    const Square push1 = move.to + Offset{ 0, -1 };
                    const Square push2 = move.to + Offset{ 0, -2 };
                    if (pos.pieceAt(push1).type() == PieceType::Pawn)
                    {
                        move.from = push1;
                    }
                    // NOTE: if we leave else if here then msvc optimizer breaks the code by assuming
                    //       that the first condition (above) is always true
                    else // if (pos.pieceAt(push2).type() == PieceType::Pawn)
                    {
                        move.from = push2;
                    }
                }
                else
                {
                    ASSERT(move.to.rank() <= rank6);

                    const Square push1 = move.to + Offset{ 0, 1 };
                    const Square push2 = move.to + Offset{ 0, 2 };
                    if (pos.pieceAt(push1).type() == PieceType::Pawn)
                    {
                        move.from = push1;
                    }
                    else // if (pos.pieceAt(push2).type() == PieceType::Pawn)
                    {
                        move.from = push2;
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
            ASSERT(move.from.isOk());
            ASSERT(move.to.isOk());

            return move;
        }

        template <PieceType PieceTypeV>
        [[nodiscard]] constexpr Move sanToMove(const Position& pos, std::string_view san)
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

            const std::size_t sanLen = san.size();

            ASSERT(sanLen >= 2 && sanLen <= 4);

            const Square toSq = parseSquare(san.substr(sanLen - 2u));

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

            // if we are here then there are (should be) many pseudo-legal moves
            // but only one of them is legal

            for (Square fromSq : candidates)
            {
                const Move move{ fromSq, toSq };
                if (!pos.createsDiscoveredAttackOnOwnKing(move))
                {
                    ASSERT(pos.pieceAt(fromSq).type() == PieceTypeV);

                    return move;
                }
            }

            // shouldn't happen
            ASSERT(false);
            return Move::null();
        }

        [[nodiscard]] INTRIN_CONSTEXPR Move sanToMove_King(const Position& pos, std::string_view san)
        {
            // since we remove captures the only possible case is 
            // a1

            const Square fromSq = pos.kingSquare(pos.sideToMove());
            const Square toSq = parseSquare(san);

            ASSERT(pos.pieceAt(fromSq).type() == PieceType::King);

            return Move{ fromSq, toSq };
        }

        [[nodiscard]] constexpr Move sanToMove_Castle(const Position& pos, std::string_view san)
        {
            // either:
            // 012345 - idx
            // O-O-O
            // O-O

            const CastleType ct = san.size() == 3u ? CastleType::Short : CastleType::Long;
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
    [[nodiscard]] constexpr Move sanToMove(const Position& pos, char* san, std::size_t length)
    {
        // ?[NBRQK]?[a-h]?[1-8]?x[a-h][1-8]
        // *above regex contains all valid SAN strings
        // (but also some invalid ones)

        length = detail::removeSanDecorations(san, length);

        switch (san[0])
        {
        case 'N':
            return detail::sanToMove<PieceType::Knight>(pos, std::string_view(san + 1, length - 1));
        case 'B':
            return detail::sanToMove<PieceType::Bishop>(pos, std::string_view(san + 1, length - 1));
        case 'R':
            return detail::sanToMove<PieceType::Rook>(pos, std::string_view(san + 1, length - 1));
        case 'Q':
            return detail::sanToMove<PieceType::Queen>(pos, std::string_view(san + 1, length - 1));
        case 'K':
            return detail::sanToMove_King(pos, std::string_view(san + 1, length - 1));
        case 'O':
            return detail::sanToMove_Castle(pos, std::string_view(san, length));
        default:
            return detail::sanToMove_Pawn(pos, std::string_view(san, length));
        }
    }

    enum struct SanSpec : std::uint8_t
    {
        None = 0x0,
        Capture = 0x1,
        Check = 0x2,

        // not yet supported
        // Mate = 0x4, 
        // Compact = 0x8
    };

    [[nodiscard]] constexpr SanSpec operator|(SanSpec lhs, SanSpec rhs)
    {
        return static_cast<SanSpec>(static_cast<std::uint8_t>(lhs) | static_cast<std::uint8_t>(rhs));
    }

    [[nodiscard]] constexpr SanSpec operator&(SanSpec lhs, SanSpec rhs)
    {
        return static_cast<SanSpec>(static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(rhs));
    }

    // checks whether lhs contains rhs
    [[nodiscard]] constexpr bool contains(SanSpec lhs, SanSpec rhs)
    {
        return (lhs & rhs) == rhs;
    }

    template <SanSpec SpecV>
    [[nodiscard]] inline std::string moveToSan(const Position& pos, Move move)
    {
        std::string san;

        if (move.type == MoveType::Castle)
        {
            const CastlingRights type = moveToCastlingType(move);
            switch (type)
            {
            case CastlingRights::WhiteKingSide:
            case CastlingRights::BlackKingSide:
                san = "O-O";
                break;

            case CastlingRights::WhiteQueenSide:
            case CastlingRights::BlackQueenSide:
                san = "O-O-O";
                break;

            default:
                // clang gives warnings because CastlingRights has redundant values
                ASSERT(false);
            }
        }
        else
        {
            const Piece piece = pos.pieceAt(move.from);

            if (piece.type() != PieceType::Pawn)
            {
                san += detail::pieceTypeToChar(piece.type());
            }

            detail::appendSquareToString(move.from, san);

            if constexpr (contains(SpecV, SanSpec::Capture))
            {
                const bool isCapture =
                    (move.type == MoveType::EnPassant)
                    ? true
                    : (pos.pieceAt(move.to) != Piece::none());

                if (isCapture)
                {
                    san += 'x';
                }
            }

            detail::appendSquareToString(move.to, san);

            if (move.promotedPiece != Piece::none())
            {
                san += '=';
                san += detail::pieceTypeToChar(move.promotedPiece.type());
            }
        }

        if constexpr (contains(SpecV, SanSpec::Check))
        {
            if (pos.isCheck(move))
            {
                san += '+';
            }
        }

        return san;
    }

    constexpr std::array<bool, 256> validStart = []() {
        std::array<bool, 256> validStart{};

        validStart['N'] = true;
        validStart['B'] = true;
        validStart['R'] = true;
        validStart['Q'] = true;
        validStart['K'] = true;
        validStart['O'] = true;
        validStart['a'] = true;
        validStart['b'] = true;
        validStart['c'] = true;
        validStart['d'] = true;
        validStart['e'] = true;
        validStart['f'] = true;
        validStart['g'] = true;
        validStart['h'] = true;

        return validStart;
    }();

    [[nodiscard]] constexpr bool isValidSanMoveStart(char c)
    {
        return validStart[static_cast<unsigned char>(c)];
    }

    [[nodiscard]] constexpr Move sanToMove(const Position& pos, std::string_view san)
    {
        constexpr int maxSanLength = 15; // a very generous upper bound

        ASSERT(san.size() <= maxSanLength);

        char buffer[maxSanLength + 1] = { '\0' };
        detail::strcpy(buffer, san.data(), san.size());

        return sanToMove(pos, buffer, san.size());
    }
}

#if defined(USE_CONSTEXPR_INTRINSICS)
static_assert(san::sanToMove(Position::startPosition(), "a4") == Move{ A2, A4 });
static_assert(san::sanToMove(Position::startPosition(), "e3") == Move{ E2, E3 });
static_assert(san::sanToMove(Position::startPosition(), "Nf3") == Move{ G1, F3 });

static_assert(san::sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "a5") == Move{ A7, A5 });
static_assert(san::sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "e6") == Move{ E7, E6 });
static_assert(san::sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "Nf6") == Move{ G8, F6 });

static_assert(san::sanToMove(Position::fromFen("k7/8/8/4pP2/8/8/8/K7 w - e6 0 2"), "fxe6") == Move{ F5, E6, MoveType::EnPassant });

static_assert(san::sanToMove(Position::fromFen("k4q2/4p3/3Q1Q2/8/8/8/8/5K2 w - - 0 1"), "Qxe7") == Move{ D6, E7 });
static_assert(san::sanToMove(Position::fromFen("k2q4/4p3/3Q1Q2/8/8/8/8/3K4 w - - 0 1"), "Qxe7!?") == Move{ F6, E7 });

static_assert(san::sanToMove(Position::fromFen("k7/8/3Q1Q2/4r3/3Q1Q2/8/8/3K4 w - - 0 1"), "Qf6xe5") == Move{ F6, E5 });

static_assert(san::sanToMove(Position::fromFen("k7/8/3Q1Q2/4r3/8/8/8/3K4 w - - 0 1"), "Qfxe5??!") == Move{ F6, E5 });

static_assert(san::sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Nxd5") == Move{ B4, D5 });
static_assert(san::sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Kc1?") == Move{ D1, C1 });
static_assert(san::sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Nd4") == Move{ F5, D4 });

static_assert(san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Bh8") == Move{ E5, H8 });
static_assert(san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Beg7") == Move{ E5, G7 });
static_assert(san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Bhg7") == Move{ H6, G7 });
static_assert(san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Be4") == Move{ F3, E4 });

static_assert(san::sanToMove(Position::fromFen("8/2B5/7B/2B5/k1B5/2B5/8/K7 w - - 0 1"), "B7e5") == Move{ C7, E5 });

static_assert(san::sanToMove(Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "O-O!") == Move{ E1, H1, MoveType::Castle });
static_assert(san::sanToMove(Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "O-O-ON") == Move{ E1, A1, MoveType::Castle });

static_assert(san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=Q") == Move{ D7, D8, MoveType::Promotion, whiteQueen });
static_assert(san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=R") == Move{ D7, D8, MoveType::Promotion, whiteRook });
static_assert(san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=B") == Move{ D7, D8, MoveType::Promotion, whiteBishop });
static_assert(san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=N") == Move{ D7, D8, MoveType::Promotion, whiteKnight });

static_assert(san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=Q") == Move{ E2, E1, MoveType::Promotion, blackQueen });
static_assert(san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=R") == Move{ E2, E1, MoveType::Promotion, blackRook });
static_assert(san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=B") == Move{ E2, E1, MoveType::Promotion, blackBishop });
static_assert(san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=N") == Move{ E2, E1, MoveType::Promotion, blackKnight });
#endif
