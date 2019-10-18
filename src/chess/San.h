#pragma once

#include <string_view>
#include <optional>

#include "chess/detail/ParserBits.h"

#include "util/Assert.h"
#include "Chess.h"
#include "Position.h"

namespace san
{
    using namespace std::literals;

    namespace detail
    {
        [[nodiscard]] FORCEINLINE constexpr int strlen(const char* san)
        {
            ASSERT(san != nullptr);

            // optimized for short strings
            const char* cur = san;
            while (*cur) ++cur;
            return static_cast<int>(cur - san);
        }

        FORCEINLINE constexpr void strcpy(char* out, const char* san)
        {
            ASSERT(out != nullptr);
            ASSERT(san != nullptr);

            while (*san)* out++ = *san++;
            *out = '\0';
        }

        FORCEINLINE constexpr void strcpy(char* out, const char* san, std::size_t length)
        {
            ASSERT(out != nullptr);
            ASSERT(san != nullptr);

            const char* end = san + length;
            while (san != end)* out++ = *san++;
            *out = '\0';
        }

        [[nodiscard]] FORCEINLINE constexpr bool contains(std::string_view s, char c)
        {
            return s.find(c) != std::string::npos;
        }

        [[nodiscard]] FORCEINLINE constexpr bool isSanCapture(std::string_view san)
        {
            return contains(san, 'x');
        }

        // returns new length
        // requires that there is no decorations
        FORCEINLINE inline std::size_t removeSanCapture(char* san, std::size_t length)
        {
            // There is no valid san with length less than 4
            // that has a capture
            if (length < 4)
            {
                return length;
            }

            const char* end = san + length;
            for (; san != end && *san != 'x'; ++san);
            if (san == end)
            {
                return length;
            }

            ASSERT(san[0] == 'x');
            // x__
            // ^san
            // after the x there is either a1 or a1=Q
            // so we have to move at most 5 characters (0 terminator)
            // The buffer is always large enough so that this code is valid.
            san[0] = san[1];
            san[1] = san[2];
            san[2] = san[3];
            san[3] = san[4];
            san[4] = san[5];
            
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

        FORCEINLINE inline std::size_t removeSanDecorations(char* san, std::size_t length)
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
                --length;
                if (cur == san)
                {
                    return 0;
                }
            }

            return removeSanCapture(san, length);
        }

        [[nodiscard]] FORCEINLINE constexpr bool isPromotedPieceType(char c)
        {
            return c == 'N' || c == 'B' || c == 'R' || c == 'Q';
        }

        [[nodiscard]] FORCEINLINE constexpr PieceType parsePromotedPieceType(char c)
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

        [[nodiscard]] FORCEINLINE constexpr char pieceTypeToChar(PieceType pt)
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

        [[nodiscard]] inline Move sanToMove_Pawn(const Position& pos, const char* san, std::size_t sanLen)
        {
            // since we remove capture information it's either
            // 012345 idx
            // a1
            // aa1
            // a1=Q
            // aa1=Q

            ASSERT(sanLen >= 2 && sanLen <= 5);

            const Color color = pos.sideToMove();

            Move move{ Square::none(), Square::none(), MoveType::Normal, Piece::none() };

            if (sanLen == 2 || sanLen == 4)
            {
                // a1
                // a1=Q

                move.to = parser_bits::parseSquare(san);

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

                const File fromFile = parser_bits::parseFile(san[0]);
                const File toFile = parser_bits::parseFile(san[1]);
                const Rank toRank = parser_bits::parseRank(san[2]);

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
        [[nodiscard]] inline Move sanToMove(const Position& pos, const char* san, std::size_t sanLen)
        {
            static_assert(
                PieceTypeV == PieceType::Knight
                || PieceTypeV == PieceType::Bishop
                || PieceTypeV == PieceType::Rook
                || PieceTypeV == PieceType::Queen);

            // either
            // 012345 - indices
            // Na1
            // Naa1
            // N1a1
            // Na1a1

            ASSERT(sanLen >= 3 && sanLen <= 5);

            const Square toSq = parser_bits::parseSquare(san + (sanLen - 2u));

            if (sanLen == 5)
            {
                // we have everything we need already in the san
                const Square fromSq = parser_bits::parseSquare(san + 1);

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

            if (sanLen == 4)
            {
                // we have one of the following to disambiguate with:
                // aa1
                // 1a1

                if (parser_bits::isFile(san[1]))
                {
                    const File fromFile = parser_bits::parseFile(san[1]);
                    candidates &= bb::file(fromFile);
                }
                else // if (isRank(san[0]))
                {
                    const Rank fromRank = parser_bits::parseRank(san[1]);
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
                auto occ = pos.piecesBB();
                candidates &= bb::attacks<PieceTypeV>(toSq, occ);

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

        [[nodiscard]] inline Move sanToMove_King(const Position& pos, const char* san, std::size_t length)
        {
            // since we remove captures the only possible case is 
            // Ka1

            const Square fromSq = pos.kingSquare(pos.sideToMove());
            const Square toSq = parser_bits::parseSquare(san + 1);

            ASSERT(pos.pieceAt(fromSq).type() == PieceType::King);

            return Move{ fromSq, toSq };
        }

        [[nodiscard]] inline Move sanToMove_Castle(const Position& pos, const char* san, std::size_t length)
        {
            // either:
            // 012345 - idx
            // O-O-O
            // O-O

            const CastleType ct = length == 3u ? CastleType::Short : CastleType::Long;
            const Color c = pos.sideToMove();

            const Move move = Move::castle(ct, c);

            ASSERT(pos.pieceAt(move.from).type() == PieceType::King);
            ASSERT(pos.pieceAt(move.to).type() == PieceType::Rook);

            return move;
        }


        [[nodiscard]] inline std::optional<Move> trySanToMove_Pawn(const Position& pos, const char* san, std::size_t sanLen)
        {
            // since we remove capture information it's either
            // 012345 idx
            // a1
            // aa1
            // a1=Q
            // aa1=Q

            if (sanLen < 2 || sanLen > 5) return {};

            const Color color = pos.sideToMove();

            Move move{ Square::none(), Square::none(), MoveType::Normal, Piece::none() };

            if (sanLen == 2 || sanLen == 4)
            {
                // a1
                // a1=Q

                if (!parser_bits::isSquare(san)) return {};
                move.to = parser_bits::parseSquare(san);

                if (color == Color::White)
                {
                    if (move.to.rank() < rank3) return {};

                    const Square push1 = move.to + Offset{ 0, -1 };
                    const Square push2 = move.to + Offset{ 0, -2 };
                    if (pos.pieceAt(push1).type() == PieceType::Pawn)
                    {
                        move.from = push1;
                    }
                    else if (pos.pieceAt(push2).type() == PieceType::Pawn)
                    {
                        move.from = push2;
                    }
                    else
                    {
                        return {};
                    }
                }
                else
                {
                    if (move.to.rank() > rank6) return {};

                    const Square push1 = move.to + Offset{ 0, 1 };
                    const Square push2 = move.to + Offset{ 0, 2 };
                    if (pos.pieceAt(push1).type() == PieceType::Pawn)
                    {
                        move.from = push1;
                    }
                    else if (pos.pieceAt(push2).type() == PieceType::Pawn)
                    {
                        move.from = push2;
                    }
                    else
                    {
                        return {};
                    }
                }

                if (pos.pieceAt(move.to) != Piece::none()) return {};
            }
            else if (sanLen == 3 || sanLen == 5)
            {
                // aa1
                // aa1=Q

                if (!parser_bits::isFile(san[0])) return {};
                if (!parser_bits::isFile(san[1])) return {};
                if (!parser_bits::isRank(san[2])) return {};
                const File fromFile = parser_bits::parseFile(san[0]);
                const File toFile = parser_bits::parseFile(san[1]);
                const Rank toRank = parser_bits::parseRank(san[2]);

                if (pos.sideToMove() == Color::White)
                {
                    move.from = Square(fromFile, toRank - 1);
                }
                else
                {
                    move.from = Square(fromFile, toRank + 1);
                }

                move.to = Square(toFile, toRank);
                if (pos.pieceAt(move.to) == Piece::none())
                {
                    move.type = MoveType::EnPassant;
                    if (move.to != pos.epSquare()) return {};
                }
                else
                {
                    if (pos.pieceAt(move.to).color() == pos.sideToMove()) return {};
                }
            }

            if (sanLen >= 4)
            {
                // promotion

                if (!isPromotedPieceType(san[sanLen - 1])) return {};
                const PieceType promotedPieceType = parsePromotedPieceType(san[sanLen - 1]);

                move.type = MoveType::Promotion;
                move.promotedPiece = Piece(promotedPieceType, color);
            }
 
            if (pos.pieceAt(move.from).type() != PieceType::Pawn) return {};
            if (pos.pieceAt(move.from).color() != pos.sideToMove()) return {};
            if (!move.from.isOk()) return {};
            if (!move.to.isOk()) return {};
            if (pos.createsAttackOnOwnKing(move)) return {};

            return move;
        }

        template <PieceType PieceTypeV>
        [[nodiscard]] inline std::optional<Move> trySanToMove(const Position& pos, const char* san, std::size_t sanLen)
        {
            static_assert(
                PieceTypeV == PieceType::Knight
                || PieceTypeV == PieceType::Bishop
                || PieceTypeV == PieceType::Rook
                || PieceTypeV == PieceType::Queen);

            // either
            // 012345 - indices
            // Na1
            // Naa1
            // N1a1
            // Na1a1

            auto isValid = [&pos](Move move) {
                if (pos.pieceAt(move.from).type() != PieceTypeV) return false;
                if (pos.pieceAt(move.from).color() != pos.sideToMove()) return false;
                if (pos.pieceAt(move.to) != Piece::none() && pos.pieceAt(move.to).color() == pos.sideToMove()) return false;
                if (pos.createsDiscoveredAttackOnOwnKing(move)) return false;
                return true;
            };

            if (sanLen < 3 || sanLen > 5) return {};
            if (!parser_bits::isSquare(san + (sanLen - 2u))) return {};
            
            const Square toSq = parser_bits::parseSquare(san + (sanLen - 2u));

            if (sanLen == 5)
            {
                // we have everything we need already in the san
                if (!parser_bits::isSquare(san + 1)) return {};

                const Square fromSq = parser_bits::parseSquare(san + 1);

                const Move move{ fromSq, toSq };
                if (!isValid(move)) return {};
                return move;
            }

            // first consider all candidates with ray attacks to the toSq
            Bitboard candidates = pos.piecesBB(Piece(PieceTypeV, pos.sideToMove()));
            candidates &= bb::pseudoAttacks<PieceTypeV>(toSq);

            if (candidates.exactlyOne())
            {
                const Square fromSq = candidates.first();

                const Move move{ fromSq, toSq };
                if (!isValid(move)) return {};
                return move;
            }

            if (sanLen == 4)
            {
                // we have one of the following to disambiguate with:
                // aa1
                // 1a1

                if (parser_bits::isFile(san[1]))
                {
                    const File fromFile = parser_bits::parseFile(san[1]);
                    candidates &= bb::file(fromFile);
                }
                else if (parser_bits::isRank(san[1]))
                {
                    const Rank fromRank = parser_bits::parseRank(san[1]);
                    candidates &= bb::rank(fromRank);
                }

                if (candidates.exactlyOne())
                {
                    const Square fromSq = candidates.first();

                    const Move move{ fromSq, toSq };
                    if (!isValid(move)) return {};
                    return move;
                }
            }

            // if we have a knight then attacks==pseudoAttacks
            if (PieceTypeV != PieceType::Knight)
            {
                auto occ = pos.piecesBB();
                candidates &= bb::attacks<PieceTypeV>(toSq, occ);

                if (candidates.exactlyOne())
                {
                    const Square fromSq = candidates.first();

                    const Move move{ fromSq, toSq };
                    if (!isValid(move)) return {};
                    return move;
                }
            }

            // if we are here then there are (should be) many pseudo-legal moves
            // but only one of them is legal

            for (Square fromSq : candidates)
            {
                const Move move{ fromSq, toSq };
                if (!pos.createsDiscoveredAttackOnOwnKing(move))
                {
                    if (!isValid(move)) return {};
                    return move;
                }
            }

            return {};
        }

        [[nodiscard]] inline std::optional<Move> trySanToMove_King(const Position& pos, const char* san, std::size_t length)
        {
            // since we remove captures the only possible case is 
            // Ka1

            if (length != 3) return {};
            if (!parser_bits::isSquare(san + 1)) return {};

            const Square fromSq = pos.kingSquare(pos.sideToMove());
            const Square toSq = parser_bits::parseSquare(san + 1);

            const Move move{ fromSq, toSq };
            if (pos.createsAttackOnOwnKing(move)) return {};

            return move;
        }

        [[nodiscard]] inline std::optional<Move> trySanToMove_Castle(const Position& pos, const char* san, std::size_t length)
        {
            // either:
            // 012345 - idx
            // O-O-O
            // O-O

            if (length != 3 && length != 5) return {};
            if (length == 3 && san != "O-O"sv) return {};
            if (length == 5 && san != "O-O-O"sv) return {};

            const CastleType ct = length == 3u ? CastleType::Short : CastleType::Long;
            const Color c = pos.sideToMove();

            const CastlingRights castlingRights = pos.castlingRights();
            const CastlingRights requiredCastlingRights =
                ct == CastleType::Short
                ? (c == Color::White ? CastlingRights::WhiteKingSide : CastlingRights::BlackKingSide)
                : (c == Color::White ? CastlingRights::WhiteQueenSide : CastlingRights::BlackQueenSide);

            const Move move = Move::castle(ct, c);

            if (
                !contains(castlingRights, requiredCastlingRights)
                || pos.pieceAt(move.from).type() != PieceType::King
                || pos.pieceAt(move.to).type() != PieceType::Rook
                )
            {
                return {};
            }
            
            return move;
        }

        namespace lookup::sanToMove
        {
            constexpr std::array<Move(*)(const Position&, const char*, std::size_t), 256> funcs = []() {
                std::array<Move(*)(const Position&, const char*, std::size_t), 256> funcs{};

                for (auto& f : funcs)
                {
                    f = [](const Position& pos, const char* san, std::size_t length) {return Move::null(); };
                }

                funcs['N'] = detail::sanToMove<PieceType::Knight>;
                funcs['B'] = detail::sanToMove<PieceType::Bishop>;
                funcs['R'] = detail::sanToMove<PieceType::Rook>;
                funcs['Q'] = detail::sanToMove<PieceType::Queen>;
                funcs['K'] = detail::sanToMove_King;
                funcs['O'] = detail::sanToMove_Castle;
                funcs['a'] = detail::sanToMove_Pawn;
                funcs['b'] = detail::sanToMove_Pawn;
                funcs['c'] = detail::sanToMove_Pawn;
                funcs['d'] = detail::sanToMove_Pawn;
                funcs['e'] = detail::sanToMove_Pawn;
                funcs['f'] = detail::sanToMove_Pawn;
                funcs['g'] = detail::sanToMove_Pawn;
                funcs['h'] = detail::sanToMove_Pawn;

                return funcs;
            }();
        }

        // assumes that the the san is correct and the move
        // described by it is legal
        // NOT const char* because it removes signs of capture
        [[nodiscard]] inline Move sanToMove(const Position& pos, char* san, std::size_t length)
        {
            // ?[NBRQK]?[a-h]?[1-8]?x[a-h][1-8]
            // *above regex contains all valid SAN strings
            // (but also some invalid ones)

            length = detail::removeSanDecorations(san, length);

            return lookup::sanToMove::funcs[static_cast<unsigned char>(san[0])](pos, san, length);
        }

        [[nodiscard]] inline std::optional<Move> trySanToMove(const Position& pos, char* san, std::size_t length)
        {
            // ?[NBRQK]?[a-h]?[1-8]?x[a-h][1-8]
            // *above regex contains all valid SAN strings
            // (but also some invalid ones)

            length = detail::removeSanDecorations(san, length);
            if (length < 2)
            {
                return {};
            }

            switch (san[0])
            {
            case 'N':
                return detail::trySanToMove<PieceType::Knight>(pos, san, length);
            case 'B':
                return detail::trySanToMove<PieceType::Bishop>(pos, san, length);
            case 'R':
                return detail::trySanToMove<PieceType::Rook>(pos, san, length);
            case 'Q':
                return detail::trySanToMove<PieceType::Queen>(pos, san, length);
            case 'K':
                return detail::trySanToMove_King(pos, san, length);
            case 'O':
                return detail::trySanToMove_Castle(pos, san, length);
            case '-':
                return Move::null();
            default:
                return detail::trySanToMove_Pawn(pos, san, length);
            }
        }
    }

    enum struct SanSpec : std::uint8_t
    {
        None = 0x0,
        Capture = 0x1,
        Check = 0x2,

        Compact = 0x8

        // not yet supported
        // Mate = 0x4
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

    namespace detail
    {
        inline void appendUnambiguousCompactFrom(const Position& pos, const Move& move, std::string& san)
        {
            const Piece piece = pos.pieceAt(move.from);

            const PieceType type = piece.type();

            const bool isCapture =
                (move.type == MoveType::EnPassant)
                || (pos.pieceAt(move.to) != Piece::none());

            if (type == PieceType::Pawn)
            {
                if (isCapture)
                {
                    parser_bits::appendFileToString(move.from.file(), san);
                    return;
                }
                return;
            }
            
            // There is only one king so no disambiguation needed.
            if (type == PieceType::King)
            {
                return;
            }

            // We have to find out whether disambiguation is needed
            // We don't care about performance here so we do it the easy but slow way.
            Bitboard candidates = pos.piecesBB(piece);
            candidates &= bb::attacks(type, move.to, pos.piecesBB());
            if (candidates.exactlyOne())
            {
                // Do nothing, no disambiguation needed.
                return;
            }
                
            for (Square fromSq : candidates)
            {
                const Move candidateMove{ fromSq, move.to };
                if (pos.createsDiscoveredAttackOnOwnKing(candidateMove))
                {
                    candidates ^= fromSq;
                }
            }

            if (candidates.exactlyOne())
            {
                // Do nothing, no disambiguation needed
                // as there is only one legal move.
                return;
            }

            if ((candidates & bb::file(move.from.file())).exactlyOne())
            {
                // Adding file disambiguates.
                parser_bits::appendFileToString(move.from.file(), san);
                return;
            }

            if ((candidates & bb::rank(move.from.rank())).exactlyOne())
            {
                // Adding rank disambiguates.
                parser_bits::appendRankToString(move.from.rank(), san);
                return;
            }

            // Full square is needed.
            parser_bits::appendSquareToString(move.from, san);
        }
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

            const PieceType type = piece.type();

            if (type != PieceType::Pawn)
            {
                san += detail::pieceTypeToChar(piece.type());
            }

            if constexpr (contains(SpecV, SanSpec::Compact))
            {
                detail::appendUnambiguousCompactFrom(pos, move, san);
            }
            else
            {
                parser_bits::appendSquareToString(move.from, san);
            }

            if constexpr (contains(SpecV, SanSpec::Capture))
            {
                const bool isCapture =
                    (move.type == MoveType::EnPassant)
                    || (pos.pieceAt(move.to) != Piece::none());

                if (isCapture)
                {
                    san += 'x';
                }
            }

            parser_bits::appendSquareToString(move.to, san);

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
        validStart['-'] = true;

        return validStart;
    }();

    [[nodiscard]] constexpr bool isValidSanMoveStart(char c)
    {
        return validStart[static_cast<unsigned char>(c)];
    }

    [[nodiscard]] inline Move sanToMove(const Position& pos, std::string_view san)
    {
        constexpr int maxSanLength = 15; // a very generous upper bound

        ASSERT(san.size() <= maxSanLength);

        char buffer[maxSanLength + 1] = { '\0' };
        detail::strcpy(buffer, san.data(), san.size());

        return detail::sanToMove(pos, buffer, san.size());
    }

    [[nodiscard]] inline std::optional<Move> trySanToMove(const Position& pos, std::string_view san)
    {
        constexpr int maxSanLength = 15; // a very generous upper bound

        if (san.size() > maxSanLength)
        {
            return {};
        }

        char buffer[maxSanLength + 1] = { '\0' };
        detail::strcpy(buffer, san.data(), san.size());

        return detail::trySanToMove(pos, buffer, san.size());
    }
}
