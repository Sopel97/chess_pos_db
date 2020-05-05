#include "Uci.h"

#include "detail/ParserBits.h"

#include "Chess.h"
#include "Position.h"

#include <string>
#include <string_view>
#include <optional>

namespace uci
{
    [[nodiscard]] std::string moveToUci(const Position& pos, const Move& move)
    {
        std::string s;
        
        parser_bits::appendSquareToString(move.from, s);

        if (move.type == MoveType::Castle)
        {
            const CastleType castleType = CastlingTraits::moveCastlingType(move);

            const Square kingDestination = CastlingTraits::rookCastleDestinations[pos.sideToMove()][castleType];
            parser_bits::appendSquareToString(kingDestination, s);
        }
        else
        {
            parser_bits::appendSquareToString(move.to, s);

            if (move.type == MoveType::Promotion)
            {
                // lowercase piece symbol
                s += EnumTraits<PieceType>::toChar(move.promotedPiece.type(), Color::Black);
            }
        }

        return s;
    }

    [[nodiscard]] Move uciToMove(const Position& pos, std::string_view sv)
    {
        const Square from = parser_bits::parseSquare(sv.data());
        const Square to = parser_bits::parseSquare(sv.data() + 2);

        if (sv.size() == 5)
        {
            const PieceType promotedPieceType = *fromChar<PieceType>(sv[4]);
            return Move::promotion(from, to, Piece(promotedPieceType, pos.sideToMove()));
        }
        else
        {
            if (
                pos.pieceAt(from).type() == PieceType::King
                && std::abs(from.file() - to.file()) > 1
                )
            {
                // uci king destinations are on files C or G.
                const CastleType castleType =
                    (to.file() == fileG)
                    ? CastleType::Short
                    : CastleType::Long;

                return Move::castle(castleType, pos.sideToMove());
            }
            else if (pos.epSquare() == to)
            {
                return Move::enPassant(from, to);
            }
            else
            {
                return Move::normal(from, to);
            }
        }
    }

    [[nodiscard]] std::optional<Move> tryUciToMove(const Position& pos, std::string_view sv)
    {
        if (sv.size() < 4 || sv.size() > 5)
        {
            return std::nullopt;
        }

        const auto from = parser_bits::tryParseSquare(sv.substr(0, 2));
        const auto to = parser_bits::tryParseSquare(sv.substr(2, 2));

        Move move{};

        if (!from.has_value() || !to.has_value())
        {
            return std::nullopt;
        }

        if (sv.size() == 5)
        {
            const auto promotedPieceType = fromChar<PieceType>(sv[4]);
            if (!promotedPieceType.has_value())
            {
                return std::nullopt;
            }

            if (
                *promotedPieceType != PieceType::Knight
                && *promotedPieceType != PieceType::Bishop
                && *promotedPieceType != PieceType::Rook
                && *promotedPieceType != PieceType::Queen
                )
            {
                return std::nullopt;
            }

            move = Move::promotion(*from, *to, Piece(*promotedPieceType, pos.sideToMove()));
        }
        else // sv.size() == 4
        {

            if (
                pos.pieceAt(*from).type() == PieceType::King
                && std::abs(from->file() - to->file()) > 1
                )
            {
                // uci king destinations are on files C or G.

                if (pos.sideToMove() == Color::White)
                {
                    if (*from != e1)
                    {
                        return std::nullopt;
                    }

                    if (*to != c1 && *to != g1)
                    {
                        return std::nullopt;
                    }
                }
                else
                {
                    if (*from != e8)
                    {
                        return std::nullopt;
                    }

                    if (*to != c8 && *to != g8)
                    {
                        return std::nullopt;
                    }
                }

                const CastleType castleType =
                    (to->file() == fileG)
                    ? CastleType::Short
                    : CastleType::Long;

                move = Move::castle(castleType, pos.sideToMove());
            }
            else if (to == pos.epSquare())
            {
                move = Move::enPassant(*from, *to);
            }
            else
            {
                move = Move::normal(*from, *to);
            }
        }

        if (!pos.isMoveLegal(move))
        {
            return std::nullopt;
        }

        return move;
    }
}