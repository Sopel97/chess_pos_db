#include "Eran.h"

#include "detail/ParserBits.h"

#include "Chess.h"
#include "Position.h"

#include <string>
#include <string_view>

using namespace std::literals;

namespace eran
{
    [[nodiscard]] std::string reverseMoveToEran(const Position& pos, const ReverseMove& rmove)
    {
        static constexpr EnumArray<PieceType, std::string_view> pieceTypeSymbol = [](){
            EnumArray<PieceType, std::string_view> ptc{};

            ptc[PieceType::Pawn] = ""sv;
            ptc[PieceType::Knight] = "N"sv;
            ptc[PieceType::Bishop] = "B"sv;
            ptc[PieceType::Rook] = "R"sv;
            ptc[PieceType::Queen] = "Q"sv;
            ptc[PieceType::King] = "K"sv;
            ptc[PieceType::None] = ""sv;

            return ptc;
        }();

        const Move& move = rmove.move;

        std::string result = "";

        if (move.type == MoveType::Castle)
        {
            if (move.to.file() == fileA)
            {
                result = "O-O-O";
            }
            else
            {
                result = "O-O";
            }
        }
        else if (move.type == MoveType::Promotion)
        {
            const PieceType promotedPieceType = pos.pieceAt(move.to).type();
            const PieceType capturedPieceType = rmove.capturedPiece.type();

            parser_bits::appendSquareToString(move.from, result);

            if (capturedPieceType == PieceType::None)
            {
                result += '-';
            }
            else
            {
                result += 'x';
                result += pieceTypeSymbol[capturedPieceType];
            }
            parser_bits::appendSquareToString(move.to, result);

            result += '=';
            result += pieceTypeSymbol[promotedPieceType];
        }
        else
        {
            const PieceType movedPieceType = pos.pieceAt(move.to).type();
            const PieceType capturedPieceType = rmove.capturedPiece.type();

            result += pieceTypeSymbol[movedPieceType];
            parser_bits::appendSquareToString(move.from, result);

            if (capturedPieceType == PieceType::None)
            {
                result += '-';
            }
            else
            {
                result += 'x';
                result += pieceTypeSymbol[capturedPieceType];
            }
            parser_bits::appendSquareToString(move.to, result);
        }

        result += ' ';
        parser_bits::appendCastlingRightsToString(rmove.oldCastlingRights, result);

        result += ' ';
        parser_bits::appendEpSquareToString(rmove.oldEpSquare, result);

        return result;
    }

    [[nodiscard]] ReverseMove eranToReverseMove(const Position& pos, std::string_view sv)
    {
        std::string_view moveSv = sv.substr(0, sv.find(' '));
        sv.remove_prefix(moveSv.size() + 1);

        const std::string_view castlingRightsSv = sv.substr(0, sv.find(' '));
        sv.remove_prefix(castlingRightsSv.size() + 1);

        const std::string_view epSquareSv = sv;

        Move move{};
        PieceType capturedPieceType = PieceType::None;
        if (moveSv == "O-O-O"sv || moveSv == "O-O"sv)
        {
            if (moveSv == "O-O-O"sv)
            {
                move = Move::castle(CastleType::Long, !pos.sideToMove());
            }
            else
            {
                move = Move::castle(CastleType::Short, !pos.sideToMove());
            }
        }
        else
        {
            PieceType pt = PieceType::Pawn;
            if (!parser_bits::isSquare(moveSv.data()))
            {
                // we have a piece type
                pt = *EnumTraits<PieceType>::fromChar(moveSv[0]);
                moveSv.remove_prefix(1);
            }

            move.from = parser_bits::parseSquare(moveSv.data());
            moveSv.remove_prefix(2);

            if (moveSv[0] == 'x')
            {
                moveSv.remove_prefix(1);
                if (!parser_bits::isSquare(moveSv.data()))
                {
                    // captured piece type
                    capturedPieceType = *EnumTraits<PieceType>::fromChar(moveSv[0]);
                    moveSv.remove_prefix(1);
                }
                else
                {
                    capturedPieceType = PieceType::Pawn;
                }
            }
            else
            {
                moveSv.remove_prefix(1);
            }

            move.to = parser_bits::parseSquare(moveSv.data());
            moveSv.remove_prefix(2);

            PieceType promotedPieceType = PieceType::None;
            if (pt == PieceType::Pawn && (move.to.rank() == rank1 || move.to.rank() == rank8))
            {
                promotedPieceType = *EnumTraits<PieceType>::fromChar(moveSv[0]);
                moveSv.remove_prefix(1);
            }

            if (promotedPieceType != PieceType::None)
            {
                move.promotedPiece = Piece(promotedPieceType, !pos.sideToMove());
                move.type = MoveType::Promotion;
            }
        }

        const CastlingRights oldCastlingRights = *parser_bits::tryParseCastlingRights(castlingRightsSv);
        const Square oldEpSquare = *parser_bits::tryParseEpSquare(epSquareSv);

        if (oldEpSquare == move.to && capturedPieceType == PieceType::Pawn)
        {
            capturedPieceType = PieceType::None;
            move.type = MoveType::EnPassant;
        }

        ReverseMove rm{};
        rm.move = move;
        if (capturedPieceType != PieceType::None)
        {
            rm.capturedPiece = Piece(capturedPieceType, pos.sideToMove());
        }
        rm.oldCastlingRights = oldCastlingRights;
        rm.oldEpSquare = oldEpSquare;

        return rm;
    }
}