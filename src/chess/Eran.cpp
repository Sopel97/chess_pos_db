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
}