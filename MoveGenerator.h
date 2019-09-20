#pragma once

#include "Chess.h"
#include "Position.h"

#include <algorithm>
#include <vector>

namespace movegen
{
    // For a pseudo-legal move the following are true:
    //  - the moving piece has the pos.sideToMove() color
    //  - the destination square is either empty or has a piece of the opposite color
    //  - if it is a pawn move it is valid (but may be illegal due to discovered checks)
    //  - if it is not a pawn move then the destination square is contained in attacks()
    //  - if it is a castling it is legal
    //  - a move other than castling may create a discovered attack on the king
    //  - a king may walk into a check

    namespace detail
    {
        // move has to be pseudo-legal
        bool isLegal(const Position& pos, Move move)
        {
            if (move.type == MoveType::Castle)
            {
                return true;
            }

            return !pos.createsAttackOnOwnKing(move);
        }
    }

    template <PieceType PieceTypeV>
    void generatePseudoLegalMoves(const Position& pos, std::vector<Move>& moves)
    {
        static_assert(PieceTypeV != PieceType::None);
        static_assert(PieceTypeV != PieceType::Pawn);
    }

    template <>
    void generatePseudoLegalMoves<PieceType::Pawn>(const Position& pos, std::vector<Move>& moves)
    {

    }

    void generateCastlingMoves(const Position& pos, std::vector<Move>& moves)
    {

    }

    // pos must not have a 'king capture' available
    std::vector<Move> generateAllPseudoLegalMoves(const Position& pos)
    {
        std::vector<Move> moves;

        generatePseudoLegalMoves<PieceType::Pawn>(pos, moves);
        generatePseudoLegalMoves<PieceType::Knight>(pos, moves);
        generatePseudoLegalMoves<PieceType::Bishop>(pos, moves);
        generatePseudoLegalMoves<PieceType::Rook>(pos, moves);
        generatePseudoLegalMoves<PieceType::Queen>(pos, moves);
        generatePseudoLegalMoves<PieceType::King>(pos, moves);
        generateCastlingMoves(pos, moves);

        return moves;
    }

    std::vector<Move> generateAllLegalMoves(const Position& pos)
    {
        std::vector<Move> moves = generateAllPseudoLegalMoves(pos);

        moves.erase(std::remove_if(moves.begin(), moves.end(), [pos](Move move) { return !detail::isLegal(pos, move); }), moves.end());

        return moves;
    }
}
