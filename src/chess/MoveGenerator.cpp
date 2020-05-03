#include "MoveGenerator.h"

#include "Chess.h"
#include "Position.h"

#include <vector>

namespace movegen
{
    [[nodiscard]] std::vector<Move> generatePseudoLegalMoves(const Position& pos)
    {
        std::vector<Move> moves;

        auto addMove = [&moves](Move move) {
            moves.emplace_back(move);
        };

        forEachPseudoLegalMove(pos, addMove);

        return moves;
    }

    // Generates all legal moves for the position.
    // `pos` must be a legal chess position
    [[nodiscard]] std::vector<Move> generateLegalMoves(const Position& pos)
    {
        std::vector<Move> moves;

        auto addMove = [&moves](Move move) {
            moves.emplace_back(move);
        };

        forEachLegalMove(pos, addMove);

        return moves;
    }
}