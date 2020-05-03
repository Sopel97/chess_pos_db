#include "MoveGenerator.h"

#include "Chess.h"
#include "Position.h"

#include <vector>

namespace movegen
{
    // pos must not have a 'king capture' available
    [[nodiscard]] std::vector<Move> generatePseudoLegalMoves(const Position& pos)
    {
        std::vector<Move> moves;

        auto addMove = [&moves](Move move) {
            moves.emplace_back(move);
        };

        forEachPseudoLegalMove(pos, addMove);

        return moves;
    }

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