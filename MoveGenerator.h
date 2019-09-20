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
        [[nodiscard]] inline bool isLegal(const Position& pos, Move move)
        {
            if (move.type == MoveType::Castle)
            {
                return true;
            }

            return !pos.createsAttackOnOwnKing(move);
        }
    }

    template <PieceType PieceTypeV>
    inline void generatePseudoLegalMoves(const Position& pos, std::vector<Move>& moves)
    {
        static_assert(PieceTypeV != PieceType::None);
        static_assert(PieceTypeV != PieceType::Pawn);

        const Color sideToMove = pos.sideToMove();
        const Bitboard ourPieces = pos.piecesBB(sideToMove);
        const Bitboard theirPieces = pos.piecesBB(!sideToMove);
        const Bitboard occupied = ourPieces | theirPieces;
        const Bitboard pieces = pos.piecesBB(Piece(PieceTypeV, sideToMove));
        for (Square fromSq : pieces)
        {
            const Bitboard attacks = bb::attacks<PieceTypeV>(fromSq, occupied) & ~ourPieces;
            for (Square toSq : attacks)
            {
                Move move{ fromSq, toSq };
                moves.emplace_back(move);
            }
        }
    }

    template <>
    inline void generatePseudoLegalMoves<PieceType::Pawn>(const Position& pos, std::vector<Move>& moves)
    {
        const Color sideToMove = pos.sideToMove();
        const Bitboard ourPieces = pos.piecesBB(sideToMove);
        const Bitboard theirPieces = pos.piecesBB(!sideToMove);
        const Bitboard occupied = ourPieces | theirPieces;
        const Bitboard pawns = pos.piecesBB(Piece(PieceType::Pawn, sideToMove));

        auto generate = [&](Square fromSq)
        {
            const Bitboard attacks = bb::pawnAttacks(Bitboard::square(fromSq), sideToMove) & theirPieces;

            const Rank startRank = sideToMove == Color::White ? rank2 : rank7;
            const Rank secondToLastRank = sideToMove == Color::White ? rank7 : rank2;
            const Offset forward = Offset{ 0, (sideToMove == Color::White ? 1 : -1) };

            if (fromSq.rank() == secondToLastRank)
            {
                for (Square toSq : attacks)
                {
                    for (PieceType pt : { PieceType::Knight, PieceType::Bishop, PieceType::Rook, PieceType::Queen })
                    {
                        Move move{ fromSq, toSq, MoveType::Promotion, Piece(pt, sideToMove) };
                        moves.emplace_back(move);
                    }
                }

                const Square toSq = fromSq + forward;
                if (!occupied.isSet(toSq))
                {
                    for (PieceType pt : { PieceType::Knight, PieceType::Bishop, PieceType::Rook, PieceType::Queen })
                    {
                        Move move{ fromSq, toSq, MoveType::Promotion, Piece(pt, sideToMove) };
                        moves.emplace_back(move);
                    }
                }
            }
            else
            {
                for (Square toSq : attacks)
                {
                    Move move{ fromSq, toSq };
                    moves.emplace_back(move);
                }

                const Square toSq = fromSq + forward;

                if (!occupied.isSet(toSq))
                {
                    if (fromSq.rank() == startRank)
                    {
                        const Square toSq2 = toSq + forward;
                        if (!occupied.isSet(toSq2))
                        {
                            Move move{ fromSq, toSq2 };
                            moves.emplace_back(move);
                        }
                    }

                    Move move{ fromSq, toSq };
                    moves.emplace_back(move);
                }
            }

            // TODO: en passant
        };

        for (Square fromSq : pawns)
        {
            generate(fromSq);
        }
    }

    inline void generateCastlingMoves(const Position& pos, std::vector<Move>& moves)
    {

    }

    // pos must not have a 'king capture' available
    inline std::vector<Move> generateAllPseudoLegalMoves(const Position& pos)
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

    inline std::vector<Move> generateAllLegalMoves(const Position& pos)
    {
        std::vector<Move> moves = generateAllPseudoLegalMoves(pos);

        moves.erase(std::remove_if(moves.begin(), moves.end(), [pos](Move move) { return !detail::isLegal(pos, move); }), moves.end());

        return moves;
    }
}
