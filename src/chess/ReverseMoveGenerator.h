#pragma once

#include "Bitboard.h"
#include "CastlingTraits.h"
#include "Chess.h"
#include "Position.h"

#include "enum/EnumArray.h"

namespace movegen
{
    struct PieceSetMask
    {
        bool pawn;
        bool knight;
        bool lightSquareBishop;
        bool darkSquareBishop;
        bool rook;
        bool queen;
    };

    struct PieceSet
    {
        std::uint8_t pawnCount;
        std::uint8_t knightCount;
        std::uint8_t lightSquareBishopCount;
        std::uint8_t darkSquareBishopCount;
        std::uint8_t rookCount;
        std::uint8_t queenCount;

        constexpr static PieceSet standardPieceSet()
        {
            return PieceSet(8, 2, 1, 1, 2, 1);
        }

        constexpr PieceSet() :
            pawnCount{},
            knightCount{},
            lightSquareBishopCount{},
            darkSquareBishopCount{},
            rookCount{},
            queenCount{}
        {
        }

        constexpr PieceSet(int pawnCount, int knightCount, int lsbCount, int dsbCount, int rookCount, int queenCount) :
            pawnCount(pawnCount),
            knightCount(knightCount),
            lightSquareBishopCount(lsbCount),
            darkSquareBishopCount(dsbCount),
            rookCount(rookCount),
            queenCount(queenCount)
        {
        }

        PieceSet(const Board& board, Color color) :
            pawnCount(board.pieceCount(Piece(PieceType::Pawn, color))),
            knightCount(board.pieceCount(Piece(PieceType::Knight, color))),
            lightSquareBishopCount((board.piecesBB(Piece(PieceType::Bishop, color)) & bb::lightSquares).count()),
            darkSquareBishopCount((board.piecesBB(Piece(PieceType::Bishop, color)) & bb::darkSquares).count()),
            rookCount(board.pieceCount(Piece(PieceType::Rook, color))),
            queenCount(board.pieceCount(Piece(PieceType::Queen, color)))
        {

        }

        [[nodiscard]] constexpr bool canTurnInto(const PieceSet& future) const
        {
            // Pawns can turn into pieces but the pawn count is bounded from above.

            const int availablePawnPromotions = pawnCount - future.pawnCount;
            if (availablePawnPromotions < 0)
            {
                // pawns cannot be added
                return false;
            }

            const int additionalQueens = std::max(0, future.queenCount - queenCount);
            const int additionalRooks = std::max(0, future.rookCount - rookCount);
            const int additionalLsbs = std::max(0, future.lightSquareBishopCount - lightSquareBishopCount);
            const int additionalDsbs = std::max(0, future.darkSquareBishopCount - darkSquareBishopCount);
            const int additionalKnights = std::max(0, future.knightCount - knightCount);

            return
                additionalQueens
                + additionalRooks
                + additionalLsbs
                + additionalDsbs
                + additionalKnights
                <= availablePawnPromotions;
        }

        // this must be able to turn into current
        [[nodiscard]] constexpr PieceSetMask piecesAvailableWithRemaining(const PieceSet& current) const
        {
            // this is start pieces
            // current is what's remaining

            const int additionalQueens = std::max(0, current.queenCount - queenCount);
            const int additionalRooks = std::max(0, current.rookCount - rookCount);
            const int additionalLsbs = std::max(0, current.lightSquareBishopCount - lightSquareBishopCount);
            const int additionalDsbs = std::max(0, current.darkSquareBishopCount - darkSquareBishopCount);
            const int additionalKnights = std::max(0, current.knightCount - knightCount);
            const int additionalPieces = additionalQueens
                + additionalRooks
                + additionalLsbs
                + additionalDsbs
                + additionalKnights;
            const bool hasUnusedPawnPromotions = (pawnCount - current.pawnCount - additionalPieces) > 0;

            return {
                hasUnusedPawnPromotions,
                hasUnusedPawnPromotions || (current.queenCount < queenCount),
                hasUnusedPawnPromotions || (current.rookCount < rookCount),
                hasUnusedPawnPromotions || (current.lightSquareBishopCount < lightSquareBishopCount),
                hasUnusedPawnPromotions || (current.darkSquareBishopCount < darkSquareBishopCount),
                hasUnusedPawnPromotions || (current.knightCount < knightCount)
            };
        }
    };

    namespace detail
    {
        auto makeUncaptureMaker(const Board& board, const PieceSet& startPieceSet, Color captureColor)
        {
            const auto currentPieceSet = PieceSet(board, captureColor);
            const auto mask = startPieceSet.piecesAvailableWithRemaining(currentPieceSet);

            return [mask, captureColor](ReverseMove rm, Square sq, auto&& func)
            {
                func(rm);
                if (!((bb::rank1 | bb::rank8).isSet(sq)) && mask.pawn)
                {
                    rm.capturedPiece = Piece(PieceType::Pawn, captureColor); func(rm);
                }

                if (mask.knight)
                {
                    rm.capturedPiece = Piece(PieceType::Knight, captureColor); func(rm);
                }

                if (bb::lightSquares.isSet(sq) && mask.lightSquareBishop)
                {
                    rm.capturedPiece = Piece(PieceType::Bishop, captureColor); func(rm);
                }

                if (bb::darkSquares.isSet(sq) && mask.darkSquareBishop)
                {
                    rm.capturedPiece = Piece(PieceType::Bishop, captureColor); func(rm);
                }

                if (mask.rook)
                {
                    rm.capturedPiece = Piece(PieceType::Rook, captureColor); func(rm);
                }

                if (mask.queen)
                {
                    rm.capturedPiece = Piece(PieceType::Queen, captureColor); func(rm);
                }
            };
        }
    }

    template <typename FuncT, typename UncaptureMakerT>
    void forEachPseudoLegalPawnReverseMove(const Position& pos, FuncT&& func, UncaptureMakerT&& captureMaker)
    {

    }

    template <typename FuncT, typename UncaptureMakerT>
    void forEachPseudoLegalReverseMove(const Position& pos, FuncT&& func, UncaptureMakerT&& uncaptureMaker)
    {
        forEachPseudoLegalPawnReverseMove(pos, std::forward<FuncT>(func), std::forward<UncaptureMakerT>(uncaptureMaker));
    }

    template <typename FuncT>
    void forEachPseudoLegalReverseMove(const Position& pos, const PieceSet& startPieceSet, FuncT&& func)
    {
        auto uncaptureMaker = detail::makeUncaptureMaker(pos, startPieceSet, !pos.sideToMove());

        forEachPseudoLegalReverseMove(pos, std::forward<FuncT>(func), uncaptureMaker);
    }

    template <typename FuncT>
    void forEachPseudoLegalReverseMove(const Position& pos, FuncT&& func)
    {
        auto forwarder = [](ReverseMove rm, Square sq, auto&& func) { func(rm); };

        forEachPseudoLegalReverseMove(pos, std::forward<FuncT>(func), forwarder);
    }
}
