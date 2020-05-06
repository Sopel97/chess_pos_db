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

        [[nodiscard]] constexpr static PieceSetMask allUncaptures()
        {
            return { true, true, true, true, true, true };
        }

        [[nodiscard]] constexpr static PieceSetMask allUnpromotions()
        {
            return { false, true, true, true, true, true };
        }
    };

    struct PieceSet
    {
        std::uint8_t pawnCount;
        std::uint8_t knightCount;
        std::uint8_t lightSquareBishopCount;
        std::uint8_t darkSquareBishopCount;
        std::uint8_t rookCount;
        std::uint8_t queenCount;

        [[nodiscard]] constexpr static PieceSet standardPieceSet()
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
        [[nodiscard]] constexpr PieceSetMask uncapturesWithRemaining(const PieceSet& current) const
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

        // this must be able to turn into current
        [[nodiscard]] constexpr PieceSetMask unpromotionsWithRemaining(const PieceSet& current) const
        {
            // this is start pieces
            // current is what's remaining

            // To be able to unpromote a piece we have to have either
            // 1. an unused pawn promotion - so we have a pawn that was just captured (or a promoted piece captured)
            // 2. more pieces of the type than at the start - because that piece must have came from a promotion

            // for example with rbqqknbr/8/ppppppp1 the only possible unpromotion is queen unpromotion because otherwise
            // we would end up with two queens and 8 pawns.

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
                false,
                hasUnusedPawnPromotions || (current.queenCount > queenCount),
                hasUnusedPawnPromotions || (current.rookCount > rookCount),
                hasUnusedPawnPromotions || (current.lightSquareBishopCount > lightSquareBishopCount),
                hasUnusedPawnPromotions || (current.darkSquareBishopCount > darkSquareBishopCount),
                hasUnusedPawnPromotions || (current.knightCount > knightCount)
            };
        }
    };

    namespace detail
    {
        [[nodiscard]] auto makeUncaptureMaker(const PieceSetMask& mask, Color captureColor)
        {
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
                else if (bb::darkSquares.isSet(sq) && mask.darkSquareBishop)
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

        [[nodiscard]] auto makeUncaptureMaker(const Board& board, const PieceSet& startPieceSet, Color captureColor)
        {
            const auto currentPieceSet = PieceSet(board, captureColor);
            const auto mask = startPieceSet.uncapturesWithRemaining(currentPieceSet);

            return makeUncaptureMaker(mask, captureColor);
        }

        [[nodiscard]] auto makeUncaptureMaker(Color captureColor)
        {
            // no restrictions apart from pawns only on 48 squares.
            return [captureColor](ReverseMove rm, Square sq, auto&& func)
            {
                func(rm);

                if (!((bb::rank1 | bb::rank8).isSet(sq)))
                {
                    rm.capturedPiece = Piece(PieceType::Pawn, captureColor); func(rm);
                }

                rm.capturedPiece = Piece(PieceType::Knight, captureColor); func(rm);
                rm.capturedPiece = Piece(PieceType::Bishop, captureColor); func(rm);
                rm.capturedPiece = Piece(PieceType::Rook, captureColor); func(rm);
                rm.capturedPiece = Piece(PieceType::Queen, captureColor); func(rm);
            };
        }

        template <typename UncaptureMakerT>
        [[nodiscard]] auto makeUnpromotionMaker(const PieceSetMask& mask, Color pawnColor, UncaptureMakerT&& uncaptureMaker)
        {
            return [mask, pawnColor, uncaptureMaker](ReverseMove rm, Square sq, PieceType pt, auto&& func)
            {
                bool isValid = false;
                rm.move.type = MoveType::Promotion;
                rm.move.promotedPiece = Piece(pt, pawnColor);

                switch (pt)
                {
                case PieceType::Knight:
                {
                    if (mask.knight)
                    {
                        isValid = true;
                    }
                    break;
                }

                case PieceType::Bishop:
                {
                    if (bb::lightSquares.isSet(sq) && mask.lightSquareBishop)
                    {
                        isValid = true;
                    }
                    else if (bb::darkSquares.isSet(sq) && mask.darkSquareBishop)
                    {
                        isValid = true;
                    }

                    break;
                }

                case PieceType::Rook:
                {
                    if (mask.rook)
                    {
                        isValid = true;
                    }
                    break;
                }

                case PieceType::Queen:
                {
                    if (mask.queen)
                    {
                        isValid = true;
                    }
                    break;
                }
                }

                if (isValid)
                {
                    uncaptureMaker(rm, sq, func);
                }
            };
        }

        template <typename UncaptureMakerT>
        [[nodiscard]] auto makeUnpromotionMaker(
            const Board& board, 
            const PieceSet& startPieceSet, 
            Color pawnColor, 
            UncaptureMakerT&& uncaptureMaker
        )
        {
            const auto currentPieceSet = PieceSet(board, pawnColor);
            const auto mask = startPieceSet.uncapturesWithRemaining(currentPieceSet);

            return makeUnpromotionMaker(mask, pawnColor, std::forward<UncaptureMakerT>(uncaptureMaker));
        }

        template <typename UncaptureMakerT>
        [[nodiscard]] auto makeUnpromotionMaker(Color pawnColor, UncaptureMakerT&& uncaptureMaker)
        {
            return [pawnColor, uncaptureMaker](ReverseMove rm, Square sq, PieceType pt, auto&& func)
            {
                rm.move.type = MoveType::Promotion;
                rm.move.promotedPiece = Piece(pt, pawnColor);
                uncaptureMaker(rm, sq, func);
            };
        }

        template <typename FuncT>
        [[nodiscard]] auto makeMultiplicatorForEpRights(const Board& board, Color sideToUnmove, FuncT&& func)
        {
            const Bitboard pieces = board.piecesBB();
            Bitboard candidateEpSquares = Bitboard::none();
            if (sideToUnmove == Color::White)
            {
                const Bitboard pawnsOn6 = board.piecesBB(Piece(PieceType::Pawn, !sideToUnmove)) & bb::rank6;
                candidateEpSquares = (
                    pawnsOn6
                    & ~(pieces.shiftedVertically(-1) | pieces.shiftedVertically(-2))
                    ).shiftedVertically(1);
            }
            else
            {
                const Bitboard pawnsOn3 = board.piecesBB(Piece(PieceType::Pawn, !sideToUnmove)) & bb::rank3;
                candidateEpSquares = (
                    pawnsOn3
                    & ~(pieces.shiftedVertically(1) | pieces.shiftedVertically(2))
                    ).shiftedVertically(-1);
            }


            return [candidateEpSquares, &board, &func](ReverseMove rm)
            {
                const Piece movedPiece = board.pieceAt(rm.move.to);
                Bitboard currentCandidateEpSquares = candidateEpSquares;
                Square oldEpSquare = Square::none();
                Bitboard pawns = board.piecesBB(Piece(PieceType::Pawn, sideToUnmove));
                if (movedPiece != Piece::none() && movedPiece.type() == PieceType::Pawn)
                {
                    // We have to update our pawns locations
                    pawns ^= rm.move.to;
                    pawns ^= rm.move.from;

                    // If the reverse move is en-passnt then there was an
                    // additional ep square that was not found in the current
                    // position because the pawn was captured.
                    if (rm.move.type == MoveType::EnPassant)
                    {
                        currentCandidateEpSquares |= rm.move.to;

                        // We keep this in mind because we don't have to verify 
                        // if it was possible to have these ep rights.
                        oldEpSquare = rm.move.to;
                    }
                }

                // We only consider candidate ep squares that are attacked by our pawns.
                // Otherwise nothing could execute the en-passant so the flag
                // cannot be set.
                currentCandidateEpSquares &= bb::pawnAttacks(pawns, sideToUnmove);

                for (Square epSquare : currentCandidateEpSquares)
                {
                    if (epSquare != oldEpSquare)
                    {
                        // TODO: if performing an en-passant with this square
                        // would have put our king in a discovered check then
                        // this epSquare is not valid.
                        // This check has to be performed for each reverse move
                        // and involves doing the reverse move and checking whether
                        // our king would be attacked after en-passant.
                        // So we have to account for piece movements of 
                        // one reverse move and one move to check this.
                        // if (!valid) continue;
                    }

                    rm.oldEpSquare = epSquare;
                    func(rm);
                }

                rm.oldEpSquare = Square::none();
                func(rm);
            };
        }
    }

    template <typename FuncT, typename UncaptureMakerT, typename UnpromotionMakerT>
    void forEachPseudoLegalPawnReverseMove(
        const Position& pos, 
        FuncT&& func, 
        UncaptureMakerT&& uncaptureMaker, 
        UnpromotionMakerT&& unpromotionMaker
    )
    {

    }

    template <typename FuncT, typename UncaptureMakerT, typename UnpromotionMakerT>
    void forEachPseudoLegalReverseMove(
        const Position& pos, 
        FuncT&& func, 
        UncaptureMakerT&& uncaptureMaker, 
        UnpromotionMakerT&& unpromotionMaker
    )
    {
        forEachPseudoLegalPawnReverseMove(
            pos, 
            std::forward<FuncT>(func), 
            std::forward<UncaptureMakerT>(uncaptureMaker), 
            std::forward<UnpromotionMakerT>(unpromotionMaker)
        );
    }

    template <typename FuncT>
    void forEachPseudoLegalReverseMove(const Position& pos, const PieceSet& startPieceSet, FuncT&& func)
    {
        auto uncaptureMaker = detail::makeUncaptureMaker(pos, startPieceSet, !pos.sideToMove());
        auto unpromotionMaker = detail::makeUnpromotionMaker(pos, startPieceSet, pos.sideToMove(), uncaptureMaker);

        forEachPseudoLegalReverseMove(pos, std::forward<FuncT>(func), uncaptureMaker, unpromotionMaker);
    }

    template <typename FuncT>
    void forEachPseudoLegalReverseMove(const Position& pos, FuncT&& func)
    {
        auto uncaptureMaker = detail::makeUncaptureMaker(!pos.sideToMove());
        auto unpromotionMaker = detail::makeUnpromotionMaker(pos.sideToMove(), uncaptureMaker);

        forEachPseudoLegalReverseMove(pos, std::forward<FuncT>(func), uncaptureMaker, unpromotionMaker);
    }
}
