#pragma once

#include "Bitboard.h"
#include "CastlingTraits.h"
#include "Chess.h"
#include "Position.h"

#include "data_structure/FixedVector.h"

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

        [[nodiscard]] FixedVector<Piece, 6> uncaptures(const PieceSet& current, Color capturedPieceColor, Color squareColor) const
        {
            const auto mask = uncapturesWithRemaining(current);

            FixedVector<Piece, 6> pieces;

            pieces.emplace_back(Piece::none());

            if (mask.pawn) pieces.emplace_back(Piece(PieceType::Pawn, capturedPieceColor));
            if (mask.knight) pieces.emplace_back(Piece(PieceType::Knight, capturedPieceColor));

            if (
                (mask.lightSquareBishop && squareColor == Color::White)
                || (mask.darkSquareBishop && squareColor == Color::Black)
                ) 
                pieces.emplace_back(Piece(PieceType::Bishop, capturedPieceColor));

            if (mask.rook) pieces.emplace_back(Piece(PieceType::Rook, capturedPieceColor));
            if (mask.queen) pieces.emplace_back(Piece(PieceType::Queen, capturedPieceColor));

            return pieces;
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

        [[nodiscard]] EnumArray<PieceType, bool> unpromotions(const PieceSet& current, Color squareColor) const
        {
            const auto mask = unpromotionsWithRemaining(current);

            EnumArray<PieceType, bool> validUnpromotions;
            for (auto& p : validUnpromotions) p = false;

            if (mask.knight) validUnpromotions[PieceType::Knight] = true;

            if (
                (mask.lightSquareBishop && squareColor == Color::White)
                || (mask.darkSquareBishop && squareColor == Color::Black)
                )
                validUnpromotions[PieceType::Bishop] = true;

            if (mask.rook) validUnpromotions[PieceType::Rook] = true;
            if (mask.queen) validUnpromotions[PieceType::Queen] = true;

            return validUnpromotions;
        }
    };

    namespace detail
    {
        struct CandidateEpSquares
        {
            Bitboard ifNoUncapture;
            Bitboard ifPawnUncapture;
            Bitboard ifOtherUncapture;

            [[nodiscard]] Bitboard forUncapture(Piece piece) const
            {
                if (piece.type() == PieceType::None)
                {
                    return ifNoUncapture;
                }

                if (piece.type() == PieceType::Pawn)
                {
                    return ifPawnUncapture;
                }

                return ifOtherUncapture;
            }
        };

        [[nodiscard]] CandidateEpSquares candidateEpSquaresForReverseMove(
            const Board& board, 
            Color sideToDoEp, 
            const Move& rm
        )
        {
            const Piece movedPiece = board.pieceAt(rm.to);
            Bitboard ourPawns = board.piecesBB(Piece(PieceType::Pawn, sideToDoEp));
            Bitboard theirPawns = board.piecesBB(Piece(PieceType::Pawn, !sideToDoEp));
            Bitboard pieces = board.piecesBB();

            const Bitboard fromto = Bitboard::square(rm.from) ^ rm.to;
            pieces ^= fromto;

            if (movedPiece.type() == PieceType::Pawn)
            {
                // We have to update our pawns locations
                ourPawns ^= fromto;

                if (rm.type == MoveType::EnPassant)
                {
                    // If it's an en-passant then this must be the only possible
                    // epSquare if we want to reverse the move.
                    // That's because there can be only one epSquare for a given
                    // position.

                    const Bitboard epSquare = Bitboard::square(Square(rm.to.file(), rm.from.rank()));
                    return { epSquare, epSquare, epSquare };
                }
            }

            CandidateEpSquares candidateEpSquares{ Bitboard::none(), Bitboard::none(), Bitboard::none() };

            if (sideToDoEp == Color::White)
            {
                // Case 1. no uncapture
                candidateEpSquares.ifNoUncapture = (
                    theirPawns & bb::rank6
                    & ~(pieces.shiftedVertically(-1) | pieces.shiftedVertically(-2))
                    ).shiftedVertically(1);

                // Case 2. other uncapture
                //         In this case the uncapture may place a piece on the double push path
                //         making impossible to have done double push earlier and
                //         so the en-passant to was not possible instead of that capture.
                pieces ^= rm.to;
                const Bitboard unobstructed = ~(pieces.shiftedVertically(-1) | pieces.shiftedVertically(-2));
                candidateEpSquares.ifOtherUncapture = (
                    theirPawns & bb::rank6
                    & unobstructed
                    ).shiftedVertically(1);

                // Case 3. a pawn uncapture
                //         In this case we may have added a candidate to capture.
                //         We may have also inhibited some ep.
                //         Consider B.      p.
                //                  ..  ->  .B
                //                  pP      pP
                //         This is a generalization of Case 2. The `pieces` is already
                //         updated there.
                candidateEpSquares.ifPawnUncapture = (
                    (theirPawns | rm.to) & bb::rank6
                    & unobstructed
                    ).shiftedVertically(1);
            }
            else
            {
                // Case 1.
                candidateEpSquares.ifNoUncapture = (
                    theirPawns & bb::rank3
                    & ~(pieces.shiftedVertically(1) | pieces.shiftedVertically(2))
                    ).shiftedVertically(-1);

                // Case 2.
                pieces ^= rm.to;
                const Bitboard unobstructed = ~(pieces.shiftedVertically(1) | pieces.shiftedVertically(2));
                candidateEpSquares.ifOtherUncapture = (
                    theirPawns & bb::rank3
                    & unobstructed
                    ).shiftedVertically(-1);

                // Case 3.
                candidateEpSquares.ifPawnUncapture = (
                    (theirPawns | rm.to) & bb::rank3
                    & unobstructed
                    ).shiftedVertically(-1);
            }

            // We only consider candidate ep squares that are attacked by our pawns.
            // Otherwise nothing could execute the en-passant so the flag
            // cannot be set.
            const Bitboard ourPawnAttacks = bb::pawnAttacks(ourPawns, sideToDoEp);
            candidateEpSquares.ifNoUncapture &= ourPawnAttacks;
            candidateEpSquares.ifPawnUncapture &= ourPawnAttacks;
            candidateEpSquares.ifOtherUncapture &= ourPawnAttacks;

            return candidateEpSquares;
        }

        struct CastlingRightsByUncapture
        {
            CastlingRights ifNotRookUncapture;
            CastlingRights ifRookUncapture;
        };

        [[nodiscard]] CastlingRightsByUncapture updateCastlingRightsForReverseMove(
            CastlingRights minCastlingRights,
            const Board& board,
            Color sideToUnmove,
            const Move& rm
            )
        {
            const Piece ourRook = Piece(PieceType::Rook, sideToUnmove);
            const Piece ourKing = Piece(PieceType::King, sideToUnmove);


            if (rm.type == MoveType::Castle)
            {
                // We only have to consider adding the castling right used
                // and possibly for the other castle type. No need
                // to add anything for the opponent, and since we
                // cannot capture when castling both returned rights are the same.
                const CastleType castleType = CastlingTraits::moveCastlingType(rm);
                const CastlingRights requiredCastlingRight = CastlingTraits::castlingRights[sideToUnmove][castleType];
                CastlingRights castlingRights = minCastlingRights | requiredCastlingRight;

                const Square otherRookSq = CastlingTraits::rookStart[sideToUnmove][!castleType];
                if (board.pieceAt(otherRookSq) == ourRook)
                {
                    castlingRights |= CastlingTraits::castlingRights[sideToUnmove][!castleType];;
                }

                return { castlingRights, castlingRights };
            }

            // This is only reached if not a castling move.

            CastlingRights castlingRightsIfNotRookCapture = minCastlingRights;
            const Piece movedPiece = board.pieceAt(rm.to);
            if (movedPiece.type() == PieceType::King)
            {
                // If we move our king back to its starting position
                // then we need to add castling rights for each rook still
                // on its starting square.
                if (rm.from == CastlingTraits::kingStart[sideToUnmove])
                {
                    const Square shortRookSq = CastlingTraits::rookStart[sideToUnmove][CastleType::Short];
                    const Square longRookSq = CastlingTraits::rookStart[sideToUnmove][CastleType::Long];

                    if (board.pieceAt(shortRookSq) == ourRook)
                    {
                        castlingRightsIfNotRookCapture |= CastlingTraits::castlingRights[sideToUnmove][CastleType::Short];
                    }

                    if (board.pieceAt(longRookSq) == ourRook)
                    {
                        castlingRightsIfNotRookCapture |= CastlingTraits::castlingRights[sideToUnmove][CastleType::Long];
                    }
                }
            }
            else if (movedPiece.type() == PieceType::Rook)
            {
                // If we move a rook we only have to add its castling rights
                // if we move to its starting square and the king is at the
                // starting square.
                if (board.pieceAt(CastlingTraits::kingStart[sideToUnmove]) == ourKing)
                {
                    const Square shortRookSq = CastlingTraits::rookStart[sideToUnmove][CastleType::Short];
                    const Square longRookSq = CastlingTraits::rookStart[sideToUnmove][CastleType::Long];

                    if (rm.from == shortRookSq)
                    {
                        castlingRightsIfNotRookCapture |= CastlingTraits::castlingRights[sideToUnmove][CastleType::Short];
                    }
                    else if (rm.from == longRookSq)
                    {
                        castlingRightsIfNotRookCapture |= CastlingTraits::castlingRights[sideToUnmove][CastleType::Short];
                    }
                }
            }

            CastlingRights castlingRightsIfRookCapture = castlingRightsIfNotRookCapture;
            {
                // Now we're interested in possible uncaptures of an opponent's rook.
                // We can only add castling rights if the king is at the start place.
                const Color opponentSide = !sideToUnmove;
                const Piece theirKing = Piece(PieceType::King, opponentSide);
                if (board.pieceAt(CastlingTraits::kingStart[opponentSide]) == theirKing)
                {
                    const Piece theirRook = Piece(PieceType::Rook, opponentSide);

                    const Square shortRookSq = CastlingTraits::rookStart[opponentSide][CastleType::Short];
                    const Square longRookSq = CastlingTraits::rookStart[opponentSide][CastleType::Long];

                    if (rm.to == shortRookSq)
                    {
                        castlingRightsIfRookCapture |= CastlingTraits::castlingRights[opponentSide][CastleType::Short];
                    }
                    else if (rm.to == longRookSq)
                    {
                        castlingRightsIfRookCapture |= CastlingTraits::castlingRights[opponentSide][CastleType::Short];
                    }
                }
            }

            return { castlingRightsIfNotRookCapture, castlingRightsIfRookCapture };
        }

        [[nodiscard]] FixedVector<CastlingRights, 16> allCastlingRightsBetween(
            CastlingRights min,
            CastlingRights max
        )
        {
            const unsigned minInt = static_cast<unsigned>(min);
            const unsigned maxInt = static_cast<unsigned>(max);
            const unsigned mask = minInt ^ maxInt;

            FixedVector<CastlingRights, 16> set;

            unsigned maskSubset = 0;
            do
            {
                // We generate all subsets of the difference between min and max
                // and xor them with min to get all castling rights between min and max.
                // masked increment
                // https://stackoverflow.com/questions/44767080/incrementing-masked-bitsets
                maskSubset = ((maskSubset | ~mask) + 1) & mask;
                set.emplace_back(static_cast<CastlingRights>(minInt ^ maskSubset));
            } while (maskSubset);

            return set;
        }

        // This creates a function object that finds all
        // possible permutations of capturedPiece, oldEpSquare,
        // oldCastlingRights and emits all those moves.
        // oldCastlingRights options may depend on the uncaptured piece.
        //   - some reverse moves may add castling rights.
        // capturedPiece options are always the same regardless of the other.
        // oldEpSquare options for a given reverse move may differ depending
        // on the capturedPiece. That is because the capturedPiece
        // may have been attacking our king in which case no en-passant 
        // is possible (unless the pawn becomes a blocker).
        // We at least know that at the beginning our king is not attacked.
        // So it's enough to check which placements of
        // which opponent piece as capturedPiece would
        // result in an attack on our king and which
        // pieces are important blockers 
        // (the blockers don't change unless the reverse
        //  move is not a capture)
        // - and then when generating actual oldEpSquares we either:
        // 1. if our king was not in check then generate all oldEpSquares.
        // 2. if our king was in check then we know the offending piece
        //    and we can verify whether the en-passant pawn
        //    would become a blocker.
        // 
        // Additionally we cannot set oldEpSquare if after
        // doing the en-passant our king would end up in check.
        // This has to be checked always.
        // We can specialize for two cases:
        // 1. if our king is not on the same rank as the ep pawns
        //    we can use blockersForKing to determine if the king
        //    ends up in a discovered check. (But even if the pawn
        //    was a blocker we may have added a new blocker so we have
        //    to check further.)
        // 2. if our king is on the same rank as ep pawns then it's
        //    best to just undo the move, do the ep (not literraly,
        //    just simulate piece placements), and check if our 
        //    king ends up in check.

        // This builds a function object for the given position that
        // checks whether given a `undoMove`, `epSquare`, and `uncapturedPiece`
        // if we undid the `undoMove` and uncaptured `uncapturedPiece` would the 
        // `epSquare` be a valid en-passant target.
        [[nodiscard]] auto makeTimeTravelEpSquareValidityChecker(const Position& pos)
        {
            return [] (const Move& undoMove, Square epSquare, Piece uncapturedPiece)
            {
                // TODO: this
                return true;
            };
        }

        template <typename FuncT>
        struct Permutator
        {
            const Position& pos;

            FixedVector<Piece, 6> lightSquareUncaptures;
            FixedVector<Piece, 6> darkSquareUncaptures;
            EnumArray<PieceType, bool> isValidLightSquareUnpromotion;
            EnumArray<PieceType, bool> isValidDarkSquareUnpromotion;

            FuncT&& func;
            decltype(makeTimeTravelEpSquareValidityChecker(pos)) isTimeTravelEpSquareValid;

            [[nodiscard]] bool canUncapturePawn() const
            {
                for (Piece p : lightSquareUncaptures)
                {
                    if (p.type() == PieceType::Pawn)
                    {
                        return true;
                    }
                }

                for (Piece p : darkSquareUncaptures)
                {
                    if (p.type() == PieceType::Pawn)
                    {
                        return true;
                    }
                }

                return false;
            }

            void emitPermutations(const Move& move) const
            {
                const Color sideToUnmove = !pos.sideToMove();
                const CastlingRights minCastlingRights = pos.castlingRights();

                // Some reverse moves (pawn reverse moves) may add additional
                // possible oldEpSquares.
                const CandidateEpSquares candidateOldEpSquares =
                    candidateEpSquaresForReverseMove(pos, sideToUnmove, move);

                // When going back in time we may have an option to include more castling rights.
                // Note that the castling rights cannot be removed when we go back.
                // Also.
                // We have to handle two cases. Either this reverse moves uncaptures a rook
                // or not. If it uncaptures a rook and it happens that this rook was
                // on it's starting position then it may add additional castling rights.
                // Otherwise possibleOldCastlingRightsSetIfNotRookUncapture
                //           == possibleOldCastlingRightsSetIfRookUncapture.
                const CastlingRightsByUncapture possibleOldCastlingRights
                    = updateCastlingRightsForReverseMove(minCastlingRights, pos, sideToUnmove, move);

                // At this stage we generate all different castling rights that may be possible.
                // Specific castling rights are not dependent on each other so it makes it
                // always a power of two sized set of different castling rights.
                const auto possibleOldCastlingRightsSetIfNotRookUncapture
                    = allCastlingRightsBetween(minCastlingRights, possibleOldCastlingRights.ifNotRookUncapture);

                const auto possibleOldCastlingRightsSetIfRookUncapture
                    = allCastlingRightsBetween(minCastlingRights, possibleOldCastlingRights.ifRookUncapture);

                const Piece movedPiece = pos.pieceAt(move.to);

                const bool isPawnCapture =
                    movedPiece.type() == PieceType::Pawn
                    && move.from.file() != move.to.file();

                const bool isPawnPush =
                    movedPiece.type() == PieceType::Pawn
                    && move.from.file() == move.to.file();

                // Castlings and pawn pushes are excluded.
                // We also have to exclude en-passants.
                const bool mayHaveBeenCapture =
                    move.type != MoveType::EnPassant
                    && move.type != MoveType::Castle
                    && !isPawnPush;

                const auto& uncaptures = 
                    move.to.color() == Color::White 
                    ? lightSquareUncaptures 
                    : darkSquareUncaptures;

                ReverseMove rm{};
                rm.move = move;
                if (mayHaveBeenCapture)
                {
                    // Not all squares allow a pawn uncapture.
                    const bool canBePawnUncapture = !((bb::rank1 | bb::rank8).isSet(move.to));

                    for (Piece uncapture : uncaptures)
                    {
                        if (isPawnCapture && uncapture.type() == PieceType::None)
                        {
                            // Pawns must capture
                            continue;
                        }

                        if (!canBePawnUncapture && uncapture.type() == PieceType::Pawn)
                        {
                            continue;
                        }

                        const auto actualCandidateOldEpSquares = candidateOldEpSquares.forUncapture(uncapture);

                        const auto& oldCastlingRightsSet =
                            uncapture.type() == PieceType::Rook
                            ? possibleOldCastlingRightsSetIfNotRookUncapture
                            : possibleOldCastlingRightsSetIfRookUncapture;

                        rm.capturedPiece = uncapture;
                        for (Square candidateOldEpSquare : actualCandidateOldEpSquares)
                        {
                            if (!isTimeTravelEpSquareValid(rm.move, candidateOldEpSquare, uncapture))
                            {
                                continue;
                            }

                            rm.oldEpSquare = candidateOldEpSquare;
                            for (CastlingRights oldCastlingRights : oldCastlingRightsSet)
                            {
                                rm.oldCastlingRights = oldCastlingRights;
                                func(rm);
                            }
                        }

                        // There's always the possibility that there was no epSquare.

                        rm.oldEpSquare = Square::none();
                        for (CastlingRights oldCastlingRights : oldCastlingRightsSet)
                        {
                            rm.oldCastlingRights = oldCastlingRights;
                            func(rm);
                        }
                    }
                }
                else
                {
                    rm.capturedPiece = Piece::none();

                    // In case of an en-passant there is only one 
                    // possible oldEpSquare, because it was used.
                    // No need to check anything.
                    const auto& oldCastlingRightsSet =
                        possibleOldCastlingRightsSetIfNotRookUncapture;

                    if (move.type == MoveType::EnPassant)
                    {
                        rm.oldEpSquare = candidateOldEpSquares.ifNoUncapture.first();

                        for (CastlingRights oldCastlingRights : oldCastlingRightsSet)
                        {
                            rm.oldCastlingRights = oldCastlingRights;
                            func(rm);
                        }
                    }
                    else
                    {
                        for (Square candidateOldEpSquare : candidateOldEpSquares.ifNoUncapture)
                        {
                            if (!isTimeTravelEpSquareValid(rm.move, candidateOldEpSquare, Piece::none()))
                            {
                                continue;
                            }

                            rm.oldEpSquare = candidateOldEpSquare;
                            for (CastlingRights oldCastlingRights : oldCastlingRightsSet)
                            {
                                rm.oldCastlingRights = oldCastlingRights;
                                func(rm);
                            }
                        }

                        // If we're reversing a non en-passant move then it's possible
                        // that there was no epSquare set before the move.
                        rm.oldEpSquare = Square::none();
                        for (CastlingRights oldCastlingRights : oldCastlingRightsSet)
                        {
                            rm.oldCastlingRights = oldCastlingRights;
                            func(rm);
                        }
                    }
                }
            }
        };

        template <typename FuncT>
        [[nodiscard]] Permutator<FuncT> makeReverseMovePermutator(
            const Position& pos, 
            const FixedVector<Piece, 6>& lightSquareUncaptures,
            const FixedVector<Piece, 6>& darkSquareUncaptures,
            EnumArray<PieceType, bool> isValidLightSquareUnpromotion,
            EnumArray<PieceType, bool> isValidDarkSquareUnpromotion,
            FuncT&& func
        )
        {
            return Permutator<FuncT>{
                pos,

                lightSquareUncaptures,
                darkSquareUncaptures,
                isValidLightSquareUnpromotion,
                isValidDarkSquareUnpromotion,

                std::forward<FuncT>(func),
                makeTimeTravelEpSquareValidityChecker(pos)
            };
        }

        template <typename FuncT>
        [[nodiscard]] Permutator<FuncT> makeReverseMovePermutator(const Position& pos, const PieceSet& startPieceSet, FuncT&& func)
        {
            const PieceSet sideToMovePieceSet = PieceSet(pos, pos.sideToMove());
            const PieceSet sideToUnmovePieceSet = PieceSet(pos, !pos.sideToMove());

            const auto lightSquareUncaptures = startPieceSet.uncaptures(sideToMovePieceSet, pos.sideToMove(), Color::White);
            const auto darkSquareUncaptures = startPieceSet.uncaptures(sideToMovePieceSet, pos.sideToMove(), Color::Black);

            const auto isLightSquareUnpromotionValid = startPieceSet.unpromotions(sideToUnmovePieceSet, Color::White);
            const auto isDarkSquareUnpromotionValid = startPieceSet.unpromotions(sideToUnmovePieceSet, Color::Black);

            return makeReverseMovePermutator(
                pos,
                lightSquareUncaptures,
                darkSquareUncaptures,
                isLightSquareUnpromotionValid,
                isDarkSquareUnpromotionValid,
                std::forward<FuncT>(func)
            );
        }

        template <typename FuncT>
        [[nodiscard]] Permutator<FuncT> makeReverseMovePermutator(const Position& pos, FuncT&& func)
        {
            const auto lightSquareUncaptures = PieceSetMask::allUncaptures();
            const auto darkSquareUncaptures = PieceSetMask::allUncaptures();

            const auto isLightSquareUnpromotionValid = PieceSetMask::allUnpromotions();
            const auto isDarkSquareUnpromotionValid = PieceSetMask::allUnpromotions();

            return makeReverseMovePermutator(
                pos,
                lightSquareUncaptures,
                darkSquareUncaptures,
                isLightSquareUnpromotionValid,
                isDarkSquareUnpromotionValid,
                std::forward<FuncT>(func)
            );
        }
    }

    template <typename FuncT>
    void forEachPseudoLegalPawnNormalReverseMove(
        const Position& pos, 
        FuncT&& func
    )
    {
        constexpr Bitboard whiteSingleUnpushPawnsMask =
            bb::rank3 | bb::rank4 | bb::rank5 | bb::rank6 | bb::rank7;

        constexpr Bitboard blackSingleUnpushPawnsMask =
            bb::rank2 | bb::rank3 | bb::rank4 | bb::rank5 | bb::rank6;

        constexpr Bitboard whiteDoubleUnpushPawnsMask = bb::rank4;
        constexpr Bitboard blackDoubleUnpushPawnsMask = bb::rank5;

        const Color sideToUnmove = !pos.sideToMove();

        const int forward =
            sideToUnmove == Color::White
            ? 1
            : -1;

        // pushes

        const FlatSquareOffset singlePawnUnpush = FlatSquareOffset(0, -forward);
        const FlatSquareOffset doublePawnUnpush = FlatSquareOffset(0, -forward * 2);

        const Bitboard singleUnpushPawnsMask =
            sideToUnmove == Color::White
            ? whiteSingleUnpushPawnsMask
            : blackSingleUnpushPawnsMask;

        const Bitboard doubleUnpushPawnsMask =
            sideToUnmove == Color::White
            ? whiteDoubleUnpushPawnsMask
            : blackDoubleUnpushPawnsMask;

        const Bitboard pieces = pos.piecesBB();
        const Bitboard pawns = pos.piecesBB(Piece(PieceType::Pawn, sideToUnmove));

        const Bitboard singleUnpushablePawns = 
            pawns 
            & ~pieces.shiftedVertically(forward) 
            & singleUnpushPawnsMask;

        const Bitboard doubleUnpushablePawns = 
            singleUnpushablePawns 
            & ~pieces.shiftedVertically(forward * 2) 
            & doubleUnpushPawnsMask;

        for (Square to : singleUnpushablePawns)
        {
            const Square from = to + singlePawnUnpush;
            const Move move = Move::normal(from, to);
            func(move);
        }

        for (Square to : doubleUnpushablePawns)
        {
            const Square from = to + doublePawnUnpush;
            const Move move = Move::normal(from, to);
            func(move);
        }

        // captures

        const Offset eastCapture = Offset(1, forward);
        const Offset westCapture = Offset(-1, forward);
        const FlatSquareOffset eastUncapture = (-eastCapture).flat();
        const FlatSquareOffset westUncapture = (-westCapture).flat();

        const Bitboard pawnsThatMayHaveCaptured =
            pawns
            & singleUnpushPawnsMask;

        const Bitboard pawnsThatMayHaveCapturedEast =
            pawnsThatMayHaveCaptured
            & ~pieces.shifted(eastCapture);

        const Bitboard pawnsThatMayHaveCapturedWest =
            pawnsThatMayHaveCaptured
            & ~pieces.shifted(westCapture);

        for (Square to : pawnsThatMayHaveCapturedEast)
        {
            const Square from = to + eastUncapture;
            const Move move = Move::normal(from, to);
            func(move);
        }

        for (Square to : pawnsThatMayHaveCapturedWest)
        {
            const Square from = to + westUncapture;
            const Move move = Move::normal(from, to);
            func(move);
        }
    }

    template <typename FuncT>
    void forEachPseudoLegalPawnPromotionReverseMove(
        const Position& pos,
        const EnumArray<PieceType, bool>& isValidLightSquareUnpromotion,
        const EnumArray<PieceType, bool>& isValidDarkSquareUnpromotion,
        FuncT&& func
    )
    {
        const Color sideToUnmove = !pos.sideToMove();

        const int forward =
            sideToUnmove == Color::White
            ? 1
            : -1;

        const FlatSquareOffset singlePawnUnpush = FlatSquareOffset(0, -forward);

        const Bitboard promotionRank =
            sideToUnmove == Color::White
            ? bb::rank8
            : bb::rank1;

        const Bitboard piecesOnPromotionRank = pos.piecesBB(sideToUnmove) & promotionRank;
        Bitboard promotionTargets = Bitboard::none();
        for (Square sq : piecesOnPromotionRank)
        {
            const Color sqColor = sq.color();
            const PieceType pt = pos.pieceAt(sq).type();
            if (
                (sqColor == Color::White && isValidLightSquareUnpromotion[pt])
                || (sqColor == Color::Black && isValidDarkSquareUnpromotion[pt])
                )
            {
                promotionTargets |= sq;
            }
        }

        const Bitboard pieces = pos.piecesBB();

        // pushes

        const Bitboard pushUnpromotions =
            promotionTargets
            & ~pieces.shiftedVertically(forward);

        for (Square to : pushUnpromotions)
        {
            const Piece piece = pos.pieceAt(to);
            const Square from = to + singlePawnUnpush;
            const Move move = Move::promotion(from, to, piece);
            func(move);
        }

        // captures

        const Offset eastCapture = Offset(1, forward);
        const Offset westCapture = Offset(-1, forward);
        const FlatSquareOffset eastUncapture = (-eastCapture).flat();
        const FlatSquareOffset westUncapture = (-westCapture).flat();

        const Bitboard eastCapturePromotions =
            promotionTargets
            & ~pieces.shifted(eastCapture);

        const Bitboard westCapturePromotions =
            promotionTargets
            & ~pieces.shifted(westCapture);

        for (Square to : eastCapturePromotions)
        {
            const Piece piece = pos.pieceAt(to);
            const Square from = to + eastUncapture;
            const Move move = Move::promotion(from, to, piece);
            func(move);
        }

        for (Square to : westCapturePromotions)
        {
            const Piece piece = pos.pieceAt(to);
            const Square from = to + westUncapture;
            const Move move = Move::promotion(from, to, piece);
            func(move);
        }
    }

    template <typename FuncT>
    void forEachPseudoLegalPawnEnPassantReverseMove(
        const Position& pos,
        FuncT&& func
    )
    {

    }

    template <typename FuncT>
    void forEachPseudoLegalPawnReverseMove(detail::Permutator<FuncT>& permutator)
    {
        auto fwd = [&permutator](const Move& m) { permutator.emitPermutations(m); };

        forEachPseudoLegalPawnNormalReverseMove(
            permutator.pos,
            fwd
        );

        forEachPseudoLegalPawnPromotionReverseMove(
            permutator.pos,
            permutator.isValidLightSquareUnpromotion,
            permutator.isValidDarkSquareUnpromotion,
            fwd
        );

        if (permutator.canUncapturePawn())
        {
            forEachPseudoLegalPawnEnPassantReverseMove(
                permutator.pos,
                fwd
            );
        }
    }

    template <typename FuncT>
    void forEachPseudoLegalReverseMove(detail::Permutator<FuncT>& permutator)
    {
        forEachPseudoLegalPawnReverseMove(permutator);
    }

    template <typename FuncT>
    void forEachPseudoLegalReverseMove(const Position& pos, const PieceSet& startPieceSet, FuncT&& func)
    {
        auto permutator = detail::makeReverseMovePermutator(pos, startPieceSet, std::forward<FuncT>(func));

        forEachPseudoLegalReverseMove(permutator);
    }

    template <typename FuncT>
    void forEachPseudoLegalReverseMove(const Position& pos, FuncT&& func)
    {
        auto permutator = detail::makeReverseMovePermutator(pos, std::forward<FuncT>(func));

        forEachPseudoLegalReverseMove(permutator);
    }
}
