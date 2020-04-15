#include "MoveIndex.h"

#include "intrin/Intrinsics.h"

namespace move_index
{
    namespace detail
    {
        // The loopup tables are interdependent and we have to generate
        // them all at the same time.
        // So we create a struct with all of them so that we can return
        // them all and assign to one static variable (initialization).

        struct AllLookupTables
        {
            EnumArray2<PieceType, Square, std::uint8_t> destinationCount;
            EnumArray2<PieceType, Square, std::array<Square, maxDestinationIndex + 1>> destinationSquareByIndex;
            EnumArray<PieceType, EnumArray2<Square, Square, std::uint8_t>> destinationIndex;
            EnumArray2<PieceType, Square, Bitboard> destinationBB;
        };

        [[nodiscard]] static AllLookupTables init()
        {
            AllLookupTables tables;

            const auto pieceTypes = { 
                PieceType::Knight, 
                PieceType::Bishop, 
                PieceType::Rook, 
                PieceType::Queen, 
                PieceType::King 
            };

            for (PieceType pt : pieceTypes)
            {
                for (Square sq : values<Square>())
                {
                    const Bitboard destinationBB = bb::attacks(pt, sq, Bitboard::none());

                    tables.destinationBB[pt][sq] = destinationBB;

                    tables.destinationCount[pt][sq] = destinationBB.count();

                    int i = 0;
                    for (Square destinationSq : destinationBB)
                    {
                        tables.destinationSquareByIndex[pt][sq][i] = destinationSq;
                        tables.destinationIndex[pt][sq][destinationSq] = i;

                        ++i;
                    }
                }
            }

            return tables;
        }

        static const AllLookupTables tables = init();
    }

    [[nodiscard]] std::uint8_t destinationCount(PieceType pt, Square from)
    {
        ASSERT(pt != PieceType::Pawn && pt != PieceType::None);
        ASSERT(from.isOk());

        return detail::tables.destinationCount[pt][from];
    }

    [[nodiscard]] Square destinationSquareByIndex(PieceType pt, Square from, std::uint8_t idx)
    {
        ASSERT(pt != PieceType::Pawn && pt != PieceType::None);
        ASSERT(from.isOk());
        ASSERT(idx < destinationCount(pt, from));

        return detail::tables.destinationSquareByIndex[pt][from][idx];
    }

    [[nodiscard]] std::uint8_t destinationIndex(PieceType pt, Square from, Square to)
    {
        ASSERT(pt != PieceType::Pawn && pt != PieceType::None);
        ASSERT(from.isOk());
        ASSERT(to.isOk());

        return detail::tables.destinationIndex[pt][from][to];
    }

    [[nodiscard]] std::uint8_t castlingDestinationIndex(Square from, Square to)
    {
        ASSERT(from.isOk());
        ASSERT(to.isOk());

        // 0 - king side, 1 - queen side
        return to.file() == fileA;
    }

    [[nodiscard]] std::uint8_t pawnDestinationIndex(Square from, Square to, Color sideToMove, PieceType promotedPieceType)
    {
        static_assert(ordinal(PieceType::Bishop) == ordinal(PieceType::Knight) + 1);
        static_assert(ordinal(PieceType::Rook) == ordinal(PieceType::Knight) + 2);
        static_assert(ordinal(PieceType::Queen) == ordinal(PieceType::Knight) + 3);

        const auto fromRank = from.rank();

        unsigned idx;
        if (sideToMove == Color::White)
        {
            // capture left - 7 - 7 = 0
            // single straight - 8 - 7 = 1
            // capture right - 9 - 7 = 2
            // double move - 16 - 7 = 9 // this is fine, we don't have to normalize it to 3
            idx = ordinal(to) - ordinal(from) - 7;
        }
        else
        {
            idx = ordinal(from) - ordinal(to) - 7;
        }

        if (promotedPieceType != PieceType::None)
        {
            ASSERT(
                (sideToMove == Color::White && fromRank == rank7)
                || (sideToMove == Color::Black && fromRank == rank2));

            idx <<= 2;
            idx += ordinal(promotedPieceType) - ordinal(PieceType::Knight);
        }

        return idx;
    }

    [[nodiscard]] Move destinationIndexToPawnMove(const Position& pos, std::uint8_t index, Square from, Color sideToMove)
    {
        static_assert(ordinal(PieceType::Bishop) == ordinal(PieceType::Knight) + 1);
        static_assert(ordinal(PieceType::Rook) == ordinal(PieceType::Knight) + 2);
        static_assert(ordinal(PieceType::Queen) == ordinal(PieceType::Knight) + 3);

        const auto fromRank = from.rank();

        if (sideToMove == Color::White)
        {
            if (fromRank == rank7)
            {
                const PieceType promotedPiece = fromOrdinal<PieceType>((index & 3) + ordinal(PieceType::Knight));
                const unsigned offset = (index >> 2) + 7;
                const Square to = fromOrdinal<Square>(ordinal(from) + offset);
                return Move::promotion(from, to, Piece(promotedPiece, Color::White));
            }
            else
            {
                const unsigned offset = index + 7;
                const Square to = fromOrdinal<Square>(ordinal(from) + offset);
                if (to == pos.epSquare())
                {
                    return Move::enPassant(from, to);
                }
                else
                {
                    return Move::normal(from, to);
                }
            }
        }
        else
        {
            if (fromRank == rank2)
            {
                const PieceType promotedPiece = fromOrdinal<PieceType>((index & 3) + ordinal(PieceType::Knight));
                const unsigned offset = (index >> 2) + 7;
                const Square to = fromOrdinal<Square>(ordinal(from) - offset);
                return Move::promotion(from, to, Piece(promotedPiece, Color::Black));
            }
            else
            {
                const unsigned offset = index + 7;
                const Square to = fromOrdinal<Square>(ordinal(from) - offset);
                if (to == pos.epSquare())
                {
                    return Move::enPassant(from, to);
                }
                else
                {
                    return Move::normal(from, to);
                }
            }
        }
    }

    [[nodiscard]] Bitboard destinationsBB(PieceType pt, Square from)
    {
        return detail::tables.destinationBB[pt][from];
    }

    [[nodiscard]] bool requiresLongMoveIndex(const Position& pos)
    {
        return pos.pieceCount(Piece(PieceType::Queen, pos.sideToMove())) > 3;
    }

    template <typename IntT, unsigned MaxV>
    [[nodiscard]] static IntT moveToIndex(const Position& pos, const Move& move)
    {
        const auto type = move.type;
        const auto from = move.from;
        const auto to = move.to;
        const auto sideToMove = pos.sideToMove();

        // 0 : king side castle
        // 1 : queen side castle
        if (type == MoveType::Castle)
        {
            return castlingDestinationIndex(from, to);
        }

        const auto fromPiece = pos.pieceAt(from);
        const auto fromPieceType = fromPiece.type();

        // find base offset for the move
        unsigned offset = maxNumCastlingMoves; // because there are two castling moves

        switch (fromPieceType)
        {
        case PieceType::Queen:

            offset += maxDestinationCount(PieceType::Rook) * pos.pieceCount(Piece(PieceType::Rook, sideToMove));

        case PieceType::Rook:

            offset += maxDestinationCount(PieceType::Bishop) * pos.pieceCount(Piece(PieceType::Bishop, sideToMove));

        case PieceType::Bishop:

            offset += maxDestinationCount(PieceType::Knight) * pos.pieceCount(Piece(PieceType::Knight, sideToMove));

        case PieceType::Knight:

            offset += maxDestinationCount(PieceType::Pawn) * pos.pieceCount(Piece(PieceType::Pawn, sideToMove));

        case PieceType::Pawn:
            // 10 to x
            offset += maxDestinationCount(PieceType::King);

        case PieceType::King:
            // 2 to 9
            break;
        }

        // Say we have N knights, then all these knights fall into the same range.
        // We have to narrow down this range to properly encode the knight that was moved.
        unsigned numPiecesBefore = (pos.piecesBB(fromPiece) & bb::before(from)).count();
        unsigned localOffset = maxDestinationCount(fromPieceType) * numPiecesBefore;
        offset += localOffset;

        // now we have to compute the destination index and add it to the offset
        if (fromPieceType == PieceType::Pawn)
        {
            offset += pawnDestinationIndex(from, to, sideToMove, move.promotedPiece.type());
        }
        else
        {
            offset += destinationIndex(fromPieceType, from, to);
        }

        ASSERT(offset <= MaxV);

        return static_cast<IntT>(offset);
    }

    [[nodiscard]] std::uint8_t moveToShortIndex(const Position& pos, const Move& move)
    {
        return moveToIndex<std::uint8_t, 255>(pos, move);
    }

    [[nodiscard]] std::uint16_t moveToLongIndex(const Position& pos, const Move& move)
    {
        return moveToIndex<std::uint16_t, 256 * 256 - 1>(pos, move);
    }

    [[nodiscard]] Move shortIndexToMove(const Position& pos, std::uint8_t index)
    {
        // currently the implementation is the same so we can do it.
        // If the spec changes we have to change this too.
        return longIndexToMove(pos, index);
    }

    namespace detail
    {
        static CastleType castleTypes[2]{ CastleType::Short, CastleType::Long };
    }

    [[nodiscard]] static Move indexToMove(const Position& pos, unsigned index)
    {
        const auto sideToMove = pos.sideToMove();

        if (index < maxNumCastlingMoves + maxNumKingMoves)
        {
            if (index < 2)
            {
                // castling move
                return Move::castle(detail::castleTypes[index], sideToMove);
            }
            else
            {
                // king move, there's always only one king
                const auto from = pos.kingSquare(sideToMove);
                const auto to = destinationSquareByIndex(PieceType::King, from, index - 2);
                return Move::normal(from, to);
            }
        }
        else
        {
            unsigned offset = maxNumCastlingMoves + maxNumKingMoves;
            unsigned nextOffset = offset + maxDestinationCount(PieceType::Pawn) * pos.pieceCount(Piece(PieceType::Pawn, sideToMove));

            // pawn move
            if (index < nextOffset)
            {
                // end of the subrange for this piece
                Bitboard pawnBB = pos.piecesBB(Piece(PieceType::Pawn, sideToMove));
                unsigned localOffset = offset;
                unsigned localNextOffset = offset;
                for (;;)
                {
                    // get the upper bound for the next subrange
                    localNextOffset += maxDestinationCount(PieceType::Pawn);
                    if (index < localNextOffset)
                    {
                        // here we know the subrange of interest
                        break;
                    }

                    // we need to move to the next subrange
                    // we also need to discard the first entry in the piece bb
                    localOffset = localNextOffset;
                    pawnBB.popFirst();
                }
                
                // now the first bit in the pieces bb is our piece
                ASSERT(pawnBB.any());

                const auto from = pawnBB.first();

                return destinationIndexToPawnMove(pos, index - localOffset, from, sideToMove);
            }

            offset = nextOffset;

            // other piece types, in the same order as in spec
            for (PieceType pt : { PieceType::Knight, PieceType::Bishop, PieceType::Rook, PieceType::Queen })
            {
                nextOffset = offset + maxDestinationCount(pt) * pos.pieceCount(Piece(pt, sideToMove));

                if (index < nextOffset)
                {
                    // end of the subrange for this piece
                    Bitboard pieceBB = pos.piecesBB(Piece(pt, sideToMove));
                    unsigned localOffset = offset;
                    unsigned localNextOffset = offset;
                    for (;;)
                    {
                        // get the upper bound for the next subrange
                        localNextOffset += maxDestinationCount(pt);
                        if (index < localNextOffset)
                        {
                            // here we know the subrange of interest
                            break;
                        }

                        // we need to move to the next subrange
                        // we also need to discard the first entry in the piece bb
                        localOffset = localNextOffset;
                        pieceBB.popFirst();
                    }

                    // now the first bit in the pieces bb is our piece
                    ASSERT(pieceBB.any());

                    const auto from = pieceBB.first();
                    const auto to = destinationSquareByIndex(pt, from, index - localOffset);

                    return Move::normal(from, to);
                }

                offset = nextOffset;
            }
        }

        // This should not be reachable.
        // If it's reached it means that the index was too high
        // and doesn't correspond to any piece.
        ASSERT(false); 
        return Move::null();
    }

    [[nodiscard]] Move longIndexToMove(const Position& pos, std::uint16_t index)
    {
        return indexToMove(pos, index);
    }
}