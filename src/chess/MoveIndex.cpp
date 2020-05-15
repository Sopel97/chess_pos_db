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
                (sideToMove == Color::White && from.rank() == rank7)
                || (sideToMove == Color::Black && from.rank() == rank2));

            idx <<= 2;
            idx += ordinal(promotedPieceType) - ordinal(PieceType::Knight);
        }

        return idx;
    }

    [[nodiscard]] Move destinationIndexToPawnMove(const Square& epSquare, std::uint8_t index, Square from, Color sideToMove)
    {
        static_assert(ordinal(PieceType::Bishop) == ordinal(PieceType::Knight) + 1);
        static_assert(ordinal(PieceType::Rook) == ordinal(PieceType::Knight) + 2);
        static_assert(ordinal(PieceType::Queen) == ordinal(PieceType::Knight) + 3);

        const auto fromRank =
            sideToMove == Color::White
            ? from.rank()
            : fromOrdinal<Rank>(rank8 - from.rank());

        Piece promotedPiece = Piece::none();
        MoveType type;
        if (fromRank == rank7)
        {
            const PieceType promotedPieceType = fromOrdinal<PieceType>((index & 3) + ordinal(PieceType::Knight));
            promotedPiece = Piece(promotedPieceType, sideToMove);
            index >>= 2;
            type = MoveType::Promotion;
        }

        int offset = index + 7;
        if (sideToMove == Color::Black) offset = -offset;
        const Square to = fromOrdinal<Square>(ordinal(from) + offset);

        if (fromRank != rank7)
        {
            type =
                to == epSquare
                ? MoveType::EnPassant
                : MoveType::Normal;
        }

        return Move{ from, to, type, promotedPiece };
    }

    [[nodiscard]] Move destinationIndexToPawnMove(const Position& pos, std::uint8_t index, Square from, Color sideToMove)
    {
        return destinationIndexToPawnMove(pos.epSquare(), index, from, sideToMove);
    }

    [[nodiscard]] Bitboard destinationsBB(PieceType pt, Square from)
    {
        return detail::tables.destinationBB[pt][from];
    }

    [[nodiscard]] bool requiresLongMoveIndex(const Position& pos)
    {
        // 2 + 8*2 + 13*2 + 14*(2+8-N) + 27*(1+N) + 8 == 219 + 13N
        // For N = 2 it is < 256, for N = 3 it is >= 256
        return pos.pieceCount(Piece(PieceType::Queen, pos.sideToMove())) > 2;
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

    namespace detail
    {
        template <PieceType Pt>
        FORCEINLINE static bool indexToMoveForPieceType(const Position& pos, unsigned index, Square& from, Square& to, unsigned& offset)
        {
            const Piece piece = Piece(Pt, pos.sideToMove());
            const unsigned nextOffset = offset + maxDestinationCount(Pt) * pos.pieceCount(piece);

            if (index < nextOffset)
            {
                // end of the subrange for this piece
                const unsigned localOffset = index - offset;
                const unsigned n = localOffset / maxDestinationCount(Pt);
                Bitboard pieceBB = pos.piecesBB(piece);
                for (unsigned i = 0; i < n; ++i)
                {
                    pieceBB.popFirst();
                }

                // now the first bit in the pieces bb is our piece
                ASSERT(pieceBB.any());

                from = pieceBB.first();
                to = destinationSquareByIndex(Pt, from, localOffset - n * maxDestinationCount(Pt));

                return true;
            }

            offset = nextOffset;

            return false;
        }
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

            {
                const Piece piece = Piece(PieceType::Pawn, sideToMove);
                const unsigned nextOffset = offset + maxDestinationCount(PieceType::Pawn) * pos.pieceCount(piece);

                // pawn move
                if (index < nextOffset)
                {
                    const unsigned localOffset = index - offset;
                    const unsigned n = localOffset / maxDestinationCount(PieceType::Pawn);
                    Bitboard pieceBB = pos.piecesBB(piece);
                    for (unsigned i = 0; i < n; ++i)
                    {
                        pieceBB.popFirst();
                    }

                    // now the first bit in the pieces bb is our piece
                    ASSERT(pieceBB.any());

                    const Square from = pieceBB.first();
                    return destinationIndexToPawnMove(
                        pos, 
                        localOffset - n * maxDestinationCount(PieceType::Pawn), 
                        from, 
                        sideToMove
                        );
                }

                offset = nextOffset;
            }

            // other piece types, in the same order as in spec
            // Manually unrolled.
            Move move{};
            if (detail::indexToMoveForPieceType<PieceType::Knight>(
                pos, index, move.from, move.to, offset)) return move;

            if (detail::indexToMoveForPieceType<PieceType::Bishop>(
                pos, index, move.from, move.to, offset)) return move;

            if (detail::indexToMoveForPieceType<PieceType::Rook>(
                pos, index, move.from, move.to, offset)) return move;

            if (detail::indexToMoveForPieceType<PieceType::Queen>(
                pos, index, move.from, move.to, offset)) return move;
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