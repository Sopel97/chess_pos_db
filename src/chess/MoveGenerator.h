#pragma once

#include "Bitboard.h"
#include "CastlingTraits.h"
#include "Chess.h"
#include "Position.h"

#include <vector>

// TODO: iterators

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

    template <typename FuncT>
    inline void forEachPseudoLegalPawnMove(const Position& pos, Square from, FuncT&& f)
    {
        const Color sideToMove = pos.sideToMove();
        const Square epSquare = pos.epSquare();
        const Bitboard ourPieces = pos.piecesBB(sideToMove);
        const Bitboard theirPieces = pos.piecesBB(!sideToMove);
        const Bitboard occupied = ourPieces | theirPieces;

        Bitboard attackTargets = theirPieces;
        if (epSquare != Square::none())
        {
            attackTargets |= epSquare;
        }

        const Bitboard attacks = bb::pawnAttacks(Bitboard::square(from), sideToMove) & attackTargets;

        const Rank secondToLastRank = sideToMove == Color::White ? rank7 : rank2;
        const auto forward = sideToMove == Color::White ? FlatSquareOffset(0, 1) : FlatSquareOffset(0, -1);

        // promotions
        if (from.rank() == secondToLastRank)
        {
            // capture promotions
            for (Square toSq : attacks)
            {
                for (PieceType pt : { PieceType::Knight, PieceType::Bishop, PieceType::Rook, PieceType::Queen })
                {
                    Move move{ from, toSq, MoveType::Promotion, Piece(pt, sideToMove) };
                    f(move);
                }
            }

            // push promotions
            const Square toSq = from + forward;
            if (!occupied.isSet(toSq))
            {
                for (PieceType pt : { PieceType::Knight, PieceType::Bishop, PieceType::Rook, PieceType::Queen })
                {
                    Move move{ from, toSq, MoveType::Promotion, Piece(pt, sideToMove) };
                    f(move);
                }
            }
        }
        else
        {
            // captures
            for (Square toSq : attacks)
            {
                Move move{ from, toSq, (toSq == epSquare) ? MoveType::EnPassant : MoveType::Normal };
                f(move);
            }

            const Square toSq = from + forward;

            // single push
            if (!occupied.isSet(toSq))
            {
                const Rank startRank = sideToMove == Color::White ? rank2 : rank7;
                if (from.rank() == startRank)
                {
                    // double push
                    const Square toSq2 = toSq + forward;
                    if (!occupied.isSet(toSq2))
                    {
                        Move move{ from, toSq2 };
                        f(move);
                    }
                }

                Move move{ from, toSq };
                f(move);
            }
        }
    }

    template <Color SideToMoveV, typename FuncT>
    inline void forEachPseudoLegalPawnMove(const Position& pos, FuncT&& f)
    {
        const Square epSquare = pos.epSquare();
        const Bitboard ourPieces = pos.piecesBB(SideToMoveV);
        const Bitboard theirPieces = pos.piecesBB(!SideToMoveV);
        const Bitboard occupied = ourPieces | theirPieces;
        const Bitboard pawns = pos.piecesBB(Piece(PieceType::Pawn, SideToMoveV));

        const Bitboard secondToLastRank = SideToMoveV == Color::White ? bb::rank7 : bb::rank2;
        const Bitboard secondRank = SideToMoveV == Color::White ? bb::rank2 : bb::rank7;

        const auto singlePawnMoveDestinationOffset = SideToMoveV == Color::White ? FlatSquareOffset(0, 1) : FlatSquareOffset(0, -1);
        const auto doublePawnMoveDestinationOffset = SideToMoveV == Color::White ? FlatSquareOffset(0, 2) : FlatSquareOffset(0, -2);

        {
            const int backward = SideToMoveV == Color::White ? -1 : 1;
            const int backward2 = backward * 2;

            const Bitboard doublePawnMoveStarts =
                pawns
                & secondRank
                & ~(occupied.shiftedVertically(backward) | occupied.shiftedVertically(backward2));

            const Bitboard singlePawnMoveStarts =
                pawns
                & ~secondToLastRank
                & ~occupied.shiftedVertically(backward);

            for (Square from : doublePawnMoveStarts)
            {
                const Square to = from + doublePawnMoveDestinationOffset;
                f(Move::normal(from, to));
            }

            for (Square from : singlePawnMoveStarts)
            {
                const Square to = from + singlePawnMoveDestinationOffset;
                f(Move::normal(from, to));
            }
        }
        
        {
            const Bitboard lastRank = SideToMoveV == Color::White ? bb::rank8 : bb::rank1;
            const FlatSquareOffset westCaptureOffset = SideToMoveV == Color::White ? FlatSquareOffset(-1, 1) : FlatSquareOffset(-1, -1);
            const FlatSquareOffset eastCaptureOffset = SideToMoveV == Color::White ? FlatSquareOffset(1, 1) : FlatSquareOffset(1, -1);

            const Bitboard pawnsWithWestCapture = bb::eastPawnAttacks(theirPieces & ~lastRank, !SideToMoveV) & pawns;
            const Bitboard pawnsWithEastCapture = bb::westPawnAttacks(theirPieces & ~lastRank, !SideToMoveV) & pawns;

            for (Square from : pawnsWithWestCapture)
            {
                f(Move::normal(from, from + westCaptureOffset));
            }

            for (Square from : pawnsWithEastCapture)
            {
                f(Move::normal(from, from + eastCaptureOffset));
            }
        }

        if (epSquare != Square::none())
        {
            const Bitboard pawnsThatCanCapture = bb::pawnAttacks(Bitboard::square(epSquare), !SideToMoveV) & pawns;
            for (Square from : pawnsThatCanCapture)
            {
                f(Move::enPassant(from, epSquare));
            }
        }

        for (Square from : pawns & secondToLastRank)
        {
            const Bitboard attacks = bb::pawnAttacks(Bitboard::square(from), SideToMoveV) & theirPieces;

            // capture promotions
            for (Square to : attacks)
            {
                for (PieceType pt : { PieceType::Knight, PieceType::Bishop, PieceType::Rook, PieceType::Queen })
                {
                    Move move{ from, to, MoveType::Promotion, Piece(pt, SideToMoveV) };
                    f(move);
                }
            }

            // push promotions
            const Square to = from + singlePawnMoveDestinationOffset;
            if (!occupied.isSet(to))
            {
                for (PieceType pt : { PieceType::Knight, PieceType::Bishop, PieceType::Rook, PieceType::Queen })
                {
                    Move move{ from, to, MoveType::Promotion, Piece(pt, SideToMoveV) };
                    f(move);
                }
            }
        }
    }

    template <typename FuncT>
    inline void forEachPseudoLegalPawnMove(const Position& pos, FuncT&& f)
    {
        if (pos.sideToMove() == Color::White)
        {
            forEachPseudoLegalPawnMove<Color::White>(pos, std::forward<FuncT>(f));
        }
        else
        {
            forEachPseudoLegalPawnMove<Color::Black>(pos, std::forward<FuncT>(f));
        }
    }

    template <PieceType PieceTypeV, typename FuncT>
    inline void forEachPseudoLegalPieceMove(const Position& pos, Square from, FuncT&& f)
    {
        static_assert(PieceTypeV != PieceType::None);

        if constexpr (PieceTypeV == PieceType::Pawn)
        {
            forEachPseudoLegalPawnMove(pos, from, f);
        }
        else
        {
            const Color sideToMove = pos.sideToMove();
            const Bitboard ourPieces = pos.piecesBB(sideToMove);
            const Bitboard theirPieces = pos.piecesBB(!sideToMove);
            const Bitboard occupied = ourPieces | theirPieces;
            const Bitboard attacks = bb::attacks<PieceTypeV>(from, occupied) & ~ourPieces;

            for (Square toSq : attacks)
            {
                Move move{ from, toSq };
                f(move);
            }
        }
    }

    template <PieceType PieceTypeV, typename FuncT>
    inline void forEachPseudoLegalPieceMove(const Position& pos, FuncT&& f)
    {
        static_assert(PieceTypeV != PieceType::None);

        if constexpr (PieceTypeV == PieceType::Pawn)
        {
            forEachPseudoLegalPawnMove(pos, f);
        }
        else
        {
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
                    f(move);
                }
            }
        }
    }

    template <typename FuncT>
    inline void forEachCastlingMove(const Position& pos, FuncT&& f)
    {
        CastlingRights rights = pos.castlingRights();
        if (rights == CastlingRights::None)
        {
            return;
        }

        const Color sideToMove = pos.sideToMove();
        const Bitboard ourPieces = pos.piecesBB(sideToMove);
        const Bitboard theirPieces = pos.piecesBB(!sideToMove);
        const Bitboard occupied = ourPieces | theirPieces;

        // we first reduce the set of legal castlings by checking the paths for pieces
        if (sideToMove == Color::White)
        {
            if ((CastlingTraits::castlingPath[Color::White][CastleType::Short] & occupied).any()) rights &= ~CastlingRights::WhiteKingSide;
            if ((CastlingTraits::castlingPath[Color::White][CastleType::Long] & occupied).any()) rights &= ~CastlingRights::WhiteQueenSide;
            rights &= ~CastlingRights::Black;
        }
        else
        {
            if ((CastlingTraits::castlingPath[Color::Black][CastleType::Short] & occupied).any()) rights &= ~CastlingRights::BlackKingSide;
            if ((CastlingTraits::castlingPath[Color::Black][CastleType::Long] & occupied).any()) rights &= ~CastlingRights::BlackQueenSide;
            rights &= ~CastlingRights::White;
        }

        if (rights == CastlingRights::None)
        {
            return;
        }

        // King must not be in check. Done here because it is quite expensive.
        const Square ksq = pos.kingSquare(sideToMove);
        if (pos.isSquareAttacked(ksq, !sideToMove))
        {
            return;
        }

        // Loop through all possible castlings.
        for (CastleType castlingType : values<CastleType>())
        {
            const CastlingRights right = CastlingTraits::castlingRights[sideToMove][castlingType];

            if (!contains(rights, right))
            {
                continue;
            }

            // If we have this castling right
            // we check whether the king passes an attacked square.
            const Square passedSquare = CastlingTraits::squarePassedByKing[sideToMove][castlingType];
            if (pos.isSquareAttacked(passedSquare, !sideToMove))
            {
                continue;
            }

            // If it's a castling move then the change in square occupation
            // cannot have an effect because otherwise there would be
            // a slider attacker attacking the castling king.
            if (pos.isSquareAttacked(CastlingTraits::kingDestination[sideToMove][castlingType], !sideToMove))
            {
                continue;
            }

            // If not we can castle.
            Move move = Move::castle(castlingType, sideToMove);
            f(move);
        }
    }

    // Calls a given function for all pseudo legal moves for the position.
    // `pos` must be a legal chess position
    template <typename FuncT>
    inline void forEachPseudoLegalMove(const Position& pos, FuncT&& func)
    {
        forEachPseudoLegalPieceMove<PieceType::Pawn>(pos, func);
        forEachPseudoLegalPieceMove<PieceType::Knight>(pos, func);
        forEachPseudoLegalPieceMove<PieceType::Bishop>(pos, func);
        forEachPseudoLegalPieceMove<PieceType::Rook>(pos, func);
        forEachPseudoLegalPieceMove<PieceType::Queen>(pos, func);
        forEachPseudoLegalPieceMove<PieceType::King>(pos, func);
        forEachCastlingMove(pos, func);
    }

    // Calls a given function for all legal moves for the position.
    // `pos` must be a legal chess position
    template <typename FuncT>
    inline void forEachLegalMove(const Position& pos, FuncT&& func)
    {
        auto funcIfLegal = [&func, checker = pos.moveLegalityChecker()](Move move) {
            if (checker.isPseudoLegalMoveLegal(move))
            {
                func(move);
            }
        };

        forEachPseudoLegalPieceMove<PieceType::Pawn>(pos, funcIfLegal);
        forEachPseudoLegalPieceMove<PieceType::Knight>(pos, funcIfLegal);
        forEachPseudoLegalPieceMove<PieceType::Bishop>(pos, funcIfLegal);
        forEachPseudoLegalPieceMove<PieceType::Rook>(pos, funcIfLegal);
        forEachPseudoLegalPieceMove<PieceType::Queen>(pos, funcIfLegal);
        forEachPseudoLegalPieceMove<PieceType::King>(pos, funcIfLegal);
        forEachCastlingMove(pos, func);
    }

    // Generates all pseudo legal moves for the position.
    // `pos` must be a legal chess position
    [[nodiscard]] std::vector<Move> generatePseudoLegalMoves(const Position& pos);

    // Generates all legal moves for the position.
    // `pos` must be a legal chess position
    [[nodiscard]] std::vector<Move> generateLegalMoves(const Position& pos);
}
