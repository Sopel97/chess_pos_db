#pragma once

#include <array>
#include <cstdint>

#include "Chess.h"
#include "Bitboard.h"
#include "Position.h"

#include "enum/EnumArray.h"

namespace move_index
{
    /*
    #include <iostream>
    #include <cstdlib>
    #include "chess/San.h"
    #include "chess/MoveGenerator.h"

    void fuzzTestMoveIndex(int iterations)
    {
        // Last test:
        //     Iterations: 100000
        //     Moves: 35817734

        int movecount = 0;
        for (int i = 0; i < iterations; ++i)
        {
            auto pos = Position::startPosition();
            int movecountInThisGame = 0;
            for (;;)
            {
                if (movecountInThisGame > 400) break;

                const auto moves = movegen::generateLegalMoves(pos);
                if (moves.empty()) break;

                ++movecountInThisGame;
                const auto move = moves[rand() % moves.size()];
                const auto idx = move_index::moveToShortIndex(pos, move);
                const auto decoded = move_index::shortIndexToMove(pos, idx);
                if (move != decoded)
                {
                    std::cout << "Incorrect result:\n";
                    std::cout << pos.fen() << '\n';
                    std::cout << san::moveToSan<san::SanSpec::None>(pos, move) << '\n';
                    std::cout << "Index: " << (int)idx << '\n';
                    std::cout << "Decoded: " << ordinal(move.from) << ' ' << ordinal(move.to) << ' ' << ordinal(move.type) << ' ' << ordinal(move.promotedPiece) << '\n';
                    return;
                }

                pos.doMove(move);
            }

            movecount += movecountInThisGame;
        }

        std::cout << "Iterations: " << iterations << '\n';
        std::cout << "Moves: " << movecount << '\n';
    }
    */

    namespace detail
    {
        [[nodiscard]] constexpr EnumArray<PieceType, std::uint8_t> makeMaxDestinationCounts()
        {
            EnumArray<PieceType, std::uint8_t> maxDestinationCounts{};

            maxDestinationCounts[PieceType::Pawn] = 12;
            maxDestinationCounts[PieceType::Knight] = 8;
            maxDestinationCounts[PieceType::Bishop] = 13;
            maxDestinationCounts[PieceType::Rook] = 14;
            maxDestinationCounts[PieceType::Queen] = 27;
            maxDestinationCounts[PieceType::King] = 8;
            maxDestinationCounts[PieceType::None] = 0;

            return maxDestinationCounts;
        }

        constexpr auto maxDestinationCounts = makeMaxDestinationCounts();
    }

    constexpr std::uint8_t maxDestinationIndex = 31;
    constexpr std::uint8_t maxNumCastlingMoves = 2;
    constexpr std::uint8_t maxNumKingMoves = 8;

    [[nodiscard]] constexpr std::uint8_t maxDestinationCount(PieceType pt)
    {
        return detail::maxDestinationCounts[pt];
    }

    // This is not used in the encoding/decoding process but
    // may be useful for assertions and some other stuff.
    // The value represents the number of filled entries in 
    // destinationSquareByIndex[pt][from]
    [[nodiscard]] std::uint8_t destinationCount(PieceType pt, Square from);

    // EnumArray2<PieceType, Square, std::array<Square, 32>> destinationSquareByIndex;
    [[nodiscard]] Square destinationSquareByIndex(PieceType pt, Square from, std::uint8_t idx);

    // EnumArray<PieceType, EnumArray2<Square, Square, uint8_t>> destinationIndex;
    // doesn't include pawn moves
    [[nodiscard]] std::uint8_t destinationIndex(PieceType pt, Square from, Square to);

    // this one is special. It is 0 - for king side, or 1 - for queen side.
    [[nodiscard]] std::uint8_t castlingDestinationIndex(Square from, Square to);

    // we know the from square both when encoding and decoding so we can 
    // distinguish whether a move was a promotion or not (by checking rank)
    // For normal pawn moves we use values 0-3
    // For promotions we use values 3*sqid + 4*promotedPieceType (knight-based index)
    // so the values are in range 0-11
    [[nodiscard]] std::uint8_t pawnDestinationIndex(Square from, Square to, Color sideToMove, PieceType promotedPieceType);

    [[nodiscard]] Move destinationIndexToPawnMove(const Position& pos, std::uint8_t index, Square from, Color sideToMove);

    // This is defined for normal moves only.
    // It's mostly a helper for filling other lookup tables.
    // All destinations are filled as if the board was empty apart
    // from the piece on the from square.
    // Doesn't handle castling moves and doesn't distinguish promotions.
    [[nodiscard]] Bitboard destinationsBB(PieceType pt, Square from);

    // We cannot guarantee move index to fit in a byte when 
    // the side to move has more than 3 queens.
    [[nodiscard]] bool requiresLongMoveIndex(const Position& pos);

    // precondition: !requiresLongMoveIndex(pos)
    [[nodiscard]] std::uint8_t moveToShortIndex(const Position& pos, const Move& move);

    // precondition: requiresLongMoveIndex(pos)
    // We may use different encoding scheme here.
    [[nodiscard]] std::uint16_t moveToLongIndex(const Position& pos, const Move& move);

    // precondition: !requiresLongMoveIndex(pos)
    [[nodiscard]] Move shortIndexToMove(const Position& pos, std::uint8_t index);

    // precondition: requiresLongMoveIndex(pos)
    [[nodiscard]] Move longIndexToMove(const Position& pos, std::uint16_t index);
}
