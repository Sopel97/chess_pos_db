#include "gtest/gtest.h"

#include "chess/Position.h"

TEST(PositionTest, GeneralPositionTest) {

    ASSERT_TRUE((Position::fromFen("k7/6p1/5q2/5P2/8/8/5K2/8 b - - 0 1").piecesBB() == (Bitboard::square(F2) | F5 | F6 | G7 | A8)));
    ASSERT_TRUE((bb::isAttackedBySlider(F2, {}, {}, Bitboard::square(F6), (Bitboard::square(F2) | F6 | A8))));

    ASSERT_TRUE((
        (
            bb::pawnAttacks(Bitboard::square(G6), Color::Black) &
            Position::fromFen("k7/8/5q2/5Pp1/8/8/5K2/8 w - - 0 2").piecesBB(Piece(PieceType::Pawn, Color::White))
            ) == Bitboard::square(F5)));

    ASSERT_TRUE((Position::fromFen("rnbqkbnr/ppp2ppp/8/3ppP2/8/4P3/PPPP2PP/RNBQKBNR b KQkq - 0 3").afterMove(Move{ G7, G5 }).isEpPossible()));
    ASSERT_TRUE((!Position::fromFen("rnb1kbnr/pp3ppp/5q2/2pppP2/8/2N1P3/PPPP1KPP/R1BQ1BNR b kq - 3 5").afterMove(Move{ G7, G5 }).isEpPossible()));
    ASSERT_TRUE((Position::fromFen("rnb1kbnr/pp3ppp/5q2/2pppP2/8/2N1P3/PPPP1KPP/R1BQ1BNR b kq - 3 5").afterMove(Move{ G7, G5 }).createsDiscoveredAttackOnOwnKing(Move{ F5, G6, MoveType::EnPassant })));

    ASSERT_TRUE((Position::startPosition().afterMove(Move{ A2, A4 }) == Position::fromFen("rnbqkbnr/pppppppp/8/8/P7/8/1PPPPPPP/RNBQKBNR b KQkq -")));
    ASSERT_TRUE((Position::startPosition().afterMove(Move{ E2, E3 }) == Position::fromFen("rnbqkbnr/pppppppp/8/8/8/4P3/PPPP1PPP/RNBQKBNR b KQkq -")));
    ASSERT_TRUE((Position::startPosition().afterMove(Move{ G1, F3 }) == Position::fromFen("rnbqkbnr/pppppppp/8/8/8/5N2/PPPPPPPP/RNBQKB1R b KQkq -")));

    ASSERT_TRUE((Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -").afterMove(Move{ A7, A5 }) == Position::fromFen("rnbqkbnr/1ppppppp/8/p7/8/8/PPPPPPPP/RNBQKBNR w KQkq -")));
    ASSERT_TRUE((Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -").afterMove(Move{ E7, E6 }) == Position::fromFen("rnbqkbnr/pppp1ppp/4p3/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -")));
    ASSERT_TRUE((Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -").afterMove(Move{ G8, F6 }) == Position::fromFen("rnbqkb1r/pppppppp/5n2/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -")));

    ASSERT_TRUE((Position::fromFen("k7/8/8/4pP2/8/8/8/K7 w - e6 0 2").afterMove(Move{ F5, E6, MoveType::EnPassant }) == Position::fromFen("k7/8/4P3/8/8/8/8/K7 b - -")));

    ASSERT_TRUE((Position::fromFen("k4q2/4p3/3Q1Q2/8/8/8/8/5K2 w - - 0 1").afterMove(Move{ D6, E7 }) == Position::fromFen("k4q2/4Q3/5Q2/8/8/8/8/5K2 b - -")));
    ASSERT_TRUE((Position::fromFen("k2q4/4p3/3Q1Q2/8/8/8/8/3K4 w - - 0 1").afterMove(Move{ F6, E7 }) == Position::fromFen("k2q4/4Q3/3Q4/8/8/8/8/3K4 b - -")));

    ASSERT_TRUE((Position::fromFen("k7/8/3Q1Q2/4r3/3Q1Q2/8/8/3K4 w - - 0 1").afterMove(Move{ F6, E5 }) == Position::fromFen("k7/8/3Q4/4Q3/3Q1Q2/8/8/3K4 b - -")));

    ASSERT_TRUE((Position::fromFen("k7/8/3Q1Q2/4r3/8/8/8/3K4 w - - 0 1").afterMove(Move{ F6, E5 }) == Position::fromFen("k7/8/3Q4/4Q3/8/8/8/3K4 b - -")));

    ASSERT_TRUE((Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1").afterMove(Move{ B4, D5 }) == Position::fromFen("k7/6N1/6N1/3N1NN1/8/8/8/3K4 b - -")));
    ASSERT_TRUE((Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1").afterMove(Move{ D1, C1 }) == Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/2K5 b - -")));
    ASSERT_TRUE((Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1").afterMove(Move{ F5, D4 }) == Position::fromFen("k7/6N1/6N1/3r2N1/1N1N4/8/8/3K4 b - -")));

    ASSERT_TRUE((Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1").afterMove(Move{ E5, H8 }) == Position::fromFen("7B/8/7B/8/6B1/k4B2/4B3/K7 b - -")));
    ASSERT_TRUE((Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1").afterMove(Move{ E5, G7 }) == Position::fromFen("8/6B1/7B/8/6B1/k4B2/4B3/K7 b - -")));
    ASSERT_TRUE((Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1").afterMove(Move{ H6, G7 }) == Position::fromFen("8/6B1/8/4B3/6B1/k4B2/4B3/K7 b - -")));
    ASSERT_TRUE((Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1").afterMove(Move{ F3, E4 }) == Position::fromFen("8/8/7B/4B3/4B1B1/k7/4B3/K7 b - -")));

    ASSERT_TRUE((Position::fromFen("8/2B5/7B/2B5/k1B5/2B5/8/K7 w - - 0 1").afterMove(Move{ C7, E5 }) == Position::fromFen("8/8/7B/2B1B3/k1B5/2B5/8/K7 b - -")));

    ASSERT_TRUE((Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ E1, H1, MoveType::Castle }) == Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R4RK1 b - - 1 1")));
    ASSERT_TRUE((Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ E1, A1, MoveType::Castle }) == Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/2KR3R b - - 1 1")));

    ASSERT_TRUE((Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ D7, D8, MoveType::Promotion, whiteQueen }) == Position::fromFen("1k1Q4/6N1/5rN1/5NN1/1N6/8/8/R3K2R b KQ - 0 1")));
    ASSERT_TRUE((Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ D7, D8, MoveType::Promotion, whiteRook }) == Position::fromFen("1k1R4/6N1/5rN1/5NN1/1N6/8/8/R3K2R b KQ - 0 1")));
    ASSERT_TRUE((Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ D7, D8, MoveType::Promotion, whiteBishop }) == Position::fromFen("1k1B4/6N1/5rN1/5NN1/1N6/8/8/R3K2R b KQ - 0 1")));
    ASSERT_TRUE((Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1").afterMove(Move{ D7, D8, MoveType::Promotion, whiteKnight }) == Position::fromFen("1k1N4/6N1/5rN1/5NN1/1N6/8/8/R3K2R b KQ - 0 1")));

    ASSERT_TRUE((Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -").afterMove(Move{ E2, E1, MoveType::Promotion, blackQueen }) == Position::fromFen("k7/8/8/8/8/8/8/K3q3 w - - 0 2")));
    ASSERT_TRUE((Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -").afterMove(Move{ E2, E1, MoveType::Promotion, blackRook }) == Position::fromFen("k7/8/8/8/8/8/8/K3r3 w - - 0 2")));
    ASSERT_TRUE((Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -").afterMove(Move{ E2, E1, MoveType::Promotion, blackBishop }) == Position::fromFen("k7/8/8/8/8/8/8/K3b3 w - - 0 2")));
    ASSERT_TRUE((Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -").afterMove(Move{ E2, E1, MoveType::Promotion, blackKnight }) == Position::fromFen("k7/8/8/8/8/8/8/K3n3 w - - 0 2")));


    ASSERT_TRUE((Position::fromFen("k7/8/8/q2pP2K/8/8/8/8 w - d6 0 2").createsDiscoveredAttackOnOwnKing(Move{ E5, D6, MoveType::EnPassant })));
    ASSERT_TRUE((!Position::fromFen("k7/8/q7/3pP2K/8/8/8/8 w - d6 0 1").createsDiscoveredAttackOnOwnKing(Move{ E5, D6, MoveType::EnPassant })));
    ASSERT_TRUE((Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").createsDiscoveredAttackOnOwnKing(Move{ E5, D6, MoveType::EnPassant })));
    ASSERT_TRUE((!Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").createsDiscoveredAttackOnOwnKing(Move{ E5, E6 })));


    ASSERT_TRUE((Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").isSquareAttacked(C6, Color::Black)));
    ASSERT_TRUE((Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").isSquareAttacked(E4, Color::Black)));
    ASSERT_TRUE((Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").isSquareAttacked(D5, Color::Black)));
    ASSERT_TRUE((!Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").isSquareAttacked(H1, Color::Black)));
    ASSERT_TRUE((Position::fromFen("k7/1b6/q7/3pP3/8/5K2/8/8 w - d6 0 1").isSquareAttacked(D6, Color::White)));
    ASSERT_TRUE((!Position::fromFen("k7/qb6/8/3pP3/8/5K2/8/8 w - -").isSquareAttacked(H7, Color::Black)));

    ASSERT_TRUE((!Position::fromFen("k7/qb6/8/3pP3/8/5K2/8/8 w - -").isSquareAttackedAfterMove(G7, Move{ A8, B8 }, Color::Black)));
    ASSERT_TRUE((Position::fromFen("k7/qb6/8/3pP3/8/5K2/8/8 w - -").isSquareAttackedAfterMove(G7, Move{ A7, G1 }, Color::Black)));
    ASSERT_TRUE((Position::fromFen("k7/1b6/8/q2pP3/8/5K2/8/8 w - d6").isSquareAttackedAfterMove(H5, Move{ E5, D6, MoveType::EnPassant }, Color::Black)));
    ASSERT_TRUE((Position::fromFen("k7/1b6/8/q2pP3/8/5K2/8/8 w - d6").isSquareAttackedAfterMove(E4, Move{ E5, D6, MoveType::EnPassant }, Color::Black)));
    ASSERT_TRUE((!Position::fromFen("k7/1b6/8/q2pP3/8/5K2/8/8 w - d6").isSquareAttackedAfterMove(H1, Move{ E5, D6, MoveType::EnPassant }, Color::Black)));

    ASSERT_TRUE((!Position::fromFen("rnb2k1r/pp1Pbppp/2p5/q7/2B5/8/PPPQNnPP/RNB1K2R w KQ - 0 1").createsAttackOnOwnKing(Move{ E1, H1, MoveType::Castle })));

}