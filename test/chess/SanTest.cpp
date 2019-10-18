#include "gtest/gtest.h"

#include "chess/Chess.h"
#include "chess/Position.h"
#include "chess/San.h"

TEST(SanTest, GeneralSanTest) {
    ASSERT_TRUE((san::sanToMove(Position::startPosition(), "a4") == Move{ A2, A4 }));
    ASSERT_TRUE((san::sanToMove(Position::startPosition(), "e3") == Move{ E2, E3 }));
    ASSERT_TRUE((san::sanToMove(Position::startPosition(), "Nf3") == Move{ G1, F3 }));

    ASSERT_TRUE((san::trySanToMove(Position::startPosition(), "a4") == Move{ A2, A4 }));
    ASSERT_TRUE((san::trySanToMove(Position::startPosition(), "e3") == Move{ E2, E3 }));
    ASSERT_TRUE((san::trySanToMove(Position::startPosition(), "Nf3") == Move{ G1, F3 }));

    ASSERT_TRUE((san::sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "a5") == Move{ A7, A5 }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "e6") == Move{ E7, E6 }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "Nf6") == Move{ G8, F6 }));

    ASSERT_TRUE((san::sanToMove(Position::fromFen("k7/8/8/4pP2/8/8/8/K7 w - e6 0 2"), "fxe6") == Move{ F5, E6, MoveType::EnPassant }));

    ASSERT_TRUE((san::sanToMove(Position::fromFen("k4q2/4p3/3Q1Q2/8/8/8/8/5K2 w - - 0 1"), "Qxe7") == Move{ D6, E7 }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("k2q4/4p3/3Q1Q2/8/8/8/8/3K4 w - - 0 1"), "Qxe7!?") == Move{ F6, E7 }));

    ASSERT_TRUE((san::sanToMove(Position::fromFen("k7/8/3Q1Q2/4r3/3Q1Q2/8/8/3K4 w - - 0 1"), "Qf6xe5") == Move{ F6, E5 }));

    ASSERT_TRUE((san::sanToMove(Position::fromFen("k7/8/3Q1Q2/4r3/8/8/8/3K4 w - - 0 1"), "Qfxe5??!") == Move{ F6, E5 }));

    ASSERT_TRUE((san::sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Nxd5") == Move{ B4, D5 }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Kc1?") == Move{ D1, C1 }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Nd4") == Move{ F5, D4 }));

    ASSERT_TRUE((san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Bh8") == Move{ E5, H8 }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Beg7") == Move{ E5, G7 }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Bhg7") == Move{ H6, G7 }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Be4") == Move{ F3, E4 }));

    ASSERT_TRUE((san::sanToMove(Position::fromFen("8/2B5/7B/2B5/k1B5/2B5/8/K7 w - - 0 1"), "B7e5") == Move{ C7, E5 }));

    ASSERT_TRUE((san::sanToMove(Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "O-O!") == Move{ E1, H1, MoveType::Castle }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "O-O-ON") == Move{ E1, A1, MoveType::Castle }));

    ASSERT_TRUE((san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=Q") == Move{ D7, D8, MoveType::Promotion, whiteQueen }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=R") == Move{ D7, D8, MoveType::Promotion, whiteRook }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=B") == Move{ D7, D8, MoveType::Promotion, whiteBishop }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=N") == Move{ D7, D8, MoveType::Promotion, whiteKnight }));

    ASSERT_TRUE((san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=Q") == Move{ E2, E1, MoveType::Promotion, blackQueen }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=R") == Move{ E2, E1, MoveType::Promotion, blackRook }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=B") == Move{ E2, E1, MoveType::Promotion, blackBishop }));
    ASSERT_TRUE((san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=N") == Move{ E2, E1, MoveType::Promotion, blackKnight }));
}