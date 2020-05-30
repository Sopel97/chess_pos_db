#include "catch2/catch.hpp"

#include "chess/Chess.h"
#include "chess/Position.h"
#include "chess/San.h"

TEST_CASE("General SAN tests", "[san]") {
    REQUIRE((san::sanToMove(Position::startPosition(), "a4") == Move{ a2, a4 }));
    REQUIRE((san::sanToMove(Position::startPosition(), "e3") == Move{ e2, e3 }));
    REQUIRE((san::sanToMove(Position::startPosition(), "Nf3") == Move{ g1, f3 }));

    REQUIRE((san::trySanToMove(Position::startPosition(), "a4") == Move{ a2, a4 }));
    REQUIRE((san::trySanToMove(Position::startPosition(), "e3") == Move{ e2, e3 }));
    REQUIRE((san::trySanToMove(Position::startPosition(), "Nf3") == Move{ g1, f3 }));

    REQUIRE((san::sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "a5") == Move{ a7, a5 }));
    REQUIRE((san::sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "e6") == Move{ e7, e6 }));
    REQUIRE((san::sanToMove(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq -"), "Nf6") == Move{ g8, f6 }));

    REQUIRE((san::sanToMove(Position::fromFen("k7/8/8/4pP2/8/8/8/K7 w - e6 0 2"), "fxe6") == Move{ f5, e6, MoveType::EnPassant }));

    REQUIRE((san::sanToMove(Position::fromFen("k4q2/4p3/3Q1Q2/8/8/8/8/5K2 w - - 0 1"), "Qxe7") == Move{ d6, e7 }));
    REQUIRE((san::sanToMove(Position::fromFen("k2q4/4p3/3Q1Q2/8/8/8/8/3K4 w - - 0 1"), "Qxe7!?") == Move{ f6, e7 }));

    REQUIRE((san::sanToMove(Position::fromFen("k7/8/3Q1Q2/4r3/3Q1Q2/8/8/3K4 w - - 0 1"), "Qf6xe5") == Move{ f6, e5 }));

    REQUIRE((san::sanToMove(Position::fromFen("k7/8/3Q1Q2/4r3/8/8/8/3K4 w - - 0 1"), "Qfxe5??!") == Move{ f6, e5 }));

    REQUIRE((san::sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Nxd5") == Move{ b4, d5 }));
    REQUIRE((san::sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Kc1?") == Move{ d1, c1 }));
    REQUIRE((san::sanToMove(Position::fromFen("k7/6N1/6N1/3r1NN1/1N6/8/8/3K4 w - - 0 1"), "Nd4") == Move{ f5, d4 }));

    REQUIRE((san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Bh8") == Move{ e5, h8 }));
    REQUIRE((san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Beg7") == Move{ e5, g7 }));
    REQUIRE((san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Bhg7") == Move{ h6, g7 }));
    REQUIRE((san::sanToMove(Position::fromFen("8/8/7B/4B3/6B1/k4B2/4B3/K7 w - - 0 1"), "Be4") == Move{ f3, e4 }));

    REQUIRE((san::sanToMove(Position::fromFen("8/2B5/7B/2B5/k1B5/2B5/8/K7 w - - 0 1"), "B7e5") == Move{ c7, e5 }));

    REQUIRE((san::sanToMove(Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "O-O!") == Move{ e1, h1, MoveType::Castle }));
    REQUIRE((san::sanToMove(Position::fromFen("1k6/6N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "O-O-ON") == Move{ e1, a1, MoveType::Castle }));

    REQUIRE((san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=Q") == Move{ d7, d8, MoveType::Promotion, whiteQueen }));
    REQUIRE((san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=R") == Move{ d7, d8, MoveType::Promotion, whiteRook }));
    REQUIRE((san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=B") == Move{ d7, d8, MoveType::Promotion, whiteBishop }));
    REQUIRE((san::sanToMove(Position::fromFen("1k6/3P2N1/5rN1/5NN1/1N6/8/8/R3K2R w KQ - 0 1"), "d8=N") == Move{ d7, d8, MoveType::Promotion, whiteKnight }));

    REQUIRE((san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=Q") == Move{ e2, e1, MoveType::Promotion, blackQueen }));
    REQUIRE((san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=R") == Move{ e2, e1, MoveType::Promotion, blackRook }));
    REQUIRE((san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=B") == Move{ e2, e1, MoveType::Promotion, blackBishop }));
    REQUIRE((san::sanToMove(Position::fromFen("k7/8/8/8/8/8/4p3/K7 b - -"), "e1=N") == Move{ e2, e1, MoveType::Promotion, blackKnight }));
}