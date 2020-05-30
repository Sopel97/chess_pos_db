#include "catch2/catch.hpp"

#include "chess/Chess.h"
#include "chess/MoveGenerator.h"
#include "chess/Position.h"

static std::size_t perft(Position&& pos, int depth)
{
    if (depth > 1)
    {
        std::size_t c = 0;
        movegen::forEachLegalMove(pos, [&pos, depth, &c](Move move) {
            auto rmove = pos.doMove(move);
            c += perft(std::move(pos), depth - 1);
            pos.undoMove(rmove);
            });
        return c;
    }
    else
    {
        std::size_t c = 0;
        movegen::forEachLegalMove(pos, [&c](Move move) {
            c += 1;
            });
        return c;
    }
}

TEST_CASE("Legal move generation", "[movegen]") {
    REQUIRE(perft(Position::startPosition(), 5) == 4'865'609);
    REQUIRE(movegen::generateLegalMoves(Position::startPosition().afterMove(Move{ h2, h3 }).afterMove(Move{ a7, a5 })).size() == 19);
    REQUIRE(movegen::generateLegalMoves(Position::startPosition()).size() == 20);
    REQUIRE(movegen::generateLegalMoves(Position::startPosition().afterMove(Move{ e2, e4 })).size() == 20);
    REQUIRE(movegen::generateLegalMoves(Position::startPosition().afterMove(Move{ e2, e4 }).afterMove(Move{ e7, e5 })).size() == 29);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("rnbqkbnr/1ppppppp/8/8/Pp6/8/2PPPPPP/RNBQKBNR w KQkq - 0 3")).size() == 21);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("rnbqkbnr/2pppppp/p7/Pp6/8/8/1PPPPPPP/RNBQKBNR w KQkq b6 0 3")).size() == 22);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("rnbqkbnr/ppp1pppp/8/3p4/4P3/8/PPPP1PPP/RNBQKBNR w KQkq - 0 2")).size() == 31);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r6r/1b2k1bq/8/8/7B/8/8/R3K2R b QK - 3 2")).size() == 8);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/8/2k5/2pP4/8/B7/4K3 b - d3 5 3")).size() == 8);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r1bqkbnr/pppppppp/n7/8/8/P7/1PPPPPPP/RNBQKBNR w QqKk - 2 2")).size() == 19);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/p1pp1pb1/bn2Qnp1/2qPN3/1p2P3/2N5/PPPBBPPP/R3K2R b QqKk - 3 2")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("2kr3r/p1ppqpb1/bn2Qnp1/3PN3/1p2P3/2N5/PPPBBPPP/R3K2R b QK - 3 2")).size() == 44);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("rnb2k1r/pp1Pbppp/2p5/q7/2B5/8/PPPQNnPP/RNB1K2R w QK - 3 9")).size() == 39);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("2r5/3pk3/8/2P5/8/2K5/8/8 w - - 5 4")).size() == 9);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")).size() == 20);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq - 0 1")).size() == 20);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/p1ppqpb1/bn2pnp1/3PN3/1p2P3/2N2Q1p/PPPBBPPP/R3K2R w KQkq - 0 1")).size() == 48);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/4K2R w K - 0 1")).size() == 15);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/R3K3 w Q - 0 1")).size() == 16);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("4k2r/8/8/8/8/8/8/4K3 w k - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k3/8/8/8/8/8/8/4K3 w q - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/R3K2R w KQ - 0 1")).size() == 26);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/4K3 w kq - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/8/8/8/8/6k1/4K2R w K - 0 1")).size() == 12);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/8/8/8/8/1k6/R3K3 w Q - 0 1")).size() == 15);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("4k2r/6K1/8/8/8/8/8/8 w k - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k3/1K6/8/8/8/8/8/8 w q - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/R3K2R w KQkq - 0 1")).size() == 26);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/1R2K2R w Kkq - 0 1")).size() == 25);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/2R1K2R w Kkq - 0 1")).size() == 25);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/R3K1R1 w Qkq - 0 1")).size() == 25);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("1r2k2r/8/8/8/8/8/8/R3K2R w KQk - 0 1")).size() == 26);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("2r1k2r/8/8/8/8/8/8/R3K2R w KQk - 0 1")).size() == 25);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k1r1/8/8/8/8/8/8/R3K2R w KQq - 0 1")).size() == 25);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/4K2R b K - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/R3K3 b Q - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("4k2r/8/8/8/8/8/8/4K3 b k - 0 1")).size() == 15);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k3/8/8/8/8/8/8/4K3 b q - 0 1")).size() == 16);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/R3K2R b KQ - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/4K3 b kq - 0 1")).size() == 26);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/8/8/8/8/6k1/4K2R b K - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/8/8/8/8/1k6/R3K3 b Q - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("4k2r/6K1/8/8/8/8/8/8 b k - 0 1")).size() == 12);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k3/1K6/8/8/8/8/8/8 b q - 0 1")).size() == 15);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/R3K2R b KQkq - 0 1")).size() == 26);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/1R2K2R b Kkq - 0 1")).size() == 26);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/2R1K2R b Kkq - 0 1")).size() == 25);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/R3K1R1 b Qkq - 0 1")).size() == 25);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("1r2k2r/8/8/8/8/8/8/R3K2R b KQk - 0 1")).size() == 25);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("2r1k2r/8/8/8/8/8/8/R3K2R b KQk - 0 1")).size() == 25);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("r3k1r1/8/8/8/8/8/8/R3K2R b KQq - 0 1")).size() == 25);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/1n4N1/2k5/8/8/5K2/1N4n1/8 w - - 0 1")).size() == 14);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/1k6/8/5N2/8/4n3/8/2K5 w - - 0 1")).size() == 11);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/4k3/3Nn3/3nN3/4K3/8/8 w - - 0 1")).size() == 19);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("K7/8/2n5/1n6/8/8/8/k6N w - - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/2N5/1N6/8/8/8/K6n w - - 0 1")).size() == 17);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/1n4N1/2k5/8/8/5K2/1N4n1/8 b - - 0 1")).size() == 15);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/1k6/8/5N2/8/4n3/8/2K5 b - - 0 1")).size() == 16);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/3K4/3Nn3/3nN3/4k3/8/8 b - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("K7/8/2n5/1n6/8/8/8/k6N b - - 0 1")).size() == 17);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/2N5/1N6/8/8/8/K6n b - - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("B6b/8/8/8/2K5/4k3/8/b6B w - - 0 1")).size() == 17);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/1B6/7b/7k/8/2B1b3/7K w - - 0 1")).size() == 21);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/B7/1B6/1B6/8/8/8/K6b w - - 0 1")).size() == 21);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("K7/b7/1b6/1b6/8/8/8/k6B w - - 0 1")).size() == 7);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("B6b/8/8/8/2K5/5k2/8/b6B b - - 0 1")).size() == 6);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/1B6/7b/7k/8/2B1b3/7K b - - 0 1")).size() == 17);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/B7/1B6/1B6/8/8/8/K6b b - - 0 1")).size() == 7);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("K7/b7/1b6/1b6/8/8/8/k6B b - - 0 1")).size() == 21);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/RR6/8/8/8/8/rr6/7K w - - 0 1")).size() == 19);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("R6r/8/8/2K5/5k2/8/8/r6R w - - 0 1")).size() == 36);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/RR6/8/8/8/8/rr6/7K b - - 0 1")).size() == 19);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("R6r/8/8/2K5/5k2/8/8/r6R b - - 0 1")).size() == 36);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("6kq/8/8/8/8/8/8/7K w - - 0 1")).size() == 2);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("6KQ/8/8/8/8/8/8/7k b - - 0 1")).size() == 2);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("K7/8/8/3Q4/4q3/8/8/7k w - - 0 1")).size() == 6);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("6qk/8/8/8/8/8/8/7K b - - 0 1")).size() == 22);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("6KQ/8/8/8/8/8/8/7k b - - 0 1")).size() == 2);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("K7/8/8/3Q4/4q3/8/8/7k b - - 0 1")).size() == 6);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/8/8/8/K7/P7/k7 w - - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/8/8/8/7K/7P/7k w - - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("K7/p7/k7/8/8/8/8/8 w - - 0 1")).size() == 1);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7K/7p/7k/8/8/8/8/8 w - - 0 1")).size() == 1);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/2k1p3/3pP3/3P2K1/8/8/8/8 w - - 0 1")).size() == 7);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/8/8/8/K7/P7/k7 b - - 0 1")).size() == 1);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/8/8/8/7K/7P/7k b - - 0 1")).size() == 1);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("K7/p7/k7/8/8/8/8/8 b - - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7K/7p/7k/8/8/8/8/8 b - - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/2k1p3/3pP3/3P2K1/8/8/8/8 b - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/8/8/8/4k3/4P3/4K3 w - - 0 1")).size() == 2);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("4k3/4p3/4K3/8/8/8/8/8 b - - 0 1")).size() == 2);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/7k/7p/7P/7K/8/8 w - - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/k7/p7/P7/K7/8/8 w - - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/3k4/3p4/3P4/3K4/8/8 w - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/3k4/3p4/8/3P4/3K4/8/8 w - - 0 1")).size() == 8);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/3k4/3p4/8/3P4/3K4/8 w - - 0 1")).size() == 8);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/3p4/8/3P4/8/8/7K w - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/7k/7p/7P/7K/8/8 b - - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/k7/p7/P7/K7/8/8 b - - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/3k4/3p4/3P4/3K4/8/8 b - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/3k4/3p4/8/3P4/3K4/8/8 b - - 0 1")).size() == 8);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/8/3k4/3p4/8/3P4/3K4/8 b - - 0 1")).size() == 8);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/3p4/8/3P4/8/8/7K b - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/3p4/8/8/3P4/8/8/K7 w - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/8/8/3p4/8/8/3P4/K7 w - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/8/7p/6P1/8/8/K7 w - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/7p/8/8/6P1/8/K7 w - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/8/6p1/7P/8/8/K7 w - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/6p1/8/8/7P/8/K7 w - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/8/3p4/4p3/8/8/7K w - - 0 1")).size() == 3);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/3p4/8/8/4P3/8/7K w - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/3p4/8/8/3P4/8/8/K7 b - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/8/8/3p4/8/8/3P4/K7 b - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/8/7p/6P1/8/8/K7 b - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/7p/8/8/6P1/8/K7 b - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/8/6p1/7P/8/8/K7 b - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/6p1/8/8/7P/8/K7 b - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/8/3p4/4p3/8/8/7K b - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/8/3p4/8/8/4P3/8/7K b - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/8/8/p7/1P6/8/8/7K w - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/8/8/p7/1P6/8/8/7K b - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/8/8/1p6/P7/8/8/7K w - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/8/8/1p6/P7/8/8/7K b - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/8/p7/8/8/1P6/8/7K w - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/8/p7/8/8/1P6/8/7K b - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/8/1p6/8/8/P7/8/7K w - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("7k/8/1p6/8/8/P7/8/7K b - - 0 1")).size() == 4);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/7p/8/8/8/8/6P1/K7 w - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/7p/8/8/8/8/6P1/K7 b - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/6p1/8/8/8/8/7P/K7 w - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("k7/6p1/8/8/8/8/7P/K7 b - - 0 1")).size() == 5);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/Pk6/8/8/8/8/6Kp/8 w - - 0 1")).size() == 11);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/Pk6/8/8/8/8/6Kp/8 b - - 0 1")).size() == 11);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("3k4/3pp3/8/8/8/8/3PP3/3K4 w - - 0 1")).size() == 7);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("3k4/3pp3/8/8/8/8/3PP3/3K4 b - - 0 1")).size() == 7);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/PPPk4/8/8/8/8/4Kppp/8 w - - 0 1")).size() == 18);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("8/PPPk4/8/8/8/8/4Kppp/8 b - - 0 1")).size() == 18);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("n1n5/1Pk5/8/8/8/8/5Kp1/5N1N w - - 0 1")).size() == 24);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("n1n5/1Pk5/8/8/8/8/5Kp1/5N1N b - - 0 1")).size() == 24);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("n1n5/PPPk4/8/8/8/8/4Kppp/5N1N w - - 0 1")).size() == 24);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("n1n5/PPPk4/8/8/8/8/4Kppp/5N1N b - - 0 1")).size() == 24);
    REQUIRE(movegen::generateLegalMoves(Position::fromFen("rnbqkbnr/pppp1ppp/8/8/3p4/4P3/PPP1QPPP/RNB1KBNR b KQkq - 1 3")).size() == 31);
}