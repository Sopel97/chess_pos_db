#define _CRT_SECURE_NO_WARNINGS

#include <iostream>
#include <iomanip>
#include <filesystem>
#include <cstdio>
#include <memory>
#include <unordered_map>
#include <chrono>
#include <fstream>
#include <map>
#include <execution>
#include <atomic>

#include "LocalStorageFormat.h"

#include "Bitboard.h"
#include "Enum.h"
#include "EnumMap.h"
#include "Intrinsics.h"
#include "Pgn.h"
#include "Position.h"
#include "PositionSignature.h"
#include "San.h"
#include "MoveGenerator.h"
#include "External.h"

#include "lib/xxhash/xxhash_cpp.h"

#include "lib/robin_hood/robin_hood.h"

#include "CodingTest.h"

void print(Bitboard bb)
{
    std::cout << std::hex << std::setfill('0') << std::setw(16) << bb.bits() << '\n';
    for (Rank r = rank8; r >= rank1; --r)
    {
        for (File f = fileA; f <= fileH; ++f)
        {
            std::cout << bb.isSet(Square(f, r)) ? 'X' : '.';
        }
        std::cout << '\n';
    }
    std::cout << "\n\n";
}

auto hash(const Position& pos)
{
    return xxhash::XXH3_128bits(pos.piecesRaw(), 64);
}

void print(xxhash::XXH128_hash_t h)
{
    std::cout << std::hex << h.high64 << std::hex << h.low64;
}

void header(std::ostream& out)
{
    out.write("PGCOPY\n\377\r\n\0", 11);
    out.write("\0\0\0\0", 4);
    out.write("\0\0\0\0", 4);
}

std::uint64_t bswap(std::uint64_t v)
{
    return _byteswap_uint64(v);
}

/*
void dump(const PositionSignature& s, const std::array<std::uint64_t, 3>& wdl, std::ostream& out)
{
    char data[] =
        "\x00\x04"
        "\x00\x00\x00\x10"
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        ;

    const std::uint64_t id[2] = { bswap(s.hash().high64), bswap(s.hash().low64) };
    const std::uint64_t w = bswap(wdl[0]);
    const std::uint64_t d = bswap(wdl[1]);
    const std::uint64_t l = bswap(wdl[2]);

    std::memcpy(data + 2 + 4, &id, 16);
    std::memcpy(data + 2 + 4 + 16 + 4 + (8 + 4) * 0, &w, 8);
    std::memcpy(data + 2 + 4 + 16 + 4 + (8 + 4) * 1, &d, 8);
    std::memcpy(data + 2 + 4 + 16 + 4 + (8 + 4) * 2, &l, 8);

    out.write(data, sizeof(data) - 1);

    char data[] =
        "\x00\x05"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        ;

    const std::uint64_t id[2] = { bswap(s.hash().high64), bswap(s.hash().low64) };
    const std::uint64_t w = bswap(wdl[0]);
    const std::uint64_t d = bswap(wdl[1]);
    const std::uint64_t l = bswap(wdl[2]);

    std::memcpy(data + 2 + 4 + (8 + 4) * 0, &id[0], 8);
    std::memcpy(data + 2 + 4 + (8 + 4) * 1, &id[1], 8);
    std::memcpy(data + 2 + 4 + (8 + 4) * 2, &w, 8);
    std::memcpy(data + 2 + 4 + (8 + 4) * 3, &d, 8);
    std::memcpy(data + 2 + 4 + (8 + 4) * 4, &l, 8);

    out.write(data, sizeof(data) - 1);
}
    */

void trailer(std::ostream& out)
{
    out.write("\xFF\xFF", 2);
}
std::size_t dumpPositions(const std::vector<std::filesystem::path>& froms, const std::filesystem::path& to, std::size_t reserve, bool unique)
{
    std::vector<PositionSignature> positions;
    positions.reserve(reserve / sizeof(PositionSignature));

    int numPositions = 0;
    for (auto& from : froms)
    {
        pgn::LazyPgnFileReader fr(from);
        if (!fr.isOpen())
        {
            std::cout << "Failed to open file " << from << '\n';
            break;
        }

        for (auto& game : fr)
        {
            for (auto& pos : game.positions())
            {
                positions.emplace_back(pos);
                ++numPositions;
            }
        }
    }

    std::sort(std::execution::par_unseq, positions.begin(), positions.end());

    if (unique) positions.erase(std::unique(positions.begin(), positions.end()), positions.end());

    std::string toStr = to.string();
    FILE* file = std::fopen(toStr.c_str(), "wb");
    std::fwrite(positions.data(), sizeof(PositionSignature), positions.size(), file);
    std::fclose(file);

    return numPositions;
}

std::size_t dumpPositions(const std::filesystem::path& from, const std::filesystem::path& to, std::size_t reserve, bool unique)
{
    pgn::LazyPgnFileReader fr(from);
    if (!fr.isOpen())
    {
        std::cout << "Failed to open file.\n";
        return 0;
    }
    std::vector<PositionSignature> positions;
    positions.reserve(reserve / sizeof(PositionSignature));

    int numPositions = 0;
    for (auto& game : fr)
    {
        for (auto& pos : game.positions())
        {
            positions.emplace_back(pos);
            ++numPositions;
        }
    }

    std::sort(std::execution::par_unseq, positions.begin(), positions.end());

    if (unique) positions.erase(std::unique(positions.begin(), positions.end()), positions.end());

    std::string toStr = to.string();
    FILE* file = std::fopen(toStr.c_str(), "wb");
    std::fwrite(positions.data(), sizeof(PositionSignature), positions.size(), file);
    std::fclose(file);

    return numPositions;
}

void build()
{
    persistence::local::Database e("w:/catobase/.tmp_indexed", 4ull * 1024ull * 1024ull);
    //persistence::local::Database e("c:/dev/chess_pos_db/.tmp", 4ull * 1024ull * 1024ull);
    /*
    e.importPgns(std::execution::par_unseq, {
        {"w:/catobase/data/lichess_db_standard_rated_2013-01.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-02.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-03.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-04.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-05.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-06.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-07.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-08.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-09.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-10.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-11.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-12.pgn", GameLevel::Human}
        }, 2u * 1024u * 1024u * 1024u);
        */
    e.importPgns(std::execution::par_unseq, {
        {"w:/catobase/data/lichess_db_standard_rated_2013-01.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-02.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-03.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-04.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-05.pgn", GameLevel::Engine},
        {"w:/catobase/data/lichess_db_standard_rated_2013-06.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-07.pgn", GameLevel::Engine},
        {"w:/catobase/data/lichess_db_standard_rated_2013-08.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-09.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-10.pgn", GameLevel::Engine},
        {"w:/catobase/data/lichess_db_standard_rated_2013-11.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-12.pgn", GameLevel::Engine}
        }, 2u * 1024u * 1024u * 1024u);
}

void buildsmall()
{
    persistence::local::Database e("w:/catobase/.tmp2_indexed", 4ull * 1024ull * 1024ull);
    e.importPgns(std::execution::par_unseq, {
        {"w:/catobase/data/Server Games LiChess 2019-1.pgn", GameLevel::Human}
        }, 2u * 1024u * 1024u * 1024u);
}

void query()
{
    std::cout << "Loading db\n";
    persistence::local::Database e("w:/catobase/.tmp2_indexed", 4ull * 1024ull * 1024ull);
    std::cout << "Loaded db\n";

    std::vector<Position> positions;
    positions.emplace_back(Position::startPosition().afterMove({ E2, E4 }));

    auto results = e.queryRanges(positions);
    auto count = 0;
    {
        for (GameLevel level : values<GameLevel>())
        {
            for (GameResult result : values<GameResult>())
            {
                auto& r = results[level][result];
                r.print();
                std::cout << "\n\n";
                count += r.count();
            }
        }
    }
    std::cout << count << '\n';
}

void testMoveGen()
{
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::startPosition()).size() == 20);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::startPosition().afterMove(Move{ E2, E4 })).size() == 20);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::startPosition().afterMove(Move{ E2, E4 }).afterMove(Move{ E7, E5 })).size() == 29);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r6r/1b2k1bq/8/8/7B/8/8/R3K2R b QK - 3 2")).size() == 8);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/8/2k5/2pP4/8/B7/4K3 b - d3 5 3")).size() == 8);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r1bqkbnr/pppppppp/n7/8/8/P7/1PPPPPPP/RNBQKBNR w QqKk - 2 2")).size() == 19);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/p1pp1pb1/bn2Qnp1/2qPN3/1p2P3/2N5/PPPBBPPP/R3K2R b QqKk - 3 2")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("2kr3r/p1ppqpb1/bn2Qnp1/3PN3/1p2P3/2N5/PPPBBPPP/R3K2R b QK - 3 2")).size() == 44);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("rnb2k1r/pp1Pbppp/2p5/q7/2B5/8/PPPQNnPP/RNB1K2R w QK - 3 9")).size() == 39);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("2r5/3pk3/8/2P5/8/2K5/8/8 w - - 5 4")).size() == 9);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")).size() == 20);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR b KQkq - 0 1")).size() == 20);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/p1ppqpb1/bn2pnp1/3PN3/1p2P3/2N2Q1p/PPPBBPPP/R3K2R w KQkq - 0 1")).size() == 48);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/4K2R w K - 0 1")).size() == 15);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/R3K3 w Q - 0 1")).size() == 16);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("4k2r/8/8/8/8/8/8/4K3 w k - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k3/8/8/8/8/8/8/4K3 w q - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/R3K2R w KQ - 0 1")).size() == 26);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/4K3 w kq - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/8/8/8/8/6k1/4K2R w K - 0 1")).size() == 12);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/8/8/8/8/1k6/R3K3 w Q - 0 1")).size() == 15);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("4k2r/6K1/8/8/8/8/8/8 w k - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k3/1K6/8/8/8/8/8/8 w q - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/R3K2R w KQkq - 0 1")).size() == 26);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/1R2K2R w Kkq - 0 1")).size() == 25);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/2R1K2R w Kkq - 0 1")).size() == 25);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/R3K1R1 w Qkq - 0 1")).size() == 25);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("1r2k2r/8/8/8/8/8/8/R3K2R w KQk - 0 1")).size() == 26);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("2r1k2r/8/8/8/8/8/8/R3K2R w KQk - 0 1")).size() == 25);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k1r1/8/8/8/8/8/8/R3K2R w KQq - 0 1")).size() == 25);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/4K2R b K - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/R3K3 b Q - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("4k2r/8/8/8/8/8/8/4K3 b k - 0 1")).size() == 15);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k3/8/8/8/8/8/8/4K3 b q - 0 1")).size() == 16);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("4k3/8/8/8/8/8/8/R3K2R b KQ - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/4K3 b kq - 0 1")).size() == 26);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/8/8/8/8/6k1/4K2R b K - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/8/8/8/8/1k6/R3K3 b Q - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("4k2r/6K1/8/8/8/8/8/8 b k - 0 1")).size() == 12);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k3/1K6/8/8/8/8/8/8 b q - 0 1")).size() == 15);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/R3K2R b KQkq - 0 1")).size() == 26);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/1R2K2R b Kkq - 0 1")).size() == 26);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/2R1K2R b Kkq - 0 1")).size() == 25);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k2r/8/8/8/8/8/8/R3K1R1 b Qkq - 0 1")).size() == 25);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("1r2k2r/8/8/8/8/8/8/R3K2R b KQk - 0 1")).size() == 25);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("2r1k2r/8/8/8/8/8/8/R3K2R b KQk - 0 1")).size() == 25);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("r3k1r1/8/8/8/8/8/8/R3K2R b KQq - 0 1")).size() == 25);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/1n4N1/2k5/8/8/5K2/1N4n1/8 w - - 0 1")).size() == 14);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/1k6/8/5N2/8/4n3/8/2K5 w - - 0 1")).size() == 11);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/4k3/3Nn3/3nN3/4K3/8/8 w - - 0 1")).size() == 19);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("K7/8/2n5/1n6/8/8/8/k6N w - - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/2N5/1N6/8/8/8/K6n w - - 0 1")).size() == 17);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/1n4N1/2k5/8/8/5K2/1N4n1/8 b - - 0 1")).size() == 15);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/1k6/8/5N2/8/4n3/8/2K5 b - - 0 1")).size() == 16);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/3K4/3Nn3/3nN3/4k3/8/8 b - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("K7/8/2n5/1n6/8/8/8/k6N b - - 0 1")).size() == 17);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/2N5/1N6/8/8/8/K6n b - - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("B6b/8/8/8/2K5/4k3/8/b6B w - - 0 1")).size() == 17);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/1B6/7b/7k/8/2B1b3/7K w - - 0 1")).size() == 21);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/B7/1B6/1B6/8/8/8/K6b w - - 0 1")).size() == 21);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("K7/b7/1b6/1b6/8/8/8/k6B w - - 0 1")).size() == 7);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("B6b/8/8/8/2K5/5k2/8/b6B b - - 0 1")).size() == 6);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/1B6/7b/7k/8/2B1b3/7K b - - 0 1")).size() == 17);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/B7/1B6/1B6/8/8/8/K6b b - - 0 1")).size() == 7);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("K7/b7/1b6/1b6/8/8/8/k6B b - - 0 1")).size() == 21);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/RR6/8/8/8/8/rr6/7K w - - 0 1")).size() == 19);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("R6r/8/8/2K5/5k2/8/8/r6R w - - 0 1")).size() == 36);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/RR6/8/8/8/8/rr6/7K b - - 0 1")).size() == 19);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("R6r/8/8/2K5/5k2/8/8/r6R b - - 0 1")).size() == 36);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("6kq/8/8/8/8/8/8/7K w - - 0 1")).size() == 2);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("6KQ/8/8/8/8/8/8/7k b - - 0 1")).size() == 2);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("K7/8/8/3Q4/4q3/8/8/7k w - - 0 1")).size() == 6);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("6qk/8/8/8/8/8/8/7K b - - 0 1")).size() == 22);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("6KQ/8/8/8/8/8/8/7k b - - 0 1")).size() == 2);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("K7/8/8/3Q4/4q3/8/8/7k b - - 0 1")).size() == 6);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/8/8/8/K7/P7/k7 w - - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/8/8/8/7K/7P/7k w - - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("K7/p7/k7/8/8/8/8/8 w - - 0 1")).size() == 1);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7K/7p/7k/8/8/8/8/8 w - - 0 1")).size() == 1);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/2k1p3/3pP3/3P2K1/8/8/8/8 w - - 0 1")).size() == 7);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/8/8/8/K7/P7/k7 b - - 0 1")).size() == 1);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/8/8/8/7K/7P/7k b - - 0 1")).size() == 1);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("K7/p7/k7/8/8/8/8/8 b - - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7K/7p/7k/8/8/8/8/8 b - - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/2k1p3/3pP3/3P2K1/8/8/8/8 b - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/8/8/8/4k3/4P3/4K3 w - - 0 1")).size() == 2);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("4k3/4p3/4K3/8/8/8/8/8 b - - 0 1")).size() == 2);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/7k/7p/7P/7K/8/8 w - - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/k7/p7/P7/K7/8/8 w - - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/3k4/3p4/3P4/3K4/8/8 w - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/3k4/3p4/8/3P4/3K4/8/8 w - - 0 1")).size() == 8);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/3k4/3p4/8/3P4/3K4/8 w - - 0 1")).size() == 8);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/3p4/8/3P4/8/8/7K w - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/7k/7p/7P/7K/8/8 b - - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/k7/p7/P7/K7/8/8 b - - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/3k4/3p4/3P4/3K4/8/8 b - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/3k4/3p4/8/3P4/3K4/8/8 b - - 0 1")).size() == 8);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/8/3k4/3p4/8/3P4/3K4/8 b - - 0 1")).size() == 8);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/3p4/8/3P4/8/8/7K b - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/3p4/8/8/3P4/8/8/K7 w - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/8/8/3p4/8/8/3P4/K7 w - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/8/7p/6P1/8/8/K7 w - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/7p/8/8/6P1/8/K7 w - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/8/6p1/7P/8/8/K7 w - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/6p1/8/8/7P/8/K7 w - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/8/3p4/4p3/8/8/7K w - - 0 1")).size() == 3);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/3p4/8/8/4P3/8/7K w - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/3p4/8/8/3P4/8/8/K7 b - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/8/8/3p4/8/8/3P4/K7 b - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/8/7p/6P1/8/8/K7 b - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/7p/8/8/6P1/8/K7 b - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/8/6p1/7P/8/8/K7 b - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/6p1/8/8/7P/8/K7 b - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/8/3p4/4p3/8/8/7K b - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/8/3p4/8/8/4P3/8/7K b - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/8/8/p7/1P6/8/8/7K w - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/8/8/p7/1P6/8/8/7K b - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/8/8/1p6/P7/8/8/7K w - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/8/8/1p6/P7/8/8/7K b - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/8/p7/8/8/1P6/8/7K w - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/8/p7/8/8/1P6/8/7K b - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/8/1p6/8/8/P7/8/7K w - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("7k/8/1p6/8/8/P7/8/7K b - - 0 1")).size() == 4);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/7p/8/8/8/8/6P1/K7 w - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/7p/8/8/8/8/6P1/K7 b - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/6p1/8/8/8/8/7P/K7 w - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("k7/6p1/8/8/8/8/7P/K7 b - - 0 1")).size() == 5);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/Pk6/8/8/8/8/6Kp/8 w - - 0 1")).size() == 11);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/Pk6/8/8/8/8/6Kp/8 b - - 0 1")).size() == 11);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("3k4/3pp3/8/8/8/8/3PP3/3K4 w - - 0 1")).size() == 7);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("3k4/3pp3/8/8/8/8/3PP3/3K4 b - - 0 1")).size() == 7);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/PPPk4/8/8/8/8/4Kppp/8 w - - 0 1")).size() == 18);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("8/PPPk4/8/8/8/8/4Kppp/8 b - - 0 1")).size() == 18);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("n1n5/1Pk5/8/8/8/8/5Kp1/5N1N w - - 0 1")).size() == 24);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("n1n5/1Pk5/8/8/8/8/5Kp1/5N1N b - - 0 1")).size() == 24);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("n1n5/PPPk4/8/8/8/8/4Kppp/5N1N w - - 0 1")).size() == 24);
    TEST_ASSERT(movegen::generateAllLegalMoves(Position::fromFen("n1n5/PPPk4/8/8/8/8/4Kppp/5N1N b - - 0 1")).size() == 24);
}

int main()
{
    testMoveGen();
    return 0;
    //build();
    //buildsmall();
    query();
    return 0;
    /*
    persistence::Database e("w:/catobase/.tmp");
    e.importPgns({
        "data/lichess_db_standard_rated_2013-01.pgn"
        }, GameLevel::Human, 2u * 1024u * 1024u * 1024u);
        */
    {
        auto t0 = std::chrono::high_resolution_clock::now();
        std::vector<std::filesystem::path> infiles = {
            "data/lichess_db_standard_rated_2013-01.pgn",
            "data/lichess_db_standard_rated_2013-02.pgn",
            "data/lichess_db_standard_rated_2013-03.pgn",
            "data/lichess_db_standard_rated_2013-04.pgn",
            "data/lichess_db_standard_rated_2013-05.pgn",
            "data/lichess_db_standard_rated_2013-06.pgn",
            "data/lichess_db_standard_rated_2013-07.pgn",
            "data/lichess_db_standard_rated_2013-08.pgn",
            "data/lichess_db_standard_rated_2013-09.pgn",
            "data/lichess_db_standard_rated_2013-10.pgn",
            "data/lichess_db_standard_rated_2013-11.pgn",
            "data/lichess_db_standard_rated_2013-12.pgn"
        };
        std::vector<std::pair<std::filesystem::path, std::filesystem::path>> files = {
            { "data/lichess_db_standard_rated_2013-01.pgn", "out/lichess_db_standard_rated_2013-01.bin" },
            { "data/lichess_db_standard_rated_2013-02.pgn", "out/lichess_db_standard_rated_2013-02.bin" },
            { "data/lichess_db_standard_rated_2013-03.pgn", "out/lichess_db_standard_rated_2013-03.bin" },
            { "data/lichess_db_standard_rated_2013-04.pgn", "out/lichess_db_standard_rated_2013-04.bin" },
            { "data/lichess_db_standard_rated_2013-05.pgn", "out/lichess_db_standard_rated_2013-05.bin" },
            { "data/lichess_db_standard_rated_2013-06.pgn", "out/lichess_db_standard_rated_2013-06.bin" },
            { "data/lichess_db_standard_rated_2013-07.pgn", "out/lichess_db_standard_rated_2013-07.bin" },
            { "data/lichess_db_standard_rated_2013-08.pgn", "out/lichess_db_standard_rated_2013-08.bin" },
            { "data/lichess_db_standard_rated_2013-09.pgn", "out/lichess_db_standard_rated_2013-09.bin" },
            { "data/lichess_db_standard_rated_2013-10.pgn", "out/lichess_db_standard_rated_2013-10.bin" },
            { "data/lichess_db_standard_rated_2013-11.pgn", "out/lichess_db_standard_rated_2013-11.bin" },
            { "data/lichess_db_standard_rated_2013-12.pgn", "out/lichess_db_standard_rated_2013-12.bin" }
        };

        // 81.8045 225962527, 3 530 664k space
        // TODO: since we're not io bound maybe try parallelising the for loop, make sort sequential, and limit memory per batch to maxMemory/concurrency
        std::atomic<std::size_t> numPositions = 0;
        std::for_each(std::execution::seq, std::begin(files), std::end(files), [&numPositions](const std::pair<std::filesystem::path, std::filesystem::path>& paths){
            numPositions += dumpPositions(paths.first, paths.second, 1'000'000'000, false);
        });

        /*
        // or maybe we are io bound?
        // 72.3017 225962527, 2 843 451k space (0.8054 ratio, expected is 0.8138)
        auto numPositions = dumpPositions(infiles, "out/lichess_db_standard_rated_2013_unique.bin", 4'000'000'000, true);
        */

        auto t1 = std::chrono::high_resolution_clock::now();
        std::cout << (t1 - t0).count() / 1e9f << ' ' << numPositions << '\n';
        return 0;
    }

    test::runCodingTests();

    //pgn::LazyPgnFileReader fr("data/philidor.pgn"); //6 505 484
    //pgn::LazyPgnFileReader fr("data/problem2.pgn");
    pgn::LazyPgnFileReader fr("data/lichess_db_standard_rated_2013-01.pgn"); // 121332 8242561 7019204
    if (!fr.isOpen())
    {
        std::cout << "Failed to open file.\n";
        return 1;
    }
    auto t0 = std::chrono::high_resolution_clock::now();
    robin_hood::unordered_node_map<PositionSignature, std::array<std::uint64_t, 3>> hist;
    //std::map<PositionSignature, std::array<std::uint64_t, 3>> hist;
    hist.reserve(10'000'000);
    int numGames = 0;
    int numPositions = 0;
    for (auto& game : fr)
    {
        ++numGames;
        //std::cout << game.tagSection() << '\n';
        for (auto& pos : game.positions())
        {
            /*
            print(hash(pos));
            std::cout << '\n';

            pos.print(std::cout);
            std::cout << '\n';
            */

            ++hist[PositionSignature(pos)][0];
            ++numPositions;
        }
        if (numGames % 1000 == 0)
        {
            std::cout << numPositions << '\t' << hist.size() << '\n';
        }
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << (t1-t0).count() / 1e9f << ' ' << numGames << ' ' << numPositions << ' ' << hist.size() << '\n';

    /*
    std::ofstream out("out/100000_2_ordered_0.bin", std::ios::out | std::ios::binary);
    header(out);
    int i = 0;
    for (const auto& [key, value] : hist)
    {
        if (i % 2 == 0)
        {
            dump(key, value, out);
        }

        ++i;
        if (i >= 100'000) break;
    }
    trailer(out);
    
    std::ofstream out2("out/100000_2_ordered_1.bin", std::ios::out | std::ios::binary);
    header(out2);
    i = 0;
    for (const auto& [key, value] : hist)
    {
        if (i % 2 == 1)
        {
            dump(key, value, out2);
        }

        ++i;
        if (i >= 100'000) break;
    }
    trailer(out2);
    */
    return 0;
}
