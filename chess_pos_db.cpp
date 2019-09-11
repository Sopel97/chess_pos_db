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

int main()
{
    persistence::LocalStorageFormat e("w:/catobase/.tmp");
    e.importPgn("data/lichess_db_standard_rated_2013-01.pgn", GameLevel::Human, 256'000'000);
    return 0;
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
