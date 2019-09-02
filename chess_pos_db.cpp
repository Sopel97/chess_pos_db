#define _CRT_SECURE_NO_WARNINGS

#include <iostream>
#include <iomanip>
#include <filesystem>
#include <cstdio>
#include <memory>
#include <unordered_map>

#include "Bitboard.h"
#include "Enum.h"
#include "EnumArray.h"
#include "Intrinsics.h"
#include "Pgn.h"
#include "Position.h"
#include "PositionSignature.h"
#include "San.h"

#include "lib/xxhash/xxhash_cpp.h"

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

int main()
{
    test::runCodingTests();

    //pgn::LazyPgnFileReader fr("data/philidor.pgn"); //6 505 484
    //pgn::LazyPgnFileReader fr("data/problem2.pgn");
    pgn::LazyPgnFileReader fr("data/lichess_db_standard_rated_2013-01.pgn"); // 121332 8242561 7019204
    if (!fr.isOpen())
    {
        std::cout << "Failed to open file.\n";
        return 1;
    }

    std::unordered_map<PositionSignature, std::uint64_t> hist;
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

            ++hist[PositionSignature(pos)];
            ++numPositions;
        }
    }
    std::cout << numGames << ' ' << numPositions << ' ' << hist.size() << '\n';
    return 0;
}
