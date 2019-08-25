#define _CRT_SECURE_NO_WARNINGS

#include <iostream>
#include <iomanip>
#include <filesystem>
#include <cstdio>
#include <memory>

#include "Bitboard.h"
#include "Enum.h"
#include "EnumArray.h"
#include "Intrinsics.h"
#include "Pgn.h"
#include "Position.h"
#include "San.h"

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


int main()
{
    pgn::LazyPgnFileReader fr("data/lichess_db_standard_rated_2013-01.pgn");
    int i = 0;
    for (auto& game : fr)
    {
        ++i;
        for (auto& pos : game.positions())
        {

        }
    }
    std::cout << i << '\n';
}
