#include <iostream>
#include <iomanip>

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
    print(0x0000400040201008_bb);

    constexpr Square sq = H5;
    constexpr Bitboard occ = 0x38ddd8a535d2cbbd_bb;
    print(occ);
    print(bb::attacks(PieceType::Queen, sq, occ));
    /*
    std::cout << '\n';
    print(bb::detail::slidingAttacks<bb::detail::North>(sq, occ));
    std::cout << '\n';
    print(bb::detail::slidingAttacks<bb::detail::NorthEast>(sq, occ));
    std::cout << '\n';
    print(bb::detail::slidingAttacks<bb::detail::East>(sq, occ));
    std::cout << '\n';
    print(bb::detail::slidingAttacks<bb::detail::SouthEast>(sq, occ));
    std::cout << '\n';
    print(bb::detail::slidingAttacks<bb::detail::South>(sq, occ));
    std::cout << '\n';
    print(bb::detail::slidingAttacks<bb::detail::SouthWest>(sq, occ));
    std::cout << '\n';
    print(bb::detail::slidingAttacks<bb::detail::West>(sq, occ));
    std::cout << '\n';
    print(bb::detail::slidingAttacks<bb::detail::NorthWest>(sq, occ));
    std::cout << '\n';
    print(bb::pseudoAttacks(PieceType::Bishop, sq));
    */
}