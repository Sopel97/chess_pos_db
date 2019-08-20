#pragma once

#include "Chess.h"
#include "Enum.h"
#include "Intrinsics.h"

struct Bitboard
{
    // bits counted from the LSB
    // order is A1 A2 ... H7 H8
    // just like in Square

public:
    constexpr Bitboard() :
        m_squares(0)
    {
    }

    constexpr explicit Bitboard(Square sq) :
        m_squares(static_cast<std::uint64_t>(1ULL) << ordinal(sq))
    {
    }

    constexpr explicit Bitboard(Rank r) :
        m_squares(static_cast<std::uint64_t>(0xFFULL) << (ordinal(r) * 8))
    {
    }

    constexpr explicit Bitboard(File f) :
        m_squares(static_cast<std::uint64_t>(0x0101010101010101ULL) << ordinal(f))
    {
    }

    constexpr explicit Bitboard(Color c) :
        m_squares(c == Color::White ? 0xAA55AA55AA55AA55ULL : ~0xAA55AA55AA55AA55ULL)
    {
    }

private:
    constexpr explicit Bitboard(std::uint64_t bb) :
        m_squares(bb)
    {
    }

    // files A..file inclusive
    static constexpr std::uint64_t m_filesUpToBB[8]{
        0x0101010101010101ULL,
        0x0303030303030303ULL,
        0x0707070707070707ULL,
        0x0F0F0F0F0F0F0F0FULL,
        0x1F1F1F1F1F1F1F1FULL,
        0x3F3F3F3F3F3F3F3FULL,
        0x7F7F7F7F7F7F7F7FULL,
        0xFFFFFFFFFFFFFFFFULL
    };

public:

    static constexpr Bitboard none()
    {
        return Bitboard{};
    }

    static constexpr Bitboard all()
    {
        return ~none();
    }

    static constexpr Bitboard fromBits(std::uint64_t bits)
    {
        return Bitboard(bits);
    }

    // inclusive
    static constexpr Bitboard betweenFiles(File left, File right)
    {
        return Bitboard::fromBits(m_filesUpToBB[ordinal(right) - ordinal(left)] << ordinal(left));
    }

    constexpr bool isEmpty() const
    {
        return m_squares == 0;
    }

    constexpr bool isSet(Square sq) const
    {
        return !!((m_squares >> ordinal(sq)) & 1ull);
    }

    constexpr void set(Square sq)
    {
        *this |= Bitboard(sq);
    }

    constexpr void unset(Square sq)
    {
        *this &= ~(Bitboard(sq));
    }

    constexpr void toggle(Square sq)
    {
        *this ^= Bitboard(sq);
    }

    constexpr bool friend operator==(Bitboard lhs, Bitboard rhs) noexcept
    {
        return lhs.m_squares == rhs.m_squares;
    }

    constexpr Bitboard& operator+=(Offset offset)
    {
        if (offset.rank > 0)
        {
            m_squares <<= 8 * offset.rank;
        }
        else if (offset.rank < 0)
        {
            m_squares >>= 8 * offset.rank;
        }

        if (offset.file > 0)
        {
            const Bitboard mask = Bitboard::betweenFiles(fileA, fromOrdinal<File>(8 - offset.file));
            m_squares = (m_squares & mask.m_squares) << offset.file;
        }
        else if (offset.file < 0)
        {
            const Bitboard mask = Bitboard::betweenFiles(fromOrdinal<File>(-offset.file), fileH);
            m_squares = (m_squares & mask.m_squares) >> offset.file;
        }

        return *this;
    }

    constexpr Bitboard operator+(Offset offset) const
    {
        Bitboard bbCpy(*this);
        bbCpy += offset;
        return bbCpy;
    }

    constexpr Bitboard operator~() const
    {
        Bitboard bb = *this;
        bb.m_squares = ~m_squares;
        return bb;
    }

    constexpr Bitboard& operator^=(Color c)
    {
        m_squares ^= Bitboard(c).m_squares;
        return *this;
    }

    constexpr Bitboard& operator&=(Color c)
    {
        m_squares &= Bitboard(c).m_squares;
        return *this;
    }

    constexpr Bitboard& operator|=(Color c)
    {
        m_squares |= Bitboard(c).m_squares;
        return *this;
    }

    constexpr Bitboard operator^(Color c) const
    {
        Bitboard bb = *this;
        bb ^= c;
        return bb;
    }

    constexpr Bitboard operator&(Color c) const
    {
        Bitboard bb = *this;
        bb &= c;
        return bb;
    }

    constexpr Bitboard operator|(Color c) const
    {
        Bitboard bb = *this;
        bb |= c;
        return bb;
    }

    constexpr Bitboard& operator^=(Square sq)
    {
        m_squares ^= Bitboard(sq).m_squares;
        return *this;
    }

    constexpr Bitboard& operator&=(Square sq)
    {
        m_squares &= Bitboard(sq).m_squares;
        return *this;
    }

    constexpr Bitboard& operator|=(Square sq)
    {
        m_squares |= Bitboard(sq).m_squares;
        return *this;
    }

    constexpr Bitboard operator^(Square sq) const
    {
        Bitboard bb = *this;
        bb ^= sq;
        return bb;
    }

    constexpr Bitboard operator&(Square sq) const
    {
        Bitboard bb = *this;
        bb &= sq;
        return bb;
    }

    constexpr Bitboard operator|(Square sq) const
    {
        Bitboard bb = *this;
        bb |= sq;
        return bb;
    }

    constexpr Bitboard& operator^=(Bitboard rhs)
    {
        m_squares ^= rhs.m_squares;
        return *this;
    }

    constexpr Bitboard& operator&=(Bitboard rhs)
    {
        m_squares &= rhs.m_squares;
        return *this;
    }

    constexpr Bitboard& operator|=(Bitboard rhs)
    {
        m_squares |= rhs.m_squares;
        return *this;
    }

    constexpr Bitboard operator^(Bitboard sq) const
    {
        Bitboard bb = *this;
        bb ^= sq;
        return bb;
    }

    constexpr Bitboard operator&(Bitboard sq) const
    {
        Bitboard bb = *this;
        bb &= sq;
        return bb;
    }

    constexpr Bitboard operator|(Bitboard sq) const
    {
        Bitboard bb = *this;
        bb |= sq;
        return bb;
    }

    int count() const
    {
        return static_cast<int>(intrin::popcount(m_squares));
    }

    constexpr bool moreThanOne() const
    {
        return !!(m_squares & (m_squares - 1));
    }

    constexpr bool exactlyOne() const
    {
        return m_squares != 0 && !moreThanOne();
    }

    constexpr bool any() const
    {
        return !!m_squares;
    }

    // assumes the bitboard is not empty
    Square popFirst()
    {
        Square sq = fromOrdinal<Square>(intrin::lsb(m_squares));
        m_squares &= m_squares - 1;
        return sq;
    }

    Square first() const
    {
        return fromOrdinal<Square>(intrin::lsb(m_squares));
    }

    Square last() const
    {
        return fromOrdinal<Square>(intrin::msb(m_squares));
    }

    constexpr std::uint64_t bits() const
    {
        return m_squares;
    }

    constexpr Square first_constexpr() const
    {
        for (Square sq = A1; sq <= H8; ++sq)
        {
            if ((*this & sq).any())
            {
                return sq;
            }
        }
        return A1;
    }

    constexpr Square last_constexpr() const
    {
        for (Square sq = H8; sq >= A1; --sq)
        {
            if ((*this & sq).any())
            {
                return sq;
            }
        }
        return A1;
    }

    template <typename FuncT>
    void forEach(FuncT&& f) const
    {
        Bitboard bb = *this;

        while (!bb.isEmpty())
        {
            f(bb.popFirst());
        }
    }

    constexpr Bitboard& operator=(const Bitboard& other) = default;

private:
    std::uint64_t m_squares;
};

constexpr Bitboard operator""_bb(std::uint64_t bits)
{
    return Bitboard::fromBits(bits);
}

namespace bb
{
    namespace detail
    {
        static constexpr std::array<Offset, 8> knightOffsets{ { {-1, -2}, {-1, 2}, {1, -2}, {1, 2}, {-2, -1}, {-2, 1}, {2, -1}, {2, 1} } };

        enum Direction
        {
            North = 0,
            NorthEast,
            East,
            SouthEast,
            South,
            SouthWest,
            West,
            NorthWest
        };

        constexpr std::array<Offset, 8> offsets = { {
            { 0, 1 },
            { 1, 1 },
            { 1, 0 },
            { 1, -1 },
            { 0, -1 },
            { -1, -1 },
            { -1, 0 },
            { -1, 1 }
        } };

        static constexpr std::array<Offset, 4> bishopOffsets{
            offsets[NorthEast],
            offsets[SouthEast],
            offsets[SouthWest],
            offsets[NorthWest]
        };
        static constexpr std::array<Offset, 4> rookOffsets{
            offsets[North],
            offsets[East],
            offsets[South],
            offsets[West]
        };

        static constexpr std::array<Bitboard, cardinality<Square>()> generatePseudoAttacks_Pawn()
        {
            // pseudo attacks don't make sense for pawns
            return {};
        }

        static constexpr std::array<Bitboard, cardinality<Square>()> generatePseudoAttacks_Knight()
        {
            std::array<Bitboard, cardinality<Square>()> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                Bitboard bb{};

                for (auto&& offset : knightOffsets)
                {
                    const SquareCoords toSq = fromSq.coords() + offset;
                    if (toSq.isOk())
                    {
                        bb |= Square(toSq);
                    }
                }

                bbs[ordinal(fromSq)] = bb;
            }

            return bbs;
        }

        static constexpr Bitboard generateSliderPseudoAttacks(const std::array<Offset, 4>& offsets, Square fromSq)
        {
            Bitboard bb{};

            for (auto&& offset : offsets)
            {
                SquareCoords fromSqC = fromSq.coords();

                for (;;)
                {
                    fromSqC += offset;

                    if (!fromSqC.isOk())
                    {
                        break;
                    }

                    bb |= Square(fromSqC);
                }
            }

            return bb;
        }

        static constexpr std::array<Bitboard, cardinality<Square>()> generatePseudoAttacks_Bishop()
        {
            std::array<Bitboard, cardinality<Square>()> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[ordinal(fromSq)] = generateSliderPseudoAttacks(bishopOffsets, fromSq);
            }

            return bbs;
        }

        static constexpr std::array<Bitboard, cardinality<Square>()> generatePseudoAttacks_Rook()
        {
            std::array<Bitboard, cardinality<Square>()> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[ordinal(fromSq)] = generateSliderPseudoAttacks(rookOffsets, fromSq);
            }

            return bbs;
        }

        static constexpr std::array<Bitboard, cardinality<Square>()> generatePseudoAttacks_Queen()
        {
            std::array<Bitboard, cardinality<Square>()> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[ordinal(fromSq)] =
                    generateSliderPseudoAttacks(bishopOffsets, fromSq)
                    | generateSliderPseudoAttacks(rookOffsets, fromSq);
            }

            return bbs;
        }

        static constexpr std::array<Bitboard, cardinality<Square>()> generatePseudoAttacks_King()
        {
            std::array<Bitboard, cardinality<Square>()> bbs{};

            for (Square sq = A1; sq != Square::none(); ++sq)
            {
                const Bitboard bbsq(sq);
                const Bitboard bbh = bbsq | (bbsq + Offset{ 1, 0 }) | (bbsq + Offset{ -1, 0 }); // smear horizontally
                const Bitboard bb = bbh | (bbh + Offset{ 0, 1 }) | (bbh + Offset{ 0, -1 }); // smear vertically
                bbs[ordinal(sq)] = bb & ~bbsq; // don't include the king square
            }

            return bbs;
        }

        static constexpr auto generatePseudoAttacks()
        {
            return std::array<std::array<Bitboard, cardinality<Square>()>, cardinality<PieceType>()>{
                generatePseudoAttacks_Pawn(),
                generatePseudoAttacks_Knight(),
                generatePseudoAttacks_Bishop(),
                generatePseudoAttacks_Rook(),
                generatePseudoAttacks_Queen(),
                generatePseudoAttacks_King()
            };
        }

        constexpr auto pseudoAttacks = generatePseudoAttacks();

        static constexpr Bitboard generatePositiveRayAttacks(Direction dir, Square fromSq)
        {
            Bitboard bb{};

            const auto offset = offsets[dir];
            SquareCoords fromSqC = fromSq.coords();
            for (;;)
            {
                fromSqC += offset;

                if (!fromSqC.isOk())
                {
                    break;
                }

                bb |= Square(fromSqC);
            }

            return bb;
        }

        // classical slider move generation approach https://www.chessprogramming.org/Classical_Approach

        static constexpr std::array<Bitboard, cardinality<Square>()> generatePositiveRayAttacks(Direction dir)
        {
            std::array<Bitboard, cardinality<Square>()> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[ordinal(fromSq)] = generatePositiveRayAttacks(dir, fromSq);
            }

            return bbs;
        }

        static constexpr auto generatePositiveRayAttacks()
        {
            std::array<std::array<Bitboard, cardinality<Square>()>, 8> bbs{};

            bbs[North] = generatePositiveRayAttacks(North);
            bbs[NorthEast] = generatePositiveRayAttacks(NorthEast);
            bbs[East] = generatePositiveRayAttacks(East);
            bbs[SouthEast] = generatePositiveRayAttacks(SouthEast);
            bbs[South] = generatePositiveRayAttacks(South);
            bbs[SouthWest] = generatePositiveRayAttacks(SouthWest);
            bbs[West] = generatePositiveRayAttacks(West);
            bbs[NorthWest] = generatePositiveRayAttacks(NorthWest);

            return bbs;
        }

        constexpr auto positiveRayAttacks = generatePositiveRayAttacks();

        template <Direction DirV>
        constexpr Bitboard slidingAttacks(Square sq, Bitboard occupied)
        {
            Bitboard attacks = detail::positiveRayAttacks[DirV][ordinal(sq)];

            if constexpr (DirV == NorthWest || DirV == North || DirV == NorthEast || DirV == East)
            {
                Bitboard blocker = (attacks & occupied) | H8; // set highest bit (H8) so msb never fails
                return attacks ^ positiveRayAttacks[DirV][ordinal(blocker.first_constexpr())];
            }
            else
            {
                Bitboard blocker = (attacks & occupied) | A1;
                return attacks ^ positiveRayAttacks[DirV][ordinal(blocker.last_constexpr())];
            }
        }
    }

    constexpr Bitboard pseudoAttacks(PieceType pt, Square sq)
    {
        return detail::pseudoAttacks[ordinal(pt)][ordinal(sq)];
    }

    constexpr Bitboard attacks(PieceType pt, Square sq, Bitboard occupied)
    {
        switch (pt)
        {
        case PieceType::Bishop:
            return
                  detail::slidingAttacks<detail::NorthEast>( sq, occupied)
                | detail::slidingAttacks<detail::SouthEast>(sq, occupied)
                | detail::slidingAttacks<detail::SouthWest>(sq, occupied)
                | detail::slidingAttacks<detail::NorthWest>(sq, occupied);
        case PieceType::Rook:           
            return                      
                  detail::slidingAttacks<detail::North>(sq, occupied)
                | detail::slidingAttacks<detail::East>(sq, occupied)
                | detail::slidingAttacks<detail::South>(sq, occupied)
                | detail::slidingAttacks<detail::West>(sq, occupied);
        case PieceType::Queen:          
            return                      
                  detail::slidingAttacks<detail::North>(sq, occupied)
                | detail::slidingAttacks<detail::NorthEast>(sq, occupied)
                | detail::slidingAttacks<detail::East>(sq, occupied)
                | detail::slidingAttacks<detail::SouthEast>(sq, occupied)
                | detail::slidingAttacks<detail::South>(sq, occupied)
                | detail::slidingAttacks<detail::SouthWest>(sq, occupied)
                | detail::slidingAttacks<detail::West>(sq, occupied)
                | detail::slidingAttacks<detail::NorthWest>(sq, occupied);
        default:
            return pseudoAttacks(pt, sq);
        }
    }

    // random test cases generated with stockfish

    static_assert(attacks(PieceType::Bishop, C7, 0x401f7ac78bc80f1c_bb) == 0x0a000a0000000000_bb);
    static_assert(attacks(PieceType::Bishop, F6, 0xf258d22d4db91392_bb) == 0x0050005088000000_bb);
    static_assert(attacks(PieceType::Bishop, B1, 0x67a7aabe10d172d6_bb) == 0x0000000010080500_bb);
    static_assert(attacks(PieceType::Bishop, A3, 0x05d07b7d1e8de386_bb) == 0x0000000002000200_bb);
    static_assert(attacks(PieceType::Bishop, B5, 0x583c502c832e0a3a_bb) == 0x0008050005080000_bb);
    static_assert(attacks(PieceType::Bishop, B3, 0x8f9fccba4388a61f_bb) == 0x0000000805000500_bb);
    static_assert(attacks(PieceType::Bishop, A2, 0x8b3a26b7aa4bcecb_bb) == 0x0000000000020002_bb);
    static_assert(attacks(PieceType::Bishop, B5, 0xdb696ab700feb090_bb) == 0x0008050005080000_bb);
    static_assert(attacks(PieceType::Bishop, H4, 0x6b5bd57a3c9113ef_bb) == 0x0000004000402010_bb);
    static_assert(attacks(PieceType::Bishop, H6, 0x3fc97b87bed94159_bb) == 0x0040004020000000_bb);
    static_assert(attacks(PieceType::Bishop, H2, 0x51efc5d2498d7506_bb) == 0x0000001020400040_bb);
    static_assert(attacks(PieceType::Bishop, C8, 0x2a327e8f39fc19a6_bb) == 0x000a100000000000_bb);
    static_assert(attacks(PieceType::Bishop, H2, 0x32c51436b7c00275_bb) == 0x0000000000400040_bb);
    static_assert(attacks(PieceType::Bishop, F6, 0xf7c35c861856282a_bb) == 0x0850005088000000_bb);
    static_assert(attacks(PieceType::Bishop, B7, 0x14a93ca1d9bcea61_bb) == 0x0500050000000000_bb);
    static_assert(attacks(PieceType::Bishop, F4, 0x41dbe94941a43d12_bb) == 0x0000085000508800_bb);

    static_assert(attacks(PieceType::Rook, B7, 0x957955653083196e_bb) == 0x020d020202020000_bb);
    static_assert(attacks(PieceType::Rook, E8, 0x702751d1bb724213_bb) == 0x2f10100000000000_bb);
    static_assert(attacks(PieceType::Rook, E3, 0x884bb2027e9ac7b0_bb) == 0x0000000010e81010_bb);
    static_assert(attacks(PieceType::Rook, A3, 0x0ba88011cd101288_bb) == 0x00000000011e0101_bb);
    static_assert(attacks(PieceType::Rook, A5, 0xb23cb1552b043b6e_bb) == 0x0000010601000000_bb);
    static_assert(attacks(PieceType::Rook, F1, 0xe838ff59b1c9d964_bb) == 0x000000002020205c_bb);
    static_assert(attacks(PieceType::Rook, B1, 0x26ebdcf08553011a_bb) == 0x000000000002020d_bb);
    static_assert(attacks(PieceType::Rook, G8, 0x9ed34d63df99a685_bb) == 0xb040000000000000_bb);
    static_assert(attacks(PieceType::Rook, D3, 0x5c7fc5fc683a1085_bb) == 0x0000000008160808_bb);
    static_assert(attacks(PieceType::Rook, G4, 0x4c3fb0ceb4adb6b9_bb) == 0x00000040a0404040_bb);
    static_assert(attacks(PieceType::Rook, C3, 0xec97f42c55bc9f40_bb) == 0x00000000040b0400_bb);
    static_assert(attacks(PieceType::Rook, A6, 0xc149bd468ac1ac86_bb) == 0x0001060101010000_bb);
    static_assert(attacks(PieceType::Rook, F6, 0xb906a73e05a92c74_bb) == 0x2020dc2000000000_bb);
    static_assert(attacks(PieceType::Rook, E1, 0x7ca12fb5b05b5c4d_bb) == 0x0000000000001068_bb);
    static_assert(attacks(PieceType::Rook, F1, 0xc27697252e02cb81_bb) == 0x00000000202020df_bb);
    static_assert(attacks(PieceType::Rook, D4, 0x98d3daaa3b2e8562_bb) == 0x0000000816080000_bb);

    static_assert(attacks(PieceType::Queen, F1, 0x45e0c63e93fc6383_bb) == 0x00000000000870de_bb);
    static_assert(attacks(PieceType::Queen, H5, 0x38ddd8a535d2cbbd_bb) == 0x0000c060c0a01008_bb);
    static_assert(attacks(PieceType::Queen, G2, 0x6f23d32e2a0fd7fa_bb) == 0x0000404850e0b0e0_bb);
    static_assert(attacks(PieceType::Queen, H8, 0x360369eda9c0e07d_bb) == 0x60c0a08000000000_bb);
    static_assert(attacks(PieceType::Queen, G7, 0x48bbb7a741e6ddd9_bb) == 0xe0a0e04040000000_bb);
    static_assert(attacks(PieceType::Queen, F7, 0x5de152345f136375_bb) == 0x705f702000000000_bb);
    static_assert(attacks(PieceType::Queen, D8, 0xdc22b9f9f9d7538d_bb) == 0x141c2a0100000000_bb);
    static_assert(attacks(PieceType::Queen, H4, 0x05a6f16b79bbd6e9_bb) == 0x000080c040c02010_bb);
    static_assert(attacks(PieceType::Queen, H3, 0xbc87a781b47462ce_bb) == 0x04081020c040c080_bb);
    static_assert(attacks(PieceType::Queen, B5, 0x6c469ad3cba9b91a_bb) == 0x1008071d07080000_bb);
    static_assert(attacks(PieceType::Queen, B1, 0xe8c41087c07c91fc_bb) == 0x00000002020a0705_bb);
    static_assert(attacks(PieceType::Queen, H1, 0xfaec2f3c1e29110d_bb) == 0x0080808080a0c078_bb);
    static_assert(attacks(PieceType::Queen, E6, 0x7cc1b5019ea1196d_bb) == 0x54382c3854800000_bb);
    static_assert(attacks(PieceType::Queen, H6, 0x96b30966f70500d8_bb) == 0x20c078c080000000_bb);
    static_assert(attacks(PieceType::Queen, B5, 0x74a51eba09dd373d_bb) == 0x0000070d070a0200_bb);
    static_assert(attacks(PieceType::Queen, F7, 0xded20384ba4b0368_bb) == 0x705070a824020000_bb);

    constexpr Bitboard lightSquares = Bitboard(Color::White);
    constexpr Bitboard darkSquares = Bitboard(Color::Black);

    constexpr Bitboard fileA = Bitboard(::fileA);
    constexpr Bitboard fileB = Bitboard(::fileB);
    constexpr Bitboard fileC = Bitboard(::fileC);
    constexpr Bitboard fileD = Bitboard(::fileD);
    constexpr Bitboard fileE = Bitboard(::fileE);
    constexpr Bitboard fileF = Bitboard(::fileF);
    constexpr Bitboard fileG = Bitboard(::fileG);
    constexpr Bitboard fileH = Bitboard(::fileH);

    constexpr Bitboard rank1 = Bitboard(::rank1);
    constexpr Bitboard rank2 = Bitboard(::rank2);
    constexpr Bitboard rank3 = Bitboard(::rank3);
    constexpr Bitboard rank4 = Bitboard(::rank4);
    constexpr Bitboard rank5 = Bitboard(::rank5);
    constexpr Bitboard rank6 = Bitboard(::rank6);
    constexpr Bitboard rank7 = Bitboard(::rank7);
    constexpr Bitboard rank8 = Bitboard(::rank8);
}
