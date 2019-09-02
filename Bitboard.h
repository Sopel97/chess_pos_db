#pragma once

#include "Assert.h"
#include "Chess.h"
#include "Enum.h"
#include "EnumArray.h"
#include "Intrinsics.h"

struct BitboardIterator
{
    using value_type = Square;
    using difference_type = std::ptrdiff_t;
    using reference = Square;
    using iterator_category = std::input_iterator_tag;
    using pointer = const Square*;

    constexpr BitboardIterator() noexcept :
        m_squares(0)
    {
    }

    constexpr BitboardIterator(std::uint64_t v) noexcept :
        m_squares(v)
    {
    }

    constexpr BitboardIterator(const BitboardIterator&) = default;
    constexpr BitboardIterator(BitboardIterator&&) = default;
    constexpr BitboardIterator& operator=(const BitboardIterator&) = default;
    constexpr BitboardIterator& operator=(BitboardIterator&&) = default;

    [[nodiscard]] constexpr bool friend operator==(BitboardIterator lhs, BitboardIterator rhs) noexcept
    {
        return lhs.m_squares == rhs.m_squares;
    }

    [[nodiscard]] constexpr bool friend operator!=(BitboardIterator lhs, BitboardIterator rhs) noexcept
    {
        return lhs.m_squares != rhs.m_squares;
    }

    [[nodiscard]] INTRIN_CONSTEXPR Square operator*() const
    {
        return first();
    }

    constexpr BitboardIterator& operator++() noexcept
    {
        popFirst();
        return *this;
    }

private:
    std::uint64_t m_squares;

    constexpr void popFirst() noexcept
    {
        m_squares &= m_squares - 1;
    }

    [[nodiscard]] INTRIN_CONSTEXPR Square first() const
    {
        ASSERT(m_squares != 0);

        return fromOrdinal<Square>(intrin::lsb(m_squares));
    }
};

struct Bitboard
{
    // bits counted from the LSB
    // order is A1 A2 ... H7 H8
    // just like in Square

public:
    constexpr Bitboard() noexcept :
        m_squares(0)
    {
    }

private:
    constexpr explicit Bitboard(Square sq) noexcept :
        m_squares(static_cast<std::uint64_t>(1ULL) << ordinal(sq))
    {
        ASSERT(sq.isOk());
    }

    constexpr explicit Bitboard(Rank r) noexcept :
        m_squares(static_cast<std::uint64_t>(0xFFULL) << (ordinal(r) * 8))
    {
    }

    constexpr explicit Bitboard(File f) noexcept :
        m_squares(static_cast<std::uint64_t>(0x0101010101010101ULL) << ordinal(f))
    {
    }

    constexpr explicit Bitboard(Color c) noexcept :
        m_squares(c == Color::White ? 0xAA55AA55AA55AA55ULL : ~0xAA55AA55AA55AA55ULL)
    {
    }

    constexpr explicit Bitboard(std::uint64_t bb) noexcept :
        m_squares(bb)
    {
    }

    // files A..file inclusive
    static constexpr EnumArray<std::uint64_t, File> m_filesUpToBB{
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

    [[nodiscard]] static constexpr Bitboard none()
    {
        return Bitboard{};
    }

    [[nodiscard]] static constexpr Bitboard all()
    {
        return ~none();
    }

    [[nodiscard]] static constexpr Bitboard square(Square sq)
    {
        return Bitboard(sq);
    }

    [[nodiscard]] static constexpr Bitboard file(File f)
    {
        return Bitboard(f);
    }

    [[nodiscard]] static constexpr Bitboard rank(Rank r)
    {
        return Bitboard(r);
    }

    [[nodiscard]] static constexpr Bitboard color(Color c)
    {
        return Bitboard(c);
    }

    [[nodiscard]] static constexpr Bitboard fromBits(std::uint64_t bits)
    {
        return Bitboard(bits);
    }

    // inclusive
    [[nodiscard]] static constexpr Bitboard betweenFiles(File left, File right)
    {
        ASSERT(left <= right);

        return Bitboard::fromBits(m_filesUpToBB[right - ordinal(left)] << ordinal(left));
    }

    [[nodiscard]] constexpr bool isEmpty() const
    {
        return m_squares == 0;
    }

    [[nodiscard]] constexpr bool isSet(Square sq) const
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

    [[nodiscard]] constexpr BitboardIterator begin() const
    {
        return BitboardIterator(m_squares);
    }

    [[nodiscard]] constexpr BitboardIterator end() const
    {
        return BitboardIterator{};
    }

    [[nodiscard]] constexpr BitboardIterator cbegin() const
    {
        return BitboardIterator(m_squares);
    }

    [[nodiscard]] constexpr BitboardIterator cend() const
    {
        return BitboardIterator{};
    }

    [[nodiscard]] constexpr bool friend operator==(Bitboard lhs, Bitboard rhs) noexcept
    {
        return lhs.m_squares == rhs.m_squares;
    }

    [[nodiscard]] constexpr bool friend operator!=(Bitboard lhs, Bitboard rhs) noexcept
    {
        return lhs.m_squares != rhs.m_squares;
    }

    constexpr Bitboard& operator+=(Offset offset)
    {
        if (offset.ranks > 0)
        {
            m_squares <<= 8 * offset.ranks;
        }
        else if (offset.ranks < 0)
        {
            m_squares >>= 8 * offset.ranks;
        }

        if (offset.files > 0)
        {
            const Bitboard mask = Bitboard::betweenFiles(fileA, fromOrdinal<File>(8 - offset.files));
            m_squares = (m_squares & mask.m_squares) << offset.files;
        }
        else if (offset.files < 0)
        {
            const Bitboard mask = Bitboard::betweenFiles(fromOrdinal<File>(-offset.files), fileH);
            m_squares = (m_squares & mask.m_squares) >> offset.files;
        }

        return *this;
    }

    [[nodiscard]] constexpr Bitboard operator+(Offset offset) const
    {
        Bitboard bbCpy(*this);
        bbCpy += offset;
        return bbCpy;
    }

    [[nodiscard]] constexpr Bitboard operator~() const
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

    [[nodiscard]] constexpr Bitboard operator^(Color c) const
    {
        Bitboard bb = *this;
        bb ^= c;
        return bb;
    }

    [[nodiscard]] constexpr Bitboard operator&(Color c) const
    {
        Bitboard bb = *this;
        bb &= c;
        return bb;
    }

    [[nodiscard]] constexpr Bitboard operator|(Color c) const
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

    [[nodiscard]] constexpr Bitboard operator^(Square sq) const
    {
        Bitboard bb = *this;
        bb ^= sq;
        return bb;
    }

    [[nodiscard]] constexpr Bitboard operator&(Square sq) const
    {
        Bitboard bb = *this;
        bb &= sq;
        return bb;
    }

    [[nodiscard]] constexpr Bitboard operator|(Square sq) const
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

    [[nodiscard]] constexpr Bitboard operator^(Bitboard sq) const
    {
        Bitboard bb = *this;
        bb ^= sq;
        return bb;
    }

    [[nodiscard]] constexpr Bitboard operator&(Bitboard sq) const
    {
        Bitboard bb = *this;
        bb &= sq;
        return bb;
    }

    [[nodiscard]] constexpr Bitboard operator|(Bitboard sq) const
    {
        Bitboard bb = *this;
        bb |= sq;
        return bb;
    }

    [[nodiscard]] INTRIN_CONSTEXPR int count() const
    {
        return static_cast<int>(intrin::popcount(m_squares));
    }

    [[nodiscard]] constexpr bool moreThanOne() const
    {
        return !!(m_squares & (m_squares - 1));
    }

    [[nodiscard]] constexpr bool exactlyOne() const
    {
        return m_squares != 0 && !moreThanOne();
    }

    [[nodiscard]] constexpr bool any() const
    {
        return !!m_squares;
    }

    [[nodiscard]] INTRIN_CONSTEXPR Square first() const
    {
        ASSERT(m_squares != 0);

        return fromOrdinal<Square>(intrin::lsb(m_squares));
    }

    [[nodiscard]] INTRIN_CONSTEXPR Square last() const
    {
        ASSERT(m_squares != 0);

        return fromOrdinal<Square>(intrin::msb(m_squares));
    }

    [[nodiscard]] constexpr std::uint64_t bits() const
    {
        return m_squares;
    }

    // assumes the bitboard is not empty
    constexpr void popFirst()
    {
        m_squares &= m_squares - 1;
    }

    constexpr Bitboard& operator=(const Bitboard& other) = default;

private:
    std::uint64_t m_squares;
};

[[nodiscard]] constexpr Bitboard operator""_bb(std::uint64_t bits)
{
    return Bitboard::fromBits(bits);
}

namespace bb
{
    [[nodiscard]] constexpr Bitboard square(Square sq)
    {
        return Bitboard::square(sq);
    }

    [[nodiscard]] constexpr Bitboard rank(Rank rank)
    {
        return Bitboard::rank(rank);
    }

    [[nodiscard]] constexpr Bitboard file(File file)
    {
        return Bitboard::file(file);
    }

    [[nodiscard]] constexpr Bitboard color(Color c)
    {
        return Bitboard::color(c);
    }

    constexpr Bitboard lightSquares = bb::color(Color::White);
    constexpr Bitboard darkSquares = bb::color(Color::Black);

    constexpr Bitboard fileA = bb::file(::fileA);
    constexpr Bitboard fileB = bb::file(::fileB);
    constexpr Bitboard fileC = bb::file(::fileC);
    constexpr Bitboard fileD = bb::file(::fileD);
    constexpr Bitboard fileE = bb::file(::fileE);
    constexpr Bitboard fileF = bb::file(::fileF);
    constexpr Bitboard fileG = bb::file(::fileG);
    constexpr Bitboard fileH = bb::file(::fileH);

    constexpr Bitboard rank1 = bb::rank(::rank1);
    constexpr Bitboard rank2 = bb::rank(::rank2);
    constexpr Bitboard rank3 = bb::rank(::rank3);
    constexpr Bitboard rank4 = bb::rank(::rank4);
    constexpr Bitboard rank5 = bb::rank(::rank5);
    constexpr Bitboard rank6 = bb::rank(::rank6);
    constexpr Bitboard rank7 = bb::rank(::rank7);
    constexpr Bitboard rank8 = bb::rank(::rank8);

    namespace detail
    {
        static constexpr std::array<Offset, 8> knightOffsets{ { {-1, -2}, {-1, 2}, {1, -2}, {1, 2}, {-2, -1}, {-2, 1}, {2, -1}, {2, 1} } };
        static constexpr std::array<Offset, 8> kingOffsets{ { {-1, -1}, {-1, 0}, {-1, 1}, {0, -1}, {0, 1}, {1, -1}, {1, 0}, {1, 1} } };

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

        [[nodiscard]] static constexpr EnumArray<Bitboard, Square> generatePseudoAttacks_Pawn()
        {
            // pseudo attacks don't make sense for pawns
            return {};
        }

        [[nodiscard]] static constexpr EnumArray<Bitboard, Square> generatePseudoAttacks_Knight()
        {
            EnumArray<Bitboard, Square> bbs{};

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

                bbs[fromSq] = bb;
            }

            return bbs;
        }

        [[nodiscard]] static constexpr Bitboard generateSliderPseudoAttacks(const std::array<Offset, 4>& offsets, Square fromSq)
        {
            ASSERT(fromSq.isOk());

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

        [[nodiscard]] static constexpr EnumArray<Bitboard, Square> generatePseudoAttacks_Bishop()
        {
            EnumArray<Bitboard, Square> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generateSliderPseudoAttacks(bishopOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static constexpr EnumArray<Bitboard, Square> generatePseudoAttacks_Rook()
        {
            EnumArray<Bitboard, Square> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generateSliderPseudoAttacks(rookOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static constexpr EnumArray<Bitboard, Square> generatePseudoAttacks_Queen()
        {
            EnumArray<Bitboard, Square> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] =
                    generateSliderPseudoAttacks(bishopOffsets, fromSq)
                    | generateSliderPseudoAttacks(rookOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static constexpr EnumArray<Bitboard, Square> generatePseudoAttacks_King()
        {
            EnumArray<Bitboard, Square> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                Bitboard bb{};

                for (auto&& offset : kingOffsets)
                {
                    const SquareCoords toSq = fromSq.coords() + offset;
                    if (toSq.isOk())
                    {
                        bb |= Square(toSq);
                    }
                }

                bbs[fromSq] = bb;
            }

            return bbs;
        }

        [[nodiscard]] static constexpr auto generatePseudoAttacks()
        {
            return EnumArray2<Bitboard, PieceType, Square>{
                generatePseudoAttacks_Pawn(),
                generatePseudoAttacks_Knight(),
                generatePseudoAttacks_Bishop(),
                generatePseudoAttacks_Rook(),
                generatePseudoAttacks_Queen(),
                generatePseudoAttacks_King()
            };
        }

        // NOTE: removing constexpr reduces compile times
        static constexpr auto pseudoAttacks = generatePseudoAttacks();

        [[nodiscard]] static constexpr Bitboard generatePositiveRayAttacks(Direction dir, Square fromSq)
        {
            ASSERT(fromSq.isOk());

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

        [[nodiscard]] static constexpr EnumArray<Bitboard, Square> generatePositiveRayAttacks(Direction dir)
        {
            EnumArray<Bitboard, Square> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generatePositiveRayAttacks(dir, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static constexpr auto generatePositiveRayAttacks()
        {
            std::array<EnumArray<Bitboard, Square>, 8> bbs{};

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

        static constexpr auto positiveRayAttacks = generatePositiveRayAttacks();

        template <Direction DirV>
        [[nodiscard]] constexpr Bitboard slidingAttacks(Square sq, Bitboard occupied)
        {
            ASSERT(sq.isOk());

            Bitboard attacks = detail::positiveRayAttacks[DirV][sq];

            if constexpr (DirV == NorthWest || DirV == North || DirV == NorthEast || DirV == East)
            {
                Bitboard blocker = (attacks & occupied) | H8; // set highest bit (H8) so msb never fails
                return attacks ^ positiveRayAttacks[DirV][blocker.first()];
            }
            else
            {
                Bitboard blocker = (attacks & occupied) | A1;
                return attacks ^ positiveRayAttacks[DirV][blocker.last()];
            }
        }
    }

    template <PieceType PieceTypeV>
    [[nodiscard]] constexpr Bitboard pseudoAttacks(Square sq)
    {
        static_assert(PieceTypeV != PieceType::None && PieceTypeV != PieceType::Pawn);

        ASSERT(sq.isOk());

        return detail::pseudoAttacks[PieceTypeV][sq];
    }

    [[nodiscard]] constexpr Bitboard pseudoAttacks(PieceType pt, Square sq)
    {
        ASSERT(sq.isOk());

        return detail::pseudoAttacks[pt][sq];
    }

    template <PieceType PieceTypeV>
    [[nodiscard]] constexpr Bitboard attacks(Square sq, Bitboard occupied)
    {
        static_assert(PieceTypeV != PieceType::None && PieceTypeV != PieceType::Pawn);

        ASSERT(sq.isOk());

        if constexpr (PieceTypeV == PieceType::Bishop)
        {
            return
                detail::slidingAttacks<detail::NorthEast>(sq, occupied)
                | detail::slidingAttacks<detail::SouthEast>(sq, occupied)
                | detail::slidingAttacks<detail::SouthWest>(sq, occupied)
                | detail::slidingAttacks<detail::NorthWest>(sq, occupied);
        }
        else if constexpr (PieceTypeV == PieceType::Rook)
        {
            return
                detail::slidingAttacks<detail::North>(sq, occupied)
                | detail::slidingAttacks<detail::East>(sq, occupied)
                | detail::slidingAttacks<detail::South>(sq, occupied)
                | detail::slidingAttacks<detail::West>(sq, occupied);
        }
        else if constexpr (PieceTypeV == PieceType::Queen)
        {
            return
                detail::slidingAttacks<detail::North>(sq, occupied)
                | detail::slidingAttacks<detail::NorthEast>(sq, occupied)
                | detail::slidingAttacks<detail::East>(sq, occupied)
                | detail::slidingAttacks<detail::SouthEast>(sq, occupied)
                | detail::slidingAttacks<detail::South>(sq, occupied)
                | detail::slidingAttacks<detail::SouthWest>(sq, occupied)
                | detail::slidingAttacks<detail::West>(sq, occupied)
                | detail::slidingAttacks<detail::NorthWest>(sq, occupied);
        }
        else
        {
            return pseudoAttacks<PieceTypeV>(sq);
        }
    }

    [[nodiscard]] constexpr Bitboard attacks(PieceType pt, Square sq, Bitboard occupied)
    {
        ASSERT(sq.isOk());

        switch (pt)
        {
        case PieceType::Bishop:
            return attacks<PieceType::Bishop>(sq, occupied);
        case PieceType::Rook:
            return attacks<PieceType::Rook>(sq, occupied);
        case PieceType::Queen:
            return attacks<PieceType::Queen>(sq, occupied);
        default:
            return pseudoAttacks(pt, sq);
        }
    }

    // random test cases generated with stockfish

#if defined(USE_CONSTEXPR_INTRINSICS)
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
#endif
}
