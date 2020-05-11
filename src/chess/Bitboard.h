#pragma once

#include "Chess.h"

#include "enum/Enum.h"
#include "enum/EnumArray.h"

#include "intrin/Intrinsics.h"

#include "util/ArithmeticUtility.h"
#include "util/Assert.h"

#include <cstdint>
#include <iterator>

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
    // order is A1 B2 ... G8 H8
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
    static constexpr EnumArray<File, std::uint64_t> m_filesUpToBB{
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

        if (left == fileA)
        {
            return Bitboard::fromBits(m_filesUpToBB[right]);
        }
        else
        {
            return Bitboard::fromBits(m_filesUpToBB[right] ^ m_filesUpToBB[left - 1]);
        }
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

    constexpr Bitboard shiftedVertically(int ranks) const
    {
        if (ranks >= 0)
        {
            return fromBits(m_squares << 8 * ranks);
        }
        else
        {
            return fromBits(m_squares >> -8 * ranks);
        }
    }

    template <int files, int ranks>
    constexpr void shift()
    {
        static_assert(files >= -7);
        static_assert(ranks >= -7);
        static_assert(files <= 7);
        static_assert(ranks <= 7);

        if constexpr (files != 0)
        {
            constexpr Bitboard mask =
                files > 0
                ? Bitboard::betweenFiles(fileA, fileH - files)
                : Bitboard::betweenFiles(fileA - files, fileH);

            m_squares &= mask.m_squares;
        }

        constexpr int shift = files + ranks * 8;
        if constexpr (shift == 0)
        {
            return;
        }

        if constexpr (shift < 0)
        {
            m_squares >>= -shift;
        }
        else
        {
            m_squares <<= shift;
        }
    }

    template <int files, int ranks>
    constexpr Bitboard shifted() const
    {
        Bitboard bbCpy(*this);
        bbCpy.shift<files, ranks>();
        return bbCpy;
    }

    constexpr void shift(Offset offset)
    {
        ASSERT(offset.files >= -7);
        ASSERT(offset.ranks >= -7);
        ASSERT(offset.files <= 7);
        ASSERT(offset.ranks <= 7);

        if (offset.files != 0)
        {
            const Bitboard mask =
                offset.files > 0
                ? Bitboard::betweenFiles(fileA, fileH - offset.files)
                : Bitboard::betweenFiles(fileA - offset.files, fileH);

            m_squares &= mask.m_squares;
        }

        const int shift = offset.files + offset.ranks * 8;
        if (shift < 0)
        {
            m_squares >>= -shift;
        }
        else
        {
            m_squares <<= shift;
        }
    }

    [[nodiscard]] constexpr Bitboard shifted(Offset offset) const
    {
        Bitboard bbCpy(*this);
        bbCpy.shift(offset);
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

    [[nodiscard]] constexpr friend Bitboard operator^(Square sq, Bitboard bb)
    {
        return bb ^ sq;
    }

    [[nodiscard]] constexpr friend Bitboard operator&(Square sq, Bitboard bb)
    {
        return bb & sq;
    }

    [[nodiscard]] constexpr friend Bitboard operator|(Square sq, Bitboard bb)
    {
        return bb | sq;
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

    constexpr void popFirst()
    {
        ASSERT(m_squares != 0);

        m_squares &= m_squares - 1;
    }

    constexpr Bitboard& operator=(const Bitboard& other) = default;

private:
    std::uint64_t m_squares;
};

[[nodiscard]] constexpr Bitboard operator^(Square sq0, Square sq1)
{
    return Bitboard::square(sq0) ^ sq1;
}

[[nodiscard]] constexpr Bitboard operator&(Square sq0, Square sq1)
{
    return Bitboard::square(sq0) & sq1;
}

[[nodiscard]] constexpr Bitboard operator|(Square sq0, Square sq1)
{
    return Bitboard::square(sq0) | sq1;
}

[[nodiscard]] constexpr Bitboard operator""_bb(std::uint64_t bits)
{
    return Bitboard::fromBits(bits);
}

namespace bb
{
    namespace fancy_magics
    {
        // Implementation based on https://github.com/syzygy1/Cfish

        alignas(64) constexpr EnumArray<Square, std::uint64_t> g_rookMagics{ {
            0x0A80004000801220ull,
            0x8040004010002008ull,
            0x2080200010008008ull,
            0x1100100008210004ull,
            0xC200209084020008ull,
            0x2100010004000208ull,
            0x0400081000822421ull,
            0x0200010422048844ull,
            0x0800800080400024ull,
            0x0001402000401000ull,
            0x3000801000802001ull,
            0x4400800800100083ull,
            0x0904802402480080ull,
            0x4040800400020080ull,
            0x0018808042000100ull,
            0x4040800080004100ull,
            0x0040048001458024ull,
            0x00A0004000205000ull,
            0x3100808010002000ull,
            0x4825010010000820ull,
            0x5004808008000401ull,
            0x2024818004000A00ull,
            0x0005808002000100ull,
            0x2100060004806104ull,
            0x0080400880008421ull,
            0x4062220600410280ull,
            0x010A004A00108022ull,
            0x0000100080080080ull,
            0x0021000500080010ull,
            0x0044000202001008ull,
            0x0000100400080102ull,
            0xC020128200040545ull,
            0x0080002000400040ull,
            0x0000804000802004ull,
            0x0000120022004080ull,
            0x010A386103001001ull,
            0x9010080080800400ull,
            0x8440020080800400ull,
            0x0004228824001001ull,
            0x000000490A000084ull,
            0x0080002000504000ull,
            0x200020005000C000ull,
            0x0012088020420010ull,
            0x0010010080080800ull,
            0x0085001008010004ull,
            0x0002000204008080ull,
            0x0040413002040008ull,
            0x0000304081020004ull,
            0x0080204000800080ull,
            0x3008804000290100ull,
            0x1010100080200080ull,
            0x2008100208028080ull,
            0x5000850800910100ull,
            0x8402019004680200ull,
            0x0120911028020400ull,
            0x0000008044010200ull,
            0x0020850200244012ull,
            0x0020850200244012ull,
            0x0000102001040841ull,
            0x140900040A100021ull,
            0x000200282410A102ull,
            0x000200282410A102ull,
            0x000200282410A102ull,
            0x4048240043802106ull
                } };
        alignas(64) extern EnumArray<Square, Bitboard> g_rookMasks;
        alignas(64) extern EnumArray<Square, std::uint8_t> g_rookShifts;
        alignas(64) extern EnumArray<Square, const Bitboard*> g_rookAttacks;

        alignas(64) constexpr EnumArray<Square, std::uint64_t> g_bishopMagics{ {
            0x40106000A1160020ull,
            0x0020010250810120ull,
            0x2010010220280081ull,
            0x002806004050C040ull,
            0x0002021018000000ull,
            0x2001112010000400ull,
            0x0881010120218080ull,
            0x1030820110010500ull,
            0x0000120222042400ull,
            0x2000020404040044ull,
            0x8000480094208000ull,
            0x0003422A02000001ull,
            0x000A220210100040ull,
            0x8004820202226000ull,
            0x0018234854100800ull,
            0x0100004042101040ull,
            0x0004001004082820ull,
            0x0010000810010048ull,
            0x1014004208081300ull,
            0x2080818802044202ull,
            0x0040880C00A00100ull,
            0x0080400200522010ull,
            0x0001000188180B04ull,
            0x0080249202020204ull,
            0x1004400004100410ull,
            0x00013100A0022206ull,
            0x2148500001040080ull,
            0x4241080011004300ull,
            0x4020848004002000ull,
            0x10101380D1004100ull,
            0x0008004422020284ull,
            0x01010A1041008080ull,
            0x0808080400082121ull,
            0x0808080400082121ull,
            0x0091128200100C00ull,
            0x0202200802010104ull,
            0x8C0A020200440085ull,
            0x01A0008080B10040ull,
            0x0889520080122800ull,
            0x100902022202010Aull,
            0x04081A0816002000ull,
            0x0000681208005000ull,
            0x8170840041008802ull,
            0x0A00004200810805ull,
            0x0830404408210100ull,
            0x2602208106006102ull,
            0x1048300680802628ull,
            0x2602208106006102ull,
            0x0602010120110040ull,
            0x0941010801043000ull,
            0x000040440A210428ull,
            0x0008240020880021ull,
            0x0400002012048200ull,
            0x00AC102001210220ull,
            0x0220021002009900ull,
            0x84440C080A013080ull,
            0x0001008044200440ull,
            0x0004C04410841000ull,
            0x2000500104011130ull,
            0x1A0C010011C20229ull,
            0x0044800112202200ull,
            0x0434804908100424ull,
            0x0300404822C08200ull,
            0x48081010008A2A80ull
        } };
        alignas(64) extern EnumArray<Square, Bitboard> g_bishopMasks;
        alignas(64) extern EnumArray<Square, std::uint8_t> g_bishopShifts;
        alignas(64) extern EnumArray<Square, const Bitboard*> g_bishopAttacks;

        inline Bitboard bishopAttacks(Square s, Bitboard occupied)
        {
            const std::size_t idx =
                (occupied & fancy_magics::g_bishopMasks[s]).bits()
                * fancy_magics::g_bishopMagics[s]
                >> fancy_magics::g_bishopShifts[s];

            return fancy_magics::g_bishopAttacks[s][idx];
        }

        inline Bitboard rookAttacks(Square s, Bitboard occupied)
        {
            const std::size_t idx = 
                (occupied & fancy_magics::g_rookMasks[s]).bits()
                * fancy_magics::g_rookMagics[s]
                >> fancy_magics::g_rookShifts[s];

            return fancy_magics::g_rookAttacks[s][idx];
        }
    }

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

    [[nodiscard]] constexpr Bitboard before(Square sq)
    {
        return Bitboard::fromBits(nbitmask<std::uint64_t>[ordinal(sq)]);
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

    constexpr Bitboard a1 = bb::square(::a1);
    constexpr Bitboard a2 = bb::square(::a2);
    constexpr Bitboard a3 = bb::square(::a3);
    constexpr Bitboard a4 = bb::square(::a4);
    constexpr Bitboard a5 = bb::square(::a5);
    constexpr Bitboard a6 = bb::square(::a6);
    constexpr Bitboard a7 = bb::square(::a7);
    constexpr Bitboard a8 = bb::square(::a8);

    constexpr Bitboard b1 = bb::square(::b1);
    constexpr Bitboard b2 = bb::square(::b2);
    constexpr Bitboard b3 = bb::square(::b3);
    constexpr Bitboard b4 = bb::square(::b4);
    constexpr Bitboard b5 = bb::square(::b5);
    constexpr Bitboard b6 = bb::square(::b6);
    constexpr Bitboard b7 = bb::square(::b7);
    constexpr Bitboard b8 = bb::square(::b8);

    constexpr Bitboard c1 = bb::square(::c1);
    constexpr Bitboard c2 = bb::square(::c2);
    constexpr Bitboard c3 = bb::square(::c3);
    constexpr Bitboard c4 = bb::square(::c4);
    constexpr Bitboard c5 = bb::square(::c5);
    constexpr Bitboard c6 = bb::square(::c6);
    constexpr Bitboard c7 = bb::square(::c7);
    constexpr Bitboard c8 = bb::square(::c8);

    constexpr Bitboard d1 = bb::square(::d1);
    constexpr Bitboard d2 = bb::square(::d2);
    constexpr Bitboard d3 = bb::square(::d3);
    constexpr Bitboard d4 = bb::square(::d4);
    constexpr Bitboard d5 = bb::square(::d5);
    constexpr Bitboard d6 = bb::square(::d6);
    constexpr Bitboard d7 = bb::square(::d7);
    constexpr Bitboard d8 = bb::square(::d8);

    constexpr Bitboard e1 = bb::square(::e1);
    constexpr Bitboard e2 = bb::square(::e2);
    constexpr Bitboard e3 = bb::square(::e3);
    constexpr Bitboard e4 = bb::square(::e4);
    constexpr Bitboard e5 = bb::square(::e5);
    constexpr Bitboard e6 = bb::square(::e6);
    constexpr Bitboard e7 = bb::square(::e7);
    constexpr Bitboard e8 = bb::square(::e8);

    constexpr Bitboard f1 = bb::square(::f1);
    constexpr Bitboard f2 = bb::square(::f2);
    constexpr Bitboard f3 = bb::square(::f3);
    constexpr Bitboard f4 = bb::square(::f4);
    constexpr Bitboard f5 = bb::square(::f5);
    constexpr Bitboard f6 = bb::square(::f6);
    constexpr Bitboard f7 = bb::square(::f7);
    constexpr Bitboard f8 = bb::square(::f8);

    constexpr Bitboard g1 = bb::square(::g1);
    constexpr Bitboard g2 = bb::square(::g2);
    constexpr Bitboard g3 = bb::square(::g3);
    constexpr Bitboard g4 = bb::square(::g4);
    constexpr Bitboard g5 = bb::square(::g5);
    constexpr Bitboard g6 = bb::square(::g6);
    constexpr Bitboard g7 = bb::square(::g7);
    constexpr Bitboard g8 = bb::square(::g8);

    constexpr Bitboard h1 = bb::square(::h1);
    constexpr Bitboard h2 = bb::square(::h2);
    constexpr Bitboard h3 = bb::square(::h3);
    constexpr Bitboard h4 = bb::square(::h4);
    constexpr Bitboard h5 = bb::square(::h5);
    constexpr Bitboard h6 = bb::square(::h6);
    constexpr Bitboard h7 = bb::square(::h7);
    constexpr Bitboard h8 = bb::square(::h8);

    [[nodiscard]] Bitboard between(Square s1, Square s2);

    [[nodiscard]] Bitboard line(Square s1, Square s2);

    template <PieceType PieceTypeV>
    [[nodiscard]] Bitboard pseudoAttacks(Square sq);

    [[nodiscard]] Bitboard pseudoAttacks(PieceType pt, Square sq);

    template <PieceType PieceTypeV>
    Bitboard attacks(Square sq, Bitboard occupied)
    {
        static_assert(PieceTypeV != PieceType::None && PieceTypeV != PieceType::Pawn);

        ASSERT(sq.isOk());

        if constexpr (PieceTypeV == PieceType::Bishop)
        {
            return fancy_magics::bishopAttacks(sq, occupied);
        }
        else if constexpr (PieceTypeV == PieceType::Rook)
        {
            return fancy_magics::rookAttacks(sq, occupied);
        }
        else if constexpr (PieceTypeV == PieceType::Queen)
        {
            return
                fancy_magics::bishopAttacks(sq, occupied)
                | fancy_magics::rookAttacks(sq, occupied);
        }
        else
        {
            return pseudoAttacks<PieceTypeV>(sq);
        }
    }

    [[nodiscard]] inline Bitboard attacks(PieceType pt, Square sq, Bitboard occupied)
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

    [[nodiscard]] Bitboard pawnAttacks(Bitboard pawns, Color color);

    [[nodiscard]] Bitboard westPawnAttacks(Bitboard pawns, Color color);

    [[nodiscard]] Bitboard eastPawnAttacks(Bitboard pawns, Color color);

    [[nodiscard]] bool isAttackedBySlider(
        Square sq,
        Bitboard bishops,
        Bitboard rooks,
        Bitboard queens,
        Bitboard occupied
    );
}
