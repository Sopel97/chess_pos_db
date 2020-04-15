#pragma once

#include "Chess.h"

#include "enum/Enum.h"
#include "enum/EnumArray.h"

#include "intrin/Intrinsics.h"

#include "util/ArithmeticUtility.h"
#include "util/Assert.h"

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

    constexpr Bitboard& operator+=(Offset offset)
    {
        ASSERT(offset.files >= -7);
        ASSERT(offset.ranks >= -7);
        ASSERT(offset.files <= 7);
        ASSERT(offset.ranks <= 7);

        if (offset.ranks > 0)
        {
            m_squares <<= 8 * static_cast<std::uint64_t>(offset.ranks);
        }
        else if (offset.ranks < 0)
        {
            m_squares >>= -8 * static_cast<std::uint64_t>(offset.ranks);
        }

        if (offset.files > 0)
        {
            const File endFile = fileH - offset.files;
            const Bitboard mask = Bitboard::betweenFiles(fileA, endFile);
            m_squares = (m_squares & mask.m_squares) << offset.files;
        }
        else if (offset.files < 0)
        {
            const File startFile = fileA - offset.files;
            const Bitboard mask = Bitboard::betweenFiles(startFile, fileH);
            m_squares = (m_squares & mask.m_squares) >> -offset.files;
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
        ASSERT(m_square != 0);

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

    template <PieceType PieceTypeV>
    [[nodiscard]] Bitboard pseudoAttacks(Square sq);

    [[nodiscard]] Bitboard pseudoAttacks(PieceType pt, Square sq);

    template <PieceType PieceTypeV>
    [[nodiscard]] Bitboard attacks(Square sq, Bitboard occupied);

    [[nodiscard]] Bitboard attacks(PieceType pt, Square sq, Bitboard occupied);

    [[nodiscard]] Bitboard pawnAttacks(Bitboard pawns, Color color);

    [[nodiscard]] bool isAttackedBySlider(
        Square sq,
        Bitboard bishops,
        Bitboard rooks,
        Bitboard queens,
        Bitboard occupied
    );
}
