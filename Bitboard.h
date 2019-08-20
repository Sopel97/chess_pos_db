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

    bool moreThanOne() const
    {
        return !!(m_squares & (m_squares - 1));
    }

    bool exactlyOne() const
    {
        return m_squares != 0 && !moreThanOne();
    }

    bool any() const
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

namespace detail::bitboard
{
    static constexpr std::array<FlatSquareOffset, 8> knightOffsets{ { {-1, -2}, {-1, 2}, {1, -2}, {1, 2}, {-2, -1}, {-2, 1}, {2, -1}, {2, 1} } };
    static constexpr std::array<FlatSquareOffset, 4> bishopOffsets{ { {-1, -1}, {-1, 1}, {1, -1}, {1, 1} } };
    static constexpr std::array<FlatSquareOffset, 4> rookOffsets{ { {-1, 0}, {1, 0}, {0, -1}, {0, 1} } };

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
                const Square toSq = fromSq + offset;
                if (toSq.isOk())
                {
                    bb |= toSq;
                }
            }

            bbs[ordinal(fromSq)] = bb;
        }

        return bbs;
    }

    static constexpr Bitboard generateSliderPseudoAttacks(const std::array<FlatSquareOffset, 4>& offsets, Square fromSq)
    {
        Bitboard bb{};

        for (auto&& offset : offsets)
        {
            for (;;)
            {
                fromSq += offset;

                if (!fromSq.isOk())
                {
                    break;
                }

                bb |= fromSq;
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
                | generateSliderPseudoAttacks(bishopOffsets, fromSq);
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

    constexpr auto pseudoAttacksBB = generatePseudoAttacks();
}

constexpr Bitboard pseudoAttacksBB(PieceType pt, Square sq)
{
    return detail::bitboard::pseudoAttacksBB[ordinal(pt)][ordinal(sq)];
}
