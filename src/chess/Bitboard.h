#pragma once

#include "util/Assert.h"
#include "Chess.h"
#include "data_structure/Enum.h"
#include "data_structure/EnumMap.h"
#include "intrin/Intrinsics.h"

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
    static constexpr EnumMap<File, std::uint64_t> m_filesUpToBB{
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
            m_squares <<= 8 * offset.ranks;
        }
        else if (offset.ranks < 0)
        {
            m_squares >>= -8 * offset.ranks;
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

        [[nodiscard]] static inline EnumMap<Square, Bitboard> generatePseudoAttacks_Pawn()
        {
            // pseudo attacks don't make sense for pawns
            return {};
        }

        [[nodiscard]] static inline EnumMap<Square, Bitboard> generatePseudoAttacks_Knight()
        {
            EnumMap<Square, Bitboard> bbs{};

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

        [[nodiscard]] static inline Bitboard generateSliderPseudoAttacks(const std::array<Offset, 4>& offsets, Square fromSq)
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

        [[nodiscard]] static inline EnumMap<Square, Bitboard> generatePseudoAttacks_Bishop()
        {
            EnumMap<Square, Bitboard> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generateSliderPseudoAttacks(bishopOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static inline EnumMap<Square, Bitboard> generatePseudoAttacks_Rook()
        {
            EnumMap<Square, Bitboard> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generateSliderPseudoAttacks(rookOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static inline EnumMap<Square, Bitboard> generatePseudoAttacks_Queen()
        {
            EnumMap<Square, Bitboard> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] =
                    generateSliderPseudoAttacks(bishopOffsets, fromSq)
                    | generateSliderPseudoAttacks(rookOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static inline EnumMap<Square, Bitboard> generatePseudoAttacks_King()
        {
            EnumMap<Square, Bitboard> bbs{};

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

        [[nodiscard]] static inline auto generatePseudoAttacks()
        {
            return EnumMap2<PieceType, Square, Bitboard>{
                generatePseudoAttacks_Pawn(),
                generatePseudoAttacks_Knight(),
                generatePseudoAttacks_Bishop(),
                generatePseudoAttacks_Rook(),
                generatePseudoAttacks_Queen(),
                generatePseudoAttacks_King()
            };
        }

        static inline auto pseudoAttacks = generatePseudoAttacks();

        [[nodiscard]] static inline Bitboard generatePositiveRayAttacks(Direction dir, Square fromSq)
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

        [[nodiscard]] static inline EnumMap<Square, Bitboard> generatePositiveRayAttacks(Direction dir)
        {
            EnumMap<Square, Bitboard> bbs{};

            for (Square fromSq = A1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generatePositiveRayAttacks(dir, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static inline auto generatePositiveRayAttacks()
        {
            std::array<EnumMap<Square, Bitboard>, 8> bbs{};

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

        static inline auto positiveRayAttacks = generatePositiveRayAttacks();

        template <Direction DirV>
        [[nodiscard]] inline Bitboard slidingAttacks(Square sq, Bitboard occupied)
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
    [[nodiscard]] inline Bitboard pseudoAttacks(Square sq)
    {
        static_assert(PieceTypeV != PieceType::None && PieceTypeV != PieceType::Pawn);

        ASSERT(sq.isOk());

        return detail::pseudoAttacks[PieceTypeV][sq];
    }

    [[nodiscard]] inline Bitboard pseudoAttacks(PieceType pt, Square sq)
    {
        ASSERT(sq.isOk());

        return detail::pseudoAttacks[pt][sq];
    }

    template <PieceType PieceTypeV>
    [[nodiscard]] inline Bitboard attacks(Square sq, Bitboard occupied)
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

    [[nodiscard]] inline Bitboard pawnAttacks(Bitboard pawns, Color color)
    {
        if (color == Color::White)
        {
            pawns |= (pawns + Offset{ 1, 1 }) | (pawns + Offset{ -1, 1 });
        }
        else
        {
            pawns |= (pawns + Offset{ 1, -1 }) | (pawns + Offset{ -1, -1 });
        }

        return pawns;
    }

    [[nodiscard]] inline bool isAttackedBySlider(
        Square sq, 
        Bitboard bishops, 
        Bitboard rooks, 
        Bitboard queens, 
        Bitboard occupied
    )
    {
        const Bitboard opponentBishopLikePieces = (bishops | queens);
        const Bitboard bishopAttacks = bb::attacks<PieceType::Bishop>(sq, occupied);
        if ((bishopAttacks & opponentBishopLikePieces).any())
        {
            return true;
        }

        const Bitboard opponentRookLikePieces = (rooks | queens);
        const Bitboard rookAttacks = bb::attacks<PieceType::Rook>(sq, occupied);
        return (rookAttacks & opponentRookLikePieces).any();
    }
}
