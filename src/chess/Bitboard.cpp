#include "Bitboard.h"

#include "Chess.h"

#include "enum/Enum.h"
#include "enum/EnumArray.h"

#include "intrin/Intrinsics.h"

#include "util/Assert.h"

#include <array>

namespace bb
{
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

        [[nodiscard]] static EnumArray<Square, Bitboard> generatePseudoAttacks_Pawn()
        {
            // pseudo attacks don't make sense for pawns
            return {};
        }

        [[nodiscard]] static EnumArray<Square, Bitboard> generatePseudoAttacks_Knight()
        {
            EnumArray<Square, Bitboard> bbs{};

            for (Square fromSq = ::a1; fromSq != Square::none(); ++fromSq)
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

        [[nodiscard]] static Bitboard generateSliderPseudoAttacks(const std::array<Offset, 4> & offsets, Square fromSq)
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

        [[nodiscard]] static EnumArray<Square, Bitboard> generatePseudoAttacks_Bishop()
        {
            EnumArray<Square, Bitboard> bbs{};

            for (Square fromSq = ::a1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generateSliderPseudoAttacks(bishopOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static EnumArray<Square, Bitboard> generatePseudoAttacks_Rook()
        {
            EnumArray<Square, Bitboard> bbs{};

            for (Square fromSq = ::a1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generateSliderPseudoAttacks(rookOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static EnumArray<Square, Bitboard> generatePseudoAttacks_Queen()
        {
            EnumArray<Square, Bitboard> bbs{};

            for (Square fromSq = ::a1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] =
                    generateSliderPseudoAttacks(bishopOffsets, fromSq)
                    | generateSliderPseudoAttacks(rookOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static EnumArray<Square, Bitboard> generatePseudoAttacks_King()
        {
            EnumArray<Square, Bitboard> bbs{};

            for (Square fromSq = ::a1; fromSq != Square::none(); ++fromSq)
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

        [[nodiscard]] static EnumArray2<PieceType, Square, Bitboard> generatePseudoAttacks()
        {
            return EnumArray2<PieceType, Square, Bitboard>{
                generatePseudoAttacks_Pawn(),
                    generatePseudoAttacks_Knight(),
                    generatePseudoAttacks_Bishop(),
                    generatePseudoAttacks_Rook(),
                    generatePseudoAttacks_Queen(),
                    generatePseudoAttacks_King()
            };
        }

        static const EnumArray2<PieceType, Square, Bitboard> pseudoAttacks = generatePseudoAttacks();

        [[nodiscard]] static Bitboard generatePositiveRayAttacks(Direction dir, Square fromSq)
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

        [[nodiscard]] static EnumArray<Square, Bitboard> generatePositiveRayAttacks(Direction dir)
        {
            EnumArray<Square, Bitboard> bbs{};

            for (Square fromSq = ::a1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generatePositiveRayAttacks(dir, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static std::array<EnumArray<Square, Bitboard>, 8> generatePositiveRayAttacks()
        {
            std::array<EnumArray<Square, Bitboard>, 8> bbs{};

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

        static const std::array<EnumArray<Square, Bitboard>, 8> positiveRayAttacks = generatePositiveRayAttacks();

        template <Direction DirV>
        [[nodiscard]] static Bitboard slidingAttacks(Square sq, Bitboard occupied)
        {
            ASSERT(sq.isOk());

            Bitboard attacks = positiveRayAttacks[DirV][sq];

            if constexpr (DirV == NorthWest || DirV == North || DirV == NorthEast || DirV == East)
            {
                Bitboard blocker = (attacks & occupied) | h8; // set highest bit (H8) so msb never fails
                return attacks ^ positiveRayAttacks[DirV][blocker.first()];
            }
            else
            {
                Bitboard blocker = (attacks & occupied) | a1;
                return attacks ^ positiveRayAttacks[DirV][blocker.last()];
            }
        }

        template Bitboard slidingAttacks<Direction::North>(Square, Bitboard);
        template Bitboard slidingAttacks<Direction::NorthEast>(Square, Bitboard);
        template Bitboard slidingAttacks<Direction::East>(Square, Bitboard);
        template Bitboard slidingAttacks<Direction::SouthEast>(Square, Bitboard);
        template Bitboard slidingAttacks<Direction::South>(Square, Bitboard);
        template Bitboard slidingAttacks<Direction::SouthWest>(Square, Bitboard);
        template Bitboard slidingAttacks<Direction::West>(Square, Bitboard);
        template Bitboard slidingAttacks<Direction::NorthWest>(Square, Bitboard);

        template <PieceType PieceTypeV>
        [[nodiscard]] Bitboard pieceSlidingAttacks(Square sq, Bitboard occupied)
        {
            static_assert(
                PieceTypeV == PieceType::Rook
                || PieceTypeV == PieceType::Bishop
                || PieceTypeV == PieceType::Queen);

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
            else // if constexpr (PieceTypeV == PieceType::Queen)
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
        }

        static Bitboard generateBetween(Square s1, Square s2)
        {
            Bitboard bb = Bitboard::none();

            if (s1 == s2)
            {
                return bb;
            }

            const int fd = s2.file() - s1.file();
            const int rd = s2.rank() - s1.rank();

            if (fd == 0 || rd == 0 || fd == rd || fd == -rd)
            {
                // s1 and s2 lie on a line.
                const int fileStep = (fd > 0) - (fd < 0);
                const int rankStep = (rd > 0) - (rd < 0);
                const auto step = FlatSquareOffset(fileStep, rankStep);
                s1 += step; // omit s1
                while(s1 != s2) // omit s2
                {
                    bb |= s1;
                    s1 += step;
                }
            }

            return bb;
        }

        static Bitboard generateLine(Square s1, Square s2)
        {
            for (PieceType pt : { PieceType::Bishop, PieceType::Rook })
            {
                const Bitboard s1Attacks = pseudoAttacks[pt][s1];
                if (s1Attacks.isSet(s2))
                {
                    const Bitboard s2Attacks = pseudoAttacks[pt][s2];
                    return (s1Attacks & s2Attacks) | s1 | s2;
                }
            }

            return Bitboard::none();
        }

        static const EnumArray2<Square, Square, Bitboard> between = []()
        {
            EnumArray2<Square, Square, Bitboard> between;

            for (Square s1 : values<Square>())
            {
                for (Square s2 : values<Square>())
                {
                    between[s1][s2] = generateBetween(s1, s2);
                }
            }

            return between;
        }();

        static const EnumArray2<Square, Square, Bitboard> line = []()
        {
            EnumArray2<Square, Square, Bitboard> line;

            for (Square s1 : values<Square>())
            {
                for (Square s2 : values<Square>())
                {
                    line[s1][s2] = generateLine(s1, s2);
                }
            }

            return line;
        }();
    }

    namespace fancy_magics
    {
        enum struct MagicsType
        {
            Rook,
            Bishop
        };

        alignas(64) EnumArray<Square, Bitboard> g_rookMasks;
        alignas(64) EnumArray<Square, std::uint8_t> g_rookShifts;
        alignas(64) EnumArray<Square, const Bitboard*> g_rookAttacks;

        alignas(64) EnumArray<Square, Bitboard> g_bishopMasks;
        alignas(64) EnumArray<Square, std::uint8_t> g_bishopShifts;
        alignas(64) EnumArray<Square, const Bitboard*> g_bishopAttacks;

        alignas(64) static std::array<Bitboard, 102400> g_allRookAttacks;
        alignas(64) static std::array<Bitboard, 5248> g_allBishopAttacks;

        template <MagicsType TypeV>
        [[nodiscard]] Bitboard slidingAttacks(Square sq, Bitboard occupied)
        {
            if (TypeV == MagicsType::Rook)
            {
                return ::bb::detail::pieceSlidingAttacks<PieceType::Rook>(sq, occupied);
            }

            if (TypeV == MagicsType::Bishop)
            {
                return ::bb::detail::pieceSlidingAttacks<PieceType::Bishop>(sq, occupied);
            }

            return Bitboard::none();
        }

        template <MagicsType TypeV, std::size_t SizeV>
        [[nodiscard]] bool initMagics(
            const EnumArray<Square, std::uint64_t>& magics,
            std::array<Bitboard, SizeV>& table,
            EnumArray<Square, Bitboard>& masks,
            EnumArray<Square, std::uint8_t>& shifts,
            EnumArray<Square, const Bitboard*>& attacks
        )
        {
            std::size_t size = 0;
            for (Square sq : values<Square>()) 
            {
                const Bitboard edges =
                    ((bb::rank1 | bb::rank8) & ~Bitboard::rank(sq.rank()))
                    | ((bb::fileA | bb::fileH) & ~Bitboard::file(sq.file()));

                Bitboard* currentAttacks = table.data() + size;

                attacks[sq] = currentAttacks;
                masks[sq] = slidingAttacks<TypeV>(sq, Bitboard::none()) & ~edges;
                shifts[sq] = 64 - masks[sq].count();

                Bitboard occupied = Bitboard::none();
                do 
                {
                    const std::size_t idx = 
                        (occupied & masks[sq]).bits()
                        * magics[sq] 
                        >> shifts[sq];

                    currentAttacks[idx] = slidingAttacks<TypeV>(sq, occupied);

                    ++size;
                    occupied = Bitboard::fromBits(occupied.bits() - masks[sq].bits()) & masks[sq];
                } while (occupied.any());
            }

            return true;
        }

        static bool g_isRookMagicsInitialized = 
            initMagics<MagicsType::Rook>(g_rookMagics, g_allRookAttacks, g_rookMasks, g_rookShifts, g_rookAttacks);

        static bool g_isBishopMagicsInitialized = 
            initMagics<MagicsType::Bishop>(g_bishopMagics, g_allBishopAttacks, g_bishopMasks, g_bishopShifts, g_bishopAttacks);
    }

    [[nodiscard]] Bitboard between(Square s1, Square s2)
    {
        return detail::between[s1][s2];
    }

    [[nodiscard]] Bitboard line(Square s1, Square s2)
    {
        return detail::line[s1][s2];
    }

    template <PieceType PieceTypeV>
    [[nodiscard]] Bitboard pseudoAttacks(Square sq)
    {
        static_assert(PieceTypeV != PieceType::None && PieceTypeV != PieceType::Pawn);

        ASSERT(sq.isOk());

        return detail::pseudoAttacks[PieceTypeV][sq];
    }

    template Bitboard pseudoAttacks<PieceType::Knight>(Square);
    template Bitboard pseudoAttacks<PieceType::Bishop>(Square);
    template Bitboard pseudoAttacks<PieceType::Rook>(Square);
    template Bitboard pseudoAttacks<PieceType::Queen>(Square);
    template Bitboard pseudoAttacks<PieceType::King>(Square);

    [[nodiscard]] Bitboard pseudoAttacks(PieceType pt, Square sq)
    {
        ASSERT(sq.isOk());

        return detail::pseudoAttacks[pt][sq];
    }

    [[nodiscard]] Bitboard pawnAttacks(Bitboard pawns, Color color)
    {
        if (color == Color::White)
        {
            return pawns.shifted<1, 1>() | pawns.shifted<-1, 1>();
        }
        else
        {
            return pawns.shifted<1, -1>() | pawns.shifted<-1, -1>();
        }
    }

    [[nodiscard]] Bitboard westPawnAttacks(Bitboard pawns, Color color)
    {
        if (color == Color::White)
        {
            return pawns.shifted<-1, 1>();
        }
        else
        {
            return pawns.shifted<-1, -1>();
        }
    }

    [[nodiscard]] Bitboard eastPawnAttacks(Bitboard pawns, Color color)
    {
        if (color == Color::White)
        {
            return pawns.shifted<1, 1>();
        }
        else
        {
            return pawns.shifted<1, -1>();
        }
    }

    [[nodiscard]] bool isAttackedBySlider(
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
