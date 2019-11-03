#include "Bitboard.h"

#include "Chess.h"

#include "data_structure/Enum.h"
#include "data_structure/EnumMap.h"

#include "intrin/Intrinsics.h"

#include "util/Assert.h"

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

        [[nodiscard]] static EnumMap<Square, Bitboard> generatePseudoAttacks_Pawn()
        {
            // pseudo attacks don't make sense for pawns
            return {};
        }

        [[nodiscard]] static EnumMap<Square, Bitboard> generatePseudoAttacks_Knight()
        {
            EnumMap<Square, Bitboard> bbs{};

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

        [[nodiscard]] static EnumMap<Square, Bitboard> generatePseudoAttacks_Bishop()
        {
            EnumMap<Square, Bitboard> bbs{};

            for (Square fromSq = ::a1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generateSliderPseudoAttacks(bishopOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static EnumMap<Square, Bitboard> generatePseudoAttacks_Rook()
        {
            EnumMap<Square, Bitboard> bbs{};

            for (Square fromSq = ::a1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generateSliderPseudoAttacks(rookOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static EnumMap<Square, Bitboard> generatePseudoAttacks_Queen()
        {
            EnumMap<Square, Bitboard> bbs{};

            for (Square fromSq = ::a1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] =
                    generateSliderPseudoAttacks(bishopOffsets, fromSq)
                    | generateSliderPseudoAttacks(rookOffsets, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static EnumMap<Square, Bitboard> generatePseudoAttacks_King()
        {
            EnumMap<Square, Bitboard> bbs{};

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

        [[nodiscard]] static EnumMap2<PieceType, Square, Bitboard> generatePseudoAttacks()
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

        static const EnumMap2<PieceType, Square, Bitboard> pseudoAttacks = generatePseudoAttacks();

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

        [[nodiscard]] static EnumMap<Square, Bitboard> generatePositiveRayAttacks(Direction dir)
        {
            EnumMap<Square, Bitboard> bbs{};

            for (Square fromSq = ::a1; fromSq != Square::none(); ++fromSq)
            {
                bbs[fromSq] = generatePositiveRayAttacks(dir, fromSq);
            }

            return bbs;
        }

        [[nodiscard]] static std::array<EnumMap<Square, Bitboard>, 8> generatePositiveRayAttacks()
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

        static const std::array<EnumMap<Square, Bitboard>, 8> positiveRayAttacks = generatePositiveRayAttacks();

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

    template <PieceType PieceTypeV>
    [[nodiscard]] Bitboard attacks(Square sq, Bitboard occupied)
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

    template Bitboard attacks<PieceType::Knight>(Square, Bitboard);
    template Bitboard attacks<PieceType::Bishop>(Square, Bitboard);
    template Bitboard attacks<PieceType::Rook>(Square, Bitboard);
    template Bitboard attacks<PieceType::Queen>(Square, Bitboard);
    template Bitboard attacks<PieceType::King>(Square, Bitboard);

    [[nodiscard]] Bitboard attacks(PieceType pt, Square sq, Bitboard occupied)
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

    [[nodiscard]] Bitboard pawnAttacks(Bitboard pawns, Color color)
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
