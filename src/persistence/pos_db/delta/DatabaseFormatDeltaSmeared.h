#pragma once

#include <algorithm>
#include <cmath>

#include "chess/Chess.h"
#include "chess/Position.h"
#include "chess/MoveIndex.h"

#include "persistence/pos_db/EntryConstructionParameters.h"

#include "persistence/pos_db/OrderedEntrySetPositionDatabase.h"

#include "util/ArithmeticUtility.h"
#include "util/BitPacking.h"

#include <cstdint>

namespace persistence
{
    namespace db_delta_smeared
    {
        static constexpr std::uint64_t invalidGameOffset = std::numeric_limits<std::uint64_t>::max();

        namespace detail
        {
            inline uint32_t encodePawnNonPromotionUnmove(Square from, Square to, Color sideToUnmove)
            {
                unsigned idx;
                if (sideToUnmove == Color::White)
                {
                    // capture left - 7 - 7 = 0
                    // single straight - 8 - 7 = 1
                    // capture right - 9 - 7 = 2
                    // double move - 16 - 7 = 9 // this is fine, we don't have to normalize it to 3
                    idx = ordinal(to) - ordinal(from) - 7;
                }
                else
                {
                    idx = ordinal(from) - ordinal(to) - 7;
                }

                return idx;
            }

            inline Move decodePawnNonPromotionUnmove(uint32_t index, Square to, Square epSquare, Color sideToUnmove)
            {
                int offset = index + 7;
                if (sideToUnmove == Color::White) offset = -offset;
                const Square from = fromOrdinal<Square>(ordinal(to) + offset);

                const MoveType type =
                    to == epSquare
                    ? MoveType::EnPassant
                    : MoveType::Normal;

                return Move{ from, to, type, Piece::none() };
            }

            inline uint32_t packReverseMove(const Position& pos, const ReverseMove& rm)
            {
                const Color sideToUnmove = !pos.sideToMove();

                uint32_t toSquareIndex;
                uint32_t destinationIndex;
                if (rm.isNull())
                {
                    return
                        (1 << (20 - 4))
                        | (31 << (20 - 4 - 5));
                }
                else if (rm.move.type == MoveType::Castle)
                {
                    toSquareIndex = 0; // we can set this to zero because destinationIndex is unique

                    const bool isKingSide = rm.move.to.file() == fileH;
                    destinationIndex = isKingSide ? 30 : 31;
                }
                else if (rm.move.type == MoveType::Promotion)
                {
                    toSquareIndex = (bb::before(rm.move.to) & pos.piecesBB(sideToUnmove)).count();
                    destinationIndex = std::abs(ordinal(rm.move.to) - ordinal(rm.move.from)) - 7 + 27;
                }
                else
                {
                    toSquareIndex = (bb::before(rm.move.to) & pos.piecesBB(sideToUnmove)).count();
                    const PieceType pt = pos.pieceAt(rm.move.to).type();
                    if (pt == PieceType::Pawn)
                    {
                        destinationIndex = encodePawnNonPromotionUnmove(rm.move.from, rm.move.to, sideToUnmove);
                    }
                    else
                    {
                        destinationIndex = move_index::destinationIndex(pt, rm.move.to, rm.move.from);
                    }
                }

                const uint32_t capturedPieceType = ordinal(rm.capturedPiece.type());
                const uint32_t oldCastlingRights = ordinal(rm.oldCastlingRights);
                const uint32_t hadEpSquare = rm.oldEpSquare != Square::none();
                const uint32_t oldEpSquareFile = ordinal(rm.oldEpSquare.file());

                return
                    (toSquareIndex << (20 - 4))
                    | (destinationIndex << (20 - 4 - 5))
                    | (capturedPieceType << (20 - 4 - 5 - 3))
                    | (oldCastlingRights << (20 - 4 - 5 - 3 - 4))
                    | (hadEpSquare << (20 - 4 - 5 - 3 - 4 - 1))
                    | oldEpSquareFile;
            }

            inline ReverseMove unpackReverseMove(const Position& pos, std::uint32_t packed)
            {
                const Color sideToUnmove = !pos.sideToMove();

                constexpr std::uint32_t toSquareIndexMask = 0b1111;
                constexpr std::uint32_t destinationIndexMask = 0b11111;
                constexpr std::uint32_t capturedPieceTypeMask = 0b111;
                constexpr std::uint32_t oldCastlingRightsMask = 0b1111;
                constexpr std::uint32_t hadEpSquareMask = 0b1;
                constexpr std::uint32_t oldEpSquareFileMask = 0b111;

                const uint32_t toSquareIndex = (packed >> (20 - 4)) & toSquareIndexMask;
                const uint32_t destinationIndex = (packed >> (20 - 4 - 5)) & destinationIndexMask;
                if (toSquareIndex == 1 && destinationIndex == 31)
                {
                    return ReverseMove{};
                }

                const PieceType capturedPieceType = fromOrdinal<PieceType>((packed >> (20 - 4 - 5 - 3)) & capturedPieceTypeMask);
                const CastlingRights oldCastlingRights = fromOrdinal<CastlingRights>((packed >> (20 - 4 - 5 - 3 - 4)) & oldCastlingRightsMask);
                const bool hadEpSquare = (packed >> (20 - 4 - 5 - 3 - 4 - 1)) & hadEpSquareMask;
                const File oldEpSquareFile = fromOrdinal<File>(packed & oldEpSquareFileMask);

                ReverseMove rm{};
                if (capturedPieceType != PieceType::None)
                {
                    rm.capturedPiece = Piece(capturedPieceType, pos.sideToMove());
                }
                else
                {
                    rm.capturedPiece = Piece::none();
                }

                rm.oldCastlingRights = oldCastlingRights;

                if (hadEpSquare)
                {
                    const Rank epSquareRank =
                        pos.sideToMove() == Color::White
                        ? rank3
                        : rank6;

                    rm.oldEpSquare = Square(oldEpSquareFile, epSquareRank);
                }
                else
                {
                    rm.oldEpSquare = Square::none();
                }

                if (destinationIndex >= 30)
                {
                    // castling
                    const CastleType type =
                        destinationIndex == 30
                        ? CastleType::Short
                        : CastleType::Long;

                    rm.move = Move::castle(type, sideToUnmove);
                }
                else
                {
                    const Square toSquare = pos.piecesBB(sideToUnmove).nth(toSquareIndex);
                    if (destinationIndex >= 27)
                    {
                        // pawn promotion
                        rm.move.promotedPiece = pos.pieceAt(toSquare);
                        rm.move.type = MoveType::Promotion;
                        rm.move.to = toSquare;

                        uint32_t offset = destinationIndex - 27 + 7;
                        // The offset applies in the direction the pawn unmoves.
                        // So we have to negate it for the side that unmoves backwards, so white
                        if (sideToUnmove == Color::White)
                        {
                            offset *= -1;
                        }

                        rm.move.from = fromOrdinal<Square>(ordinal(toSquare) + offset);
                    }
                    else
                    {
                        // normal move
                        const PieceType movedPieceType = pos.pieceAt(toSquare).type();
                        if (movedPieceType == PieceType::Pawn)
                        {
                            rm.move = decodePawnNonPromotionUnmove(destinationIndex, toSquare, rm.oldEpSquare, sideToUnmove);
                        }
                        else
                        {
                            rm.move.promotedPiece = Piece::none();
                            rm.move.type = MoveType::Normal;
                            rm.move.to = toSquare;
                            rm.move.from = move_index::destinationSquareByIndex(movedPieceType, toSquare, destinationIndex);
                        }
                    }
                }

                return rm;
            }
        }

        struct UnsmearedEntry;

        struct SmearedEntry
        {
            /*
                - 32 bit hash

                - 32 bit hash

                - 11 bit hash
                - 20 bit packed reverse move
                - 1 bit is first

                - 2 bit result
                - 2 bit level
                - 13 bit total white elo (<500, 4595> - 12 bits, but may be two games)
                - 13 bit total black elo
                - 1 bit (count_with_elo-1)
                - 1 bit (count-1)

                - 32 bit first game index
            */

            using HashLast = util::BitField<std::uint32_t, std::uint32_t, 0xFFE0'0000u>;
            using PackedReverseMove = util::BitField<std::uint32_t, std::uint32_t, 0x001F'FFFEu>;
            using IsFirst = util::BitField<std::uint32_t, std::uint32_t, 0x0000'0001u>;

            using Result = util::BitField<std::uint32_t, std::uint32_t,        0b11000000'00000000'00000000'00000000u>;
            using Level = util::BitField<std::uint32_t, std::uint32_t,         0b00110000'00000000'00000000'00000000u>;
            using TotalWhiteElo = util::BitField<std::uint32_t, std::uint32_t, 0b00001111'11111111'10000000'00000000u>;
            using TotalBlackElo = util::BitField<std::uint32_t, std::uint32_t, 0b00000000'00000000'01111111'11111100u>;
            using CountWithElo = util::BitField<std::uint32_t, std::uint32_t,  0b00000000'00000000'00000000'00000010u>;
            using Count = util::BitField<std::uint32_t, std::uint32_t,         0b00000000'00000000'00000000'00000001u>;

            using Packed0 = util::PackedInts<
                HashLast,
                PackedReverseMove,
                IsFirst
            >;
            using Packed1 = util::PackedInts<
                Result,
                Level,
                TotalWhiteElo,
                TotalBlackElo,
                CountWithElo,
                Count
            >;

            static constexpr std::uint16_t minElo = 500;
            static constexpr std::uint16_t maxElo = 4595;
            static_assert(maxElo - minElo == 4095);

            [[nodiscard]] static constexpr std::uint16_t packElo(std::uint16_t e)
            {
                if (e < minElo) e = minElo;
                else if (e > maxElo) e = maxElo;
                return e - minElo;
            }

            [[nodiscard]] static constexpr std::uint16_t unpackElo(std::uint16_t p)
            {
                return p + minElo;
            }

            friend struct UnsmearedEntry;

            SmearedEntry() :
                m_hash0{},
                m_hash1{},
                m_packed0(IsFirst::mask),
                m_packed1{},
                m_firstGameIndex{}
            {
            }

            SmearedEntry(const PositionWithZobrist& pos, const ReverseMove& reverseMove = ReverseMove{}) :
                m_packed0(IsFirst::mask),
                m_packed1(Count::mask),
                m_firstGameIndex{}
            {
                const auto zobrist = pos.zobrist();
                m_hash0 = zobrist.high >> 32;
                m_hash1 = static_cast<std::uint32_t>(zobrist.high);
                m_packed0.init<HashLast>(static_cast<std::uint32_t>(zobrist.low));

                auto packedReverseMove = detail::packReverseMove(pos, reverseMove);
                m_packed0.init<PackedReverseMove>(packedReverseMove);
            }

            SmearedEntry(const EntryConstructionParameters& params) :
                m_packed0(IsFirst::mask),
                m_packed1(util::meta::TypeList<Result, Level, TotalWhiteElo, TotalBlackElo, CountWithElo, Count>{},
                    ordinal(params.result),
                    ordinal(params.level),
                    packElo(params.whiteElo),
                    packElo(params.blackElo),
                    static_cast<std::uint32_t>(params.whiteElo != 0), // the way params are provided either both elos are present or none
                    1u
                ),
                m_firstGameIndex(static_cast<std::uint32_t>(params.gameIndexOrOffset))
            {
                const auto zobrist = params.position.zobrist();
                m_hash0 = zobrist.high >> 32;
                m_hash1 = static_cast<std::uint32_t>(zobrist.high);
                m_packed0.init<HashLast>(static_cast<std::uint32_t>(zobrist.low));

                auto packedReverseMove = detail::packReverseMove(params.position, params.reverseMove);
                m_packed0.init<PackedReverseMove>(packedReverseMove);
            }

            SmearedEntry(const SmearedEntry&) = default;
            SmearedEntry(SmearedEntry&&) = default;
            SmearedEntry& operator=(const SmearedEntry&) = default;
            SmearedEntry& operator=(SmearedEntry&&) = default;

            [[nodiscard]] GameLevel level() const
            {
                return fromOrdinal<GameLevel>(static_cast<int>(m_packed1.get<Level>()));
            }

            [[nodiscard]] GameResult result() const
            {
                return fromOrdinal<GameResult>(static_cast<int>(m_packed1.get<Result>()));
            }

            [[nodiscard]] std::uint32_t totalWhiteElo() const
            {
                return m_packed1.get<TotalWhiteElo>();
            }

            [[nodiscard]] std::uint32_t totalBlackElo() const
            {
                return m_packed1.get<TotalBlackElo>();
            }

            [[nodiscard]] int eloDiff() const
            {
                return static_cast<int>(totalWhiteElo()) - static_cast<int>(totalBlackElo());
            }

            [[nodiscard]] std::array<std::uint64_t, 2> hash() const
            {
                return std::array<std::uint64_t, 2>{
                    (static_cast<std::uint64_t>(m_hash0) << 32) | m_hash1,
                    m_packed0.get<HashLast>()
                };
            }

            [[nodiscard]] SmearedEntry key() const
            {
                return *this;
            }

            [[nodiscard]] std::uint32_t countWithElo() const
            {
                return static_cast<std::uint32_t>(m_packed1.get<CountWithElo>());
            }

            [[nodiscard]] std::uint32_t count() const
            {
                return static_cast<std::uint32_t>(m_packed1.get<Count>());
            }

            [[nodiscard]] ReverseMove reverseMove(const Position& pos) const
            {
                const std::uint32_t packedInt = static_cast<std::uint32_t>(m_packed0.get<PackedReverseMove>());
                return detail::unpackReverseMove(pos, packedInt);
            }

            [[nodiscard]] bool isFirst() const
            {
                return m_packed0.getRaw<IsFirst>();
            }

            struct CompareLessWithReverseMove
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    if (lhs.m_hash0 < rhs.m_hash0) return true;
                    else if (lhs.m_hash0 > rhs.m_hash0) return false;

                    if (lhs.m_hash1 < rhs.m_hash1) return true;
                    else if (lhs.m_hash1 > rhs.m_hash1) return false;

                    const auto lhsRest = lhs.m_packed0.getRaw<HashLast, PackedReverseMove>();
                    const auto rhsRest = rhs.m_packed0.getRaw<HashLast, PackedReverseMove>();
                    return lhsRest < rhsRest;
                }
            };

            struct CompareLessWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    if (lhs.m_hash0 < rhs.m_hash0) return true;
                    else if (lhs.m_hash0 > rhs.m_hash0) return false;

                    if (lhs.m_hash1 < rhs.m_hash1) return true;
                    else if (lhs.m_hash1 > rhs.m_hash1) return false;

                    const auto lhsRest = lhs.m_packed0.getRaw<HashLast>();
                    const auto rhsRest = rhs.m_packed0.getRaw<HashLast>();
                    return lhsRest < rhsRest;
                }
            };

            struct CompareLessFull
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    if (lhs.m_hash0 < rhs.m_hash0) return true;
                    else if (lhs.m_hash0 > rhs.m_hash0) return false;

                    if (lhs.m_hash1 < rhs.m_hash1) return true;
                    else if (lhs.m_hash1 > rhs.m_hash1) return false;

                    {
                        const auto lhsRest = lhs.m_packed0.getRaw<HashLast>();
                        const auto rhsRest = rhs.m_packed0.getRaw<HashLast>();
                        if (lhsRest < rhsRest) return true;
                        else if (lhsRest > rhsRest) return false;
                    }

                    {
                        const auto lhsRest = lhs.m_packed1.getRaw<Level, Result>();
                        const auto rhsRest = rhs.m_packed1.getRaw<Level, Result>();
                        return lhsRest < rhsRest;
                    }
                }
            };

            struct CompareEqualWithReverseMove
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    return
                        lhs.m_hash0 == rhs.m_hash0
                        && lhs.m_hash1 == rhs.m_hash1
                        && lhs.m_packed0.getRaw<HashLast, PackedReverseMove>() == rhs.m_packed0.getRaw<HashLast, PackedReverseMove>();
                }
            };

            struct CompareEqualWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    return
                        lhs.m_hash0 == rhs.m_hash0
                        && lhs.m_hash1 == rhs.m_hash1
                        && lhs.m_packed0.getRaw<HashLast>() == rhs.m_packed0.getRaw<HashLast>();
                }
            };

            struct CompareEqualFull
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    return 
                        lhs.m_hash0 == rhs.m_hash0
                        && lhs.m_hash1 == rhs.m_hash1
                        && lhs.m_packed0.getRaw<HashLast, PackedReverseMove>() == rhs.m_packed0.getRaw<HashLast, PackedReverseMove>()
                        && lhs.m_packed1.getRaw<Result, Level>() == rhs.m_packed1.getRaw<Result, Level>();
                }
            };

        private:
            std::uint32_t m_hash0;
            std::uint32_t m_hash1;
            Packed0 m_packed0;
            Packed1 m_packed1;
            std::uint32_t m_firstGameIndex;

            SmearedEntry(
                const ZobristKey& zobrist,
                std::uint32_t packedReverseMove,
                GameResult result,
                GameLevel level,
                std::uint32_t totalWhiteEloPart,
                std::uint32_t totalBlackEloPart,
                std::uint32_t countWithEloPart,
                std::uint32_t countPart,
                std::uint32_t firstGameIndex,
                std::uint32_t isFirst
            ) :
                m_hash0(zobrist.high >> 32),
                m_hash1(static_cast<std::uint32_t>(zobrist.high)),
                m_packed0(
                    util::meta::TypeList<HashLast, PackedReverseMove, IsFirst>{},
                    static_cast<std::uint32_t>(zobrist.low),
                    packedReverseMove,
                    isFirst
                ),
                m_packed1(
                    util::meta::TypeList<Result, Level, TotalWhiteElo, TotalBlackElo, CountWithElo, Count>{},
                    ordinal(result),
                    ordinal(level),
                    totalWhiteEloPart,
                    totalBlackEloPart,
                    countWithEloPart,
                    countPart
                ),
                m_firstGameIndex(firstGameIndex)
            {

            }

            SmearedEntry(
                const ZobristKey& zobrist,
                std::uint32_t packedReverseMove,
                GameResult result,
                GameLevel level
            ) :
                m_hash0(zobrist.high >> 32),
                m_hash1(static_cast<std::uint32_t>(zobrist.high)),
                m_packed0(
                    util::meta::TypeList<HashLast, PackedReverseMove>{},
                    static_cast<std::uint32_t>(zobrist.low),
                    packedReverseMove
                ),
                m_packed1(
                    util::meta::TypeList<Result, Level>{},
                    ordinal(result),
                    ordinal(level)
                )
            {

            }
        };
        static_assert(sizeof(SmearedEntry) == 20);

        static_assert(std::is_trivially_copyable_v<SmearedEntry>);

        using Key = SmearedEntry;

        // First smeared entry stores the least significant bits of unsmeared

        struct UnsmearedEntry
        {
            using SmearedEntryType = SmearedEntry;

            struct Sentinel { };

            struct Iterator
            {
                Iterator(const UnsmearedEntry& unsmeared) :
                    m_zobrist(unsmeared.m_zobrist),
                    m_packedReverseMove(unsmeared.m_packedReverseMove),
                    m_result(unsmeared.m_result),
                    m_level(unsmeared.m_level),
                    m_totalWhiteElo(unsmeared.m_totalWhiteElo),
                    m_totalBlackElo(unsmeared.m_totalBlackElo),
                    m_countWithElo(unsmeared.m_countWithElo),
                    m_count(unsmeared.m_count),
                    m_firstGameIndex(unsmeared.m_firstGameIndex),
                    m_isFirst(true)
                {
                }

                [[nodiscard]] SmearedEntry operator*() const
                {
                    return SmearedEntry(
                        m_zobrist,
                        m_packedReverseMove,
                        m_result,
                        m_level,
                        m_totalWhiteElo & (SmearedEntry::TotalWhiteElo::mask >> SmearedEntry::TotalWhiteElo::shift),
                        m_totalBlackElo & (SmearedEntry::TotalBlackElo::mask >> SmearedEntry::TotalBlackElo::shift),
                        m_countWithElo & (SmearedEntry::CountWithElo::mask >> SmearedEntry::CountWithElo::shift),
                        m_count & (SmearedEntry::Count::mask >> SmearedEntry::Count::shift),
                        m_firstGameIndex,
                        m_isFirst
                    );
                }

                Iterator& operator++()
                {
                    m_totalWhiteElo >>= SmearedEntry::TotalWhiteElo::size;
                    m_totalBlackElo >>= SmearedEntry::TotalBlackElo::size;
                    m_countWithElo >>= SmearedEntry::CountWithElo::size;
                    m_count >>= SmearedEntry::Count::size;
                    m_isFirst = false;
                    return *this;
                }

                [[nodiscard]] friend bool operator==(const Iterator& lhs, Sentinel rhs) noexcept
                {
                    return !lhs.m_count;
                }

                [[nodiscard]] friend bool operator!=(const Iterator& lhs, Sentinel rhs) noexcept
                {
                    return !(lhs == rhs);
                }

            private:
                ZobristKey m_zobrist;
                std::uint32_t m_packedReverseMove;
                GameResult m_result;
                GameLevel m_level;
                std::uint64_t m_totalWhiteElo;
                std::uint64_t m_totalBlackElo;
                std::uint64_t m_countWithElo;
                std::uint64_t m_count;
                std::uint32_t m_firstGameIndex;
                bool m_isFirst;
            };

            UnsmearedEntry() :
                m_zobrist{},
                m_packedReverseMove{},
                m_result{},
                m_level{},
                m_totalWhiteElo{},
                m_totalBlackElo{},
                m_countWithElo{},
                m_count{},
                m_firstGameIndex{}
            {

            }

            explicit UnsmearedEntry(const SmearedEntry& smeared)
            {
                ASSERT(smeared.isFirst());

                m_zobrist.high = (static_cast<std::uint64_t>(smeared.m_hash0) << 32) | smeared.m_hash1;
                m_zobrist.low = smeared.m_packed0.get<SmearedEntry::HashLast>();

                m_packedReverseMove = static_cast<std::uint32_t>(smeared.m_packed0.get<SmearedEntry::PackedReverseMove>());

                m_totalWhiteElo = static_cast<std::uint64_t>(smeared.totalWhiteElo());
                m_totalBlackElo = static_cast<std::uint64_t>(smeared.totalBlackElo());

                m_countWithElo = static_cast<std::uint64_t>(smeared.countWithElo());
                m_count = static_cast<std::uint64_t>(smeared.count());

                m_result = smeared.result();
                m_level = smeared.level();

                m_firstGameIndex = smeared.m_firstGameIndex;
            }

            void combine(const UnsmearedEntry& other)
            {
                m_totalWhiteElo += other.m_totalWhiteElo;
                m_totalBlackElo += other.m_totalBlackElo;

                m_countWithElo += other.m_countWithElo;
                m_count += other.m_count;

                m_firstGameIndex = std::min(m_firstGameIndex, other.m_firstGameIndex);
            }

            void add(const SmearedEntry& smeared, std::uint32_t position)
            {
                // for adding at position 0 use constructor
                ASSERT(position != 0);

                m_totalWhiteElo += static_cast<std::uint64_t>(smeared.totalWhiteElo()) << (position * SmearedEntry::TotalWhiteElo::size);
                m_totalBlackElo += static_cast<std::uint64_t>(smeared.totalBlackElo()) << (position * SmearedEntry::TotalBlackElo::size);

                m_countWithElo += static_cast<std::uint64_t>(smeared.countWithElo()) << (position * SmearedEntry::CountWithElo::size);
                m_count += static_cast<std::uint64_t>(smeared.count()) << (position * SmearedEntry::Count::size);
            }

            [[nodiscard]] GameLevel level() const
            {
                return m_level;
            }

            [[nodiscard]] GameResult result() const
            {
                return m_result;
            }

            [[nodiscard]] std::int64_t eloDiff() const
            {
                return static_cast<std::int64_t>(m_totalWhiteElo) - static_cast<std::int64_t>(m_totalBlackElo);
            }

            [[nodiscard]] SmearedEntry key() const
            {
                return SmearedEntry(m_zobrist, m_packedReverseMove, m_result, m_level);
            }

            [[nodiscard]] std::uint64_t whiteElo() const
            {
                return m_totalWhiteElo;
            }

            [[nodiscard]] std::uint64_t blackElo() const
            {
                return m_totalBlackElo;
            }

            [[nodiscard]] std::uint64_t count() const
            {
                return m_count;
            }

            [[nodiscard]] std::uint64_t countWithElo() const
            {
                return m_countWithElo;
            }

            [[nodiscard]] std::uint32_t firstGameIndex() const
            {
                return m_firstGameIndex;
            }

            [[nodiscard]] ReverseMove reverseMove(const Position& pos) const
            {
                return detail::unpackReverseMove(pos, m_packedReverseMove);
            }

            [[nodiscard]] Iterator begin() const
            {
                return Iterator(*this);
            }

            [[nodiscard]] Sentinel end() const
            {
                return {};
            }

        private:
            ZobristKey m_zobrist;
            std::uint32_t m_packedReverseMove;
            GameResult m_result;
            GameLevel m_level;
            std::uint64_t m_totalWhiteElo;
            std::uint64_t m_totalBlackElo;
            std::uint64_t m_countWithElo;
            std::uint64_t m_count;
            std::uint32_t m_firstGameIndex;
        };

        struct Traits
        {
            static constexpr const char* name = "db_delta_smeared";

            static constexpr std::uint64_t maxGames = 4'000'000'000ull;
            static constexpr std::uint64_t maxPositions = 1'000'000'000'000ull;
            static constexpr std::uint64_t maxInstancesOfSinglePosition = 1'000'000'000'000ull;

            static constexpr bool hasOneWayKey = true;
            static constexpr std::uint64_t estimatedMaxCollisions = 16;
            static constexpr std::uint64_t estimatedMaxPositionsWithNoCollisions = 200'000'000'000ull;

            static constexpr bool hasCount = true;

            static constexpr bool hasEloDiff = true;
            static constexpr std::uint64_t maxAbsEloDiff = SmearedEntry::maxElo - SmearedEntry::minElo;
            static constexpr std::uint64_t maxAverageAbsEloDiff = maxAbsEloDiff;

            static constexpr bool hasWhiteElo = true;
            static constexpr bool hasBlackElo = false;
            static constexpr std::uint64_t minElo = SmearedEntry::minElo;
            static constexpr std::uint64_t maxElo = SmearedEntry::maxElo;
            static constexpr bool hasCountWithElo = true;

            static constexpr bool hasFirstGame = true;
            static constexpr bool hasLastGame = false;

            static constexpr bool allowsFilteringTranspositions = true;
            static constexpr bool hasReverseMove = true;

            static constexpr bool allowsFilteringByEloRange = false;
            static constexpr std::uint64_t eloFilterGranularity = 0;

            static constexpr bool allowsFilteringByMonthRange = false;
            static constexpr std::uint64_t monthFilterGranularity = 0;

            static constexpr std::uint64_t maxBytesPerPosition = 20;
            static constexpr std::optional<double> estimatedAverageBytesPerPosition = 16.0;

            static constexpr util::SemanticVersion version{ 1, 0, 0 };
            static constexpr util::SemanticVersion minimumSupportedVersion{ 1, 0, 0 };
        };

        using Database = persistence::pos_db::OrderedEntrySetPositionDatabase<
            Key,
            UnsmearedEntry,
            Traits
        >;

        extern template struct persistence::pos_db::OrderedEntrySetPositionDatabase<
            Key,
            UnsmearedEntry,
            Traits
        >;

        static_assert(Database::hasEloDiff);
        static_assert(Database::hasWhiteElo);
        static_assert(Database::hasBlackElo);
        static_assert(Database::hasCountWithElo);
        static_assert(Database::hasFirstGameIndex);
        static_assert(!Database::hasLastGameIndex);
        static_assert(!Database::hasFirstGameOffset);
        static_assert(!Database::hasLastGameOffset);
        static_assert(Database::hasReverseMove);

        static_assert(!Database::allowsFilteringByEloRange);
        static_assert(!Database::allowsFilteringByMonthRange);
    }
}
