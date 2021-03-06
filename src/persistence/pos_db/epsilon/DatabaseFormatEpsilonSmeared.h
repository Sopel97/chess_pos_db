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
    namespace db_epsilon_smeared
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
                - 64 bits hash

                - 24 bits hash
                - 20 bits reverse move
                - 2 bit result
                - 2 bit level
                - 2 bit count
                - 12 bits abs elo diff
                - 1 bit first
                - 1 bit elo diff sign
            */

            using HashLow           = util::BitField<std::uint64_t, std::uint64_t, 0xFFFF'FF00'0000'0000ull>;
            using PackedReverseMove = util::BitField<std::uint64_t, std::uint64_t, 0x0000'00FF'FFF0'0000ull>;
            using Result            = util::BitField<std::uint64_t, std::uint64_t, 0x0000'0000'000C'0000ull>;
            using Level             = util::BitField<std::uint64_t, std::uint64_t, 0x0000'0000'0003'0000ull>;
            using Count             = util::BitField<std::uint64_t, std::uint64_t, 0x0000'0000'0000'C000ull>;
            using AbsEloDiff        = util::BitField<std::uint64_t, std::uint64_t, 0x0000'0000'0000'3FFCull>;
            using IsFirst           = util::BitField<std::uint64_t, std::uint64_t, 0x0000'0000'0000'0002ull>;
            using EloDiffSign       = util::BitField<std::uint64_t, std::uint64_t, 0x0000'0000'0000'0001ull>;

            using Rest = util::PackedInts<
                HashLow,
                PackedReverseMove,
                Result,
                Level,
                Count,
                AbsEloDiff,
                IsFirst,
                EloDiffSign
            >;

            static constexpr std::int64_t maxAbsEloDiff = 800;

            friend struct UnsmearedEntry;

            SmearedEntry() :
                m_hash{},
                m_rest(IsFirst::mask)
            {
            }

            SmearedEntry(const PositionWithZobrist& pos, const ReverseMove& reverseMove = ReverseMove{}) :
                m_rest(IsFirst::mask) /* | (0 << countShift) because 0 means one entry*/
            {
                const auto zobrist = pos.zobrist();
                m_hash = zobrist.high;
                m_rest.init<HashLow>(zobrist.low);

                auto packedReverseMove = detail::packReverseMove(pos, reverseMove);
                // m_hash[0] is the most significant quad, m_hash[3] is the least significant
                // We want entries ordered with reverse move to also be ordered by just hash
                // so we have to modify the lowest bits.
                m_rest.init<PackedReverseMove>(packedReverseMove);
            }

            SmearedEntry(const EntryConstructionParameters& params) :
                m_rest(
                    util::meta::TypeList<IsFirst, Level, Result>{},
                    true,
                    /* | (0 << countShift) because 0 means one entry*/
                    ordinal(params.level),
                    ordinal(params.result)
                )
            {
                const auto eloDiff = params.eloDiff();
                const std::uint32_t eloDiffSign = eloDiff < 0;
                const auto zobrist = params.position.zobrist();
                m_hash = zobrist.high;
                m_rest.init<HashLow>(zobrist.low);
                m_rest.init<EloDiffSign>(eloDiffSign);
                
                const std::uint32_t absEloDiff = std::min<std::uint32_t>(static_cast<std::uint32_t>(std::abs(eloDiff)), maxAbsEloDiff);
                auto packedReverseMove = detail::packReverseMove(params.position, params.reverseMove);
                // m_hash[0] is the most significant quad, m_hash[3] is the least significant
                // We want entries ordered with reverse move to also be ordered by just hash
                // so we have to modify the lowest bits.
                m_rest.init<AbsEloDiff>(absEloDiff);
                m_rest.init<PackedReverseMove>(packedReverseMove);
            }

            SmearedEntry(const SmearedEntry&) = default;
            SmearedEntry(SmearedEntry&&) = default;
            SmearedEntry& operator=(const SmearedEntry&) = default;
            SmearedEntry& operator=(SmearedEntry&&) = default;

            [[nodiscard]] GameLevel level() const
            {
                return fromOrdinal<GameLevel>(static_cast<int>(m_rest.get<Level>()));
            }

            [[nodiscard]] GameResult result() const
            {
                return fromOrdinal<GameResult>(static_cast<int>(m_rest.get<Result>()));
            }

            [[nodiscard]] std::uint32_t absEloDiff() const
            {
                return static_cast<std::uint32_t>(m_rest.get<AbsEloDiff>());
            }

            [[nodiscard]] std::array<std::uint64_t, 2> hash() const
            {
                return std::array<std::uint64_t, 2>{
                    m_hash,
                    m_rest.get<HashLow>()
                };
            }

            [[nodiscard]] SmearedEntry key() const
            {
                return *this;
            }

            [[nodiscard]] std::uint32_t countMinusOne() const
            {
                return static_cast<std::uint32_t>(m_rest.get<Count>());
            }

            [[nodiscard]] ReverseMove reverseMove(const Position& pos) const
            {
                const std::uint32_t packedInt = static_cast<std::uint32_t>(m_rest.get<PackedReverseMove>());
                return detail::unpackReverseMove(pos, packedInt);
            }

            [[nodiscard]] bool isFirst() const
            {
                return m_rest.getRaw<IsFirst>();
            }

            [[nodiscard]] bool isEloNegative() const
            {
                return m_rest.getRaw<EloDiffSign>();
            }

            struct CompareLessWithReverseMove
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    if (lhs.m_hash < rhs.m_hash) return true;
                    else if (lhs.m_hash > rhs.m_hash) return false;

                    const auto lhsRest = lhs.m_rest.getRaw<HashLow, PackedReverseMove>();
                    const auto rhsRest = rhs.m_rest.getRaw<HashLow, PackedReverseMove>();
                    return lhsRest < rhsRest;
                }
            };

            struct CompareLessWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    if (lhs.m_hash < rhs.m_hash) return true;
                    else if (lhs.m_hash > rhs.m_hash) return false;

                    const auto lhsRest = lhs.m_rest.getRaw<HashLow>();
                    const auto rhsRest = rhs.m_rest.getRaw<HashLow>();
                    return lhsRest < rhsRest;
                }
            };

            struct CompareLessFull
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    if (lhs.m_hash < rhs.m_hash) return true;
                    else if (lhs.m_hash > rhs.m_hash) return false;

                    // Relative order of packed reverse move and level/result makes
                    // the packed reverse move more significant as it should be.
                    // Also hash is the most significant.
                    const auto lhsRest = lhs.m_rest.getRaw<HashLow, PackedReverseMove, Level, Result>();
                    const auto rhsRest = rhs.m_rest.getRaw<HashLow, PackedReverseMove, Level, Result>();
                    return lhsRest < rhsRest;
                }
            };

            struct CompareEqualWithReverseMove
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    return
                        lhs.m_hash == rhs.m_hash
                        && lhs.m_rest.getRaw<HashLow, PackedReverseMove>() == rhs.m_rest.getRaw<HashLow, PackedReverseMove>();
                }
            };

            struct CompareEqualWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    return
                        lhs.m_hash == rhs.m_hash
                        && lhs.m_rest.getRaw<HashLow>() == rhs.m_rest.getRaw<HashLow>();
                }
            };

            struct CompareEqualFull
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    return
                        lhs.m_hash == rhs.m_hash
                        && lhs.m_rest.getRaw<HashLow, PackedReverseMove, Level, Result>() == rhs.m_rest.getRaw<HashLow, PackedReverseMove, Level, Result>();
                }
            };

        private:
            std::uint64_t m_hash;
            Rest m_rest;

            SmearedEntry(
                const ZobristKey& zobrist,
                std::uint32_t packedReverseMove,
                GameLevel level,
                GameResult result,
                std::uint32_t countPart,
                std::uint32_t absEloDiffPart,
                std::uint32_t eloSign,
                std::uint32_t isFirst
                ) :
                m_hash(zobrist.high),
                m_rest(
                    util::meta::TypeList<HashLow, IsFirst, Count, Level, Result, EloDiffSign, PackedReverseMove, AbsEloDiff>{},
                    zobrist.low,
                    isFirst,
                    countPart,
                    ordinal(level),
                    ordinal(result),
                    eloSign,
                    packedReverseMove,
                    absEloDiffPart
                )
            {

            }

            SmearedEntry(
                const ZobristKey& zobrist,
                std::uint32_t packedReverseMove,
                GameLevel level,
                GameResult result
            ) :
                m_hash(zobrist.high),
                m_rest(
                    util::meta::TypeList<HashLow, Level, Result, PackedReverseMove>{},
                    zobrist.low,
                    ordinal(level), 
                    ordinal(result),
                    packedReverseMove
                )
            {

            }
        };
        static_assert(sizeof(SmearedEntry) == 16);

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
                    m_count(unsmeared.m_count - 1),
                    m_absEloDiff(std::abs(unsmeared.eloDiff())),
                    m_packedReverseMove(unsmeared.m_packedReverseMove),
                    m_level(unsmeared.m_level),
                    m_result(unsmeared.m_result),
                    m_eloDiffSign(unsmeared.m_eloDiff < 0),
                    m_isFirst(true)
                {
                }

                [[nodiscard]] SmearedEntry operator*() const
                {
                    return SmearedEntry(
                        m_zobrist,
                        m_packedReverseMove,
                        m_level,
                        m_result,
                        m_count & (SmearedEntry::Count::mask >> SmearedEntry::Count::shift),
                        m_absEloDiff & (SmearedEntry::AbsEloDiff::mask >> SmearedEntry::AbsEloDiff::shift),
                        m_eloDiffSign,
                        m_isFirst
                        );
                }

                Iterator& operator++()
                {
                    m_count >>= SmearedEntry::Count::size;
                    m_absEloDiff >>= SmearedEntry::AbsEloDiff::size;
                    m_isFirst = false;
                    m_eloDiffSign = 0;
                    return *this;
                }

                [[nodiscard]] friend bool operator==(const Iterator& lhs, Sentinel rhs) noexcept
                {
                    return !lhs.m_isFirst && !lhs.m_count;
                }

                [[nodiscard]] friend bool operator!=(const Iterator& lhs, Sentinel rhs) noexcept
                {
                    return !(lhs == rhs);
                }

            private:
                ZobristKey m_zobrist;
                std::uint64_t m_count;
                std::uint64_t m_absEloDiff;
                std::uint32_t m_packedReverseMove;
                GameLevel m_level;
                GameResult m_result;
                std::uint8_t m_eloDiffSign;
                bool m_isFirst;
            };

            UnsmearedEntry() :
                m_zobrist{},
                m_count{},
                m_eloDiff{},
                m_packedReverseMove{},
                m_level{},
                m_result{}
            {

            }

            explicit UnsmearedEntry(const SmearedEntry& smeared)
            {
                ASSERT(smeared.isFirst());

                m_zobrist.high = smeared.m_hash;
                m_zobrist.low = smeared.m_rest.get<SmearedEntry::HashLow>();

                m_count = static_cast<std::uint64_t>(smeared.countMinusOne()) + 1;

                m_eloDiff = smeared.absEloDiff();
                if (smeared.isEloNegative())
                {
                    m_eloDiff = -m_eloDiff;
                }

                m_packedReverseMove = static_cast<std::uint32_t>(smeared.m_rest.get<SmearedEntry::PackedReverseMove>());

                m_level = smeared.level();
                m_result = smeared.result();
            }

            void combine(const UnsmearedEntry& other)
            {
                m_count += other.m_count;
                m_eloDiff += other.m_eloDiff;
            }

            void add(const SmearedEntry& smeared, std::uint32_t position)
            {
                m_count += static_cast<std::uint64_t>(smeared.countMinusOne() + smeared.isFirst()) << (position * SmearedEntry::Count::size);
                const auto absEloDiffChange = static_cast<std::int64_t>(smeared.absEloDiff()) << (position * SmearedEntry::AbsEloDiff::size);
                if (m_eloDiff < 0)
                {
                    m_eloDiff -= absEloDiffChange;
                }
                else
                {
                    m_eloDiff += absEloDiffChange;
                }
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
                return m_eloDiff;
            }

            [[nodiscard]] SmearedEntry key() const
            {
                return SmearedEntry(m_zobrist, m_packedReverseMove, m_level, m_result);
            }

            [[nodiscard]] std::uint64_t count() const
            {
                return m_count;
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
            std::uint64_t m_count;
            std::int64_t m_eloDiff;
            std::uint32_t m_packedReverseMove;
            GameLevel m_level;
            GameResult m_result;
        };

        struct Traits
        {
            static constexpr const char* name = "db_epsilon_smeared_a";

            static constexpr std::uint64_t maxGames = 1'000'000'000'000ull;
            static constexpr std::uint64_t maxPositions = 100'000'000'000'000ull;
            static constexpr std::uint64_t maxInstancesOfSinglePosition = 1'000'000'000'000ull;

            static constexpr bool hasOneWayKey = true;
            static constexpr std::uint64_t estimatedMaxCollisions = 16;
            static constexpr std::uint64_t estimatedMaxPositionsWithNoCollisions = 20'000'000'000'000ull;

            static constexpr bool hasCount = true;

            static constexpr bool hasEloDiff = true;
            static constexpr std::uint64_t maxAbsEloDiff = SmearedEntry::maxAbsEloDiff;
            static constexpr std::uint64_t maxAverageAbsEloDiff = SmearedEntry::maxAbsEloDiff;

            static constexpr bool hasWhiteElo = false;
            static constexpr bool hasBlackElo = false;
            static constexpr std::uint64_t minElo = 0;
            static constexpr std::uint64_t maxElo = 0;
            static constexpr bool hasCountWithElo = false;

            static constexpr bool hasFirstGame = false;
            static constexpr bool hasLastGame = false;

            static constexpr bool allowsFilteringTranspositions = true;
            static constexpr bool hasReverseMove = true;

            static constexpr bool allowsFilteringByEloRange = false;
            static constexpr std::uint64_t eloFilterGranularity = 0;

            static constexpr bool allowsFilteringByMonthRange = false;
            static constexpr std::uint64_t monthFilterGranularity = 0;

            static constexpr std::uint64_t maxBytesPerPosition = 16;
            static constexpr std::optional<double> estimatedAverageBytesPerPosition = 12.0;

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
        static_assert(!Database::hasWhiteElo);
        static_assert(!Database::hasBlackElo);
        static_assert(!Database::hasCountWithElo);
        static_assert(!Database::hasFirstGameIndex);
        static_assert(!Database::hasLastGameIndex);
        static_assert(!Database::hasFirstGameOffset);
        static_assert(!Database::hasLastGameOffset);
        static_assert(Database::hasReverseMove);

        static_assert(!Database::allowsFilteringByEloRange);
        static_assert(!Database::allowsFilteringByMonthRange);
    }
}
