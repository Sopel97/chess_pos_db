#pragma once

#include "chess/Chess.h"
#include "chess/Position.h"

#include "persistence/pos_db/EntryConstructionParameters.h"

#include "persistence/pos_db/OrderedEntrySetPositionDatabase.h"

#include "util/ArithmeticUtility.h"

#include <cstdint>

namespace persistence
{
    namespace db_delta
    {
        // Have ranges of mixed values be at most this long
        struct alignas(32) Entry
        {
            // Hash              : 64
            // Elo diff + Hash   : 40 + 24
            // PackedReverseMove : 27, GameLevel : 2, GameResult : 2, padding : 1
            // Count             : 32
            // First game index  : 32
            // Last game index   : 32
            // Total             : 64b * 4 = 256b = 32B

            // Hash:96, 

            using GameIndexType = std::uint32_t;

            static constexpr std::size_t additionalHashBits = 24;

            static constexpr std::size_t levelBits = 2;
            static constexpr std::size_t resultBits = 2;

            static constexpr std::uint32_t reverseMoveShift = 32 - PackedReverseMove::numBits;
            static constexpr std::uint32_t levelShift = reverseMoveShift - levelBits;
            static constexpr std::uint32_t resultShift = levelShift - resultBits;

            static constexpr std::uint32_t levelMask = 0b11;
            static constexpr std::uint32_t resultMask = 0b11;

            static_assert(PackedReverseMove::numBits + levelBits + resultBits <= 32);

            Entry() :
                m_hashPart1{},
                m_eloDiffAndHashPart2(0),
                m_packedInfo{},
                m_count(0),
                m_firstGameIndex(std::numeric_limits<std::uint32_t>::max()),
                m_lastGameIndex(0)
            {
            }

            Entry(const PositionWithZobrist & pos, const ReverseMove & reverseMove = ReverseMove{}) :
                m_count(1),
                m_firstGameIndex(std::numeric_limits<std::uint32_t>::max()),
                m_lastGameIndex(0)
            {
                const auto zobrist = pos.zobrist();
                m_hashPart1 = zobrist.high;
                m_eloDiffAndHashPart2 = (zobrist.low & nbitmask<std::uint64_t>[additionalHashBits]);

                auto packedReverseMove = PackedReverseMove(reverseMove);
                // m_hash[0] is the most significant quad, m_hash[3] is the least significant
                // We want entries ordered with reverse move to also be ordered by just hash
                // so we have to modify the lowest bits.
                m_packedInfo = (packedReverseMove.packed() << reverseMoveShift);
            }

            Entry(const EntryConstructionParameters& params) :
                m_count(1),
                m_firstGameIndex(static_cast<std::uint32_t>(params.gameIndexOrOffset)),
                m_lastGameIndex(static_cast<std::uint32_t>(params.gameIndexOrOffset))
            {
                const auto zobrist = params.position.zobrist();
                m_hashPart1 = zobrist.high;
                m_eloDiffAndHashPart2 =
                    (static_cast<std::uint64_t>(params.eloDiff()) << additionalHashBits)
                    | (zobrist.low & nbitmask<std::uint64_t>[additionalHashBits]);

                auto packedReverseMove = PackedReverseMove(params.reverseMove);
                // m_hash[0] is the most significant quad, m_hash[3] is the least significant
                // We want entries ordered with reverse move to also be ordered by just hash
                // so we have to modify the lowest bits.
                m_packedInfo =
                    (packedReverseMove.packed() << reverseMoveShift)
                    | ((ordinal(params.level) & levelMask) << levelShift)
                    | ((ordinal(params.result) & resultMask) << resultShift);
            }

            Entry(const Entry&) = default;
            Entry(Entry&&) = default;
            Entry& operator=(const Entry&) = default;
            Entry& operator=(Entry&&) = default;

            [[nodiscard]] GameLevel level() const
            {
                return fromOrdinal<GameLevel>((m_packedInfo >> levelShift) & levelMask);
            }

            [[nodiscard]] GameResult result() const
            {
                return fromOrdinal<GameResult>((m_packedInfo >> resultShift) & resultMask);
            }

            [[nodiscard]] std::int64_t eloDiff() const
            {
                return signExtend<64 - additionalHashBits>(m_eloDiffAndHashPart2 >> additionalHashBits);
            }

            [[nodiscard]] std::array<std::uint64_t, 2> hash() const
            {
                return std::array<std::uint64_t, 2>{ m_hashPart1, ((m_eloDiffAndHashPart2& nbitmask<std::uint64_t>[additionalHashBits]) << 32) | m_packedInfo };
            }

            [[nodiscard]] Entry key() const
            {
                return *this;
            }

            [[nodiscard]] std::uint32_t count() const
            {
                return m_count;
            }

            [[nodiscard]] std::uint32_t firstGameIndex() const
            {
                return m_firstGameIndex;
            }

            [[nodiscard]] std::uint32_t lastGameIndex() const
            {
                return m_lastGameIndex;
            }

            [[nodiscard]] ReverseMove reverseMove(const Position& pos) const
            {
                const Color sideThatMoved = !pos.sideToMove();
                std::uint32_t packedInt = (m_packedInfo >> reverseMoveShift) & PackedReverseMove::mask;
                PackedReverseMove packedReverseMove(packedInt);
                return packedReverseMove.unpack(sideThatMoved);
            }

            void combine(const Entry & other)
            {
                m_eloDiffAndHashPart2 += other.m_eloDiffAndHashPart2 & ~nbitmask<std::uint64_t>[additionalHashBits];
                m_count += other.m_count;
                const auto newFirstGame = std::min(m_firstGameIndex, other.m_firstGameIndex);
                const auto newLastGame = std::max(m_lastGameIndex, other.m_lastGameIndex);
                m_firstGameIndex = newFirstGame;
                m_lastGameIndex = newLastGame;
            }

            struct CompareLessWithReverseMove
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    if (lhs.m_hashPart1 < rhs.m_hashPart1) return true;
                    else if (lhs.m_hashPart1 > rhs.m_hashPart1) return false;

                    const auto lhsAdditionalHash = lhs.additionalHash();
                    const auto rhsAdditionalHash = rhs.additionalHash();
                    if (lhsAdditionalHash < rhsAdditionalHash) return true;
                    else if (lhsAdditionalHash > rhsAdditionalHash) return false;

                    return ((lhs.m_packedInfo & (PackedReverseMove::mask << reverseMoveShift)) < (rhs.m_packedInfo & (PackedReverseMove::mask << reverseMoveShift)));
                }
            };

            struct CompareLessWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    if (lhs.m_hashPart1 < rhs.m_hashPart1) return true;
                    else if (lhs.m_hashPart1 > rhs.m_hashPart1) return false;

                    const auto lhsAdditionalHash = lhs.additionalHash();
                    const auto rhsAdditionalHash = rhs.additionalHash();
                    return lhsAdditionalHash < rhsAdditionalHash;
                }
            };

            struct CompareLessFull
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    if (lhs.m_hashPart1 < rhs.m_hashPart1) return true;
                    else if (lhs.m_hashPart1 > rhs.m_hashPart1) return false;

                    const auto lhsAdditionalHash = lhs.additionalHash();
                    const auto rhsAdditionalHash = rhs.additionalHash();
                    if (lhsAdditionalHash < rhsAdditionalHash) return true;
                    else if (lhsAdditionalHash > rhsAdditionalHash) return false;

                    return lhs.m_packedInfo < rhs.m_packedInfo;
                }
            };

            struct CompareEqualWithReverseMove
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    return
                        lhs.m_hashPart1 == rhs.m_hashPart1
                        && lhs.additionalHash() == rhs.additionalHash()
                        && (lhs.m_packedInfo & (PackedReverseMove::mask << reverseMoveShift)) == (rhs.m_packedInfo & (PackedReverseMove::mask << reverseMoveShift));
                }
            };

            struct CompareEqualWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    return
                        lhs.m_hashPart1 == rhs.m_hashPart1
                        && lhs.additionalHash() == rhs.additionalHash();
                }
            };

            struct CompareEqualFull
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    return
                        lhs.m_hashPart1 == rhs.m_hashPart1
                        && lhs.additionalHash() == rhs.additionalHash()
                        && lhs.m_packedInfo == rhs.m_packedInfo;
                }
            };

        private:
            std::uint64_t m_hashPart1;
            std::uint64_t m_eloDiffAndHashPart2;
            std::uint32_t m_packedInfo;
            std::uint32_t m_count;
            std::uint32_t m_firstGameIndex;
            std::uint32_t m_lastGameIndex;

            [[nodiscard]] std::uint32_t additionalHash() const
            {
                return m_hashPart1 & nbitmask<std::uint32_t>[additionalHashBits];
            }
        };
        static_assert(sizeof(Entry) == 32);

        static_assert(std::is_trivially_copyable_v<Entry>);

        // TODO: A more compact key type.
        using Key = Entry;

        struct Traits
        {
            static constexpr const char* name = "db_delta";

            static constexpr std::uint64_t maxGames = 1ull << 32ull;
            static constexpr std::uint64_t maxPositions = 1ull << 40ull;
            static constexpr std::uint64_t maxInstancesOfSinglePosition = 1ull << 32ull;

            static constexpr bool hasOneWayKey = true;
            static constexpr std::uint64_t estimatedMaxCollisions = 0;
            static constexpr std::uint64_t estimatedMaxPositionsWithNoCollisions = maxPositions;

            static constexpr bool hasCount = true;

            static constexpr bool hasEloDiff = true;
            static constexpr std::uint64_t maxAbsEloDiff = 4000;
            static constexpr std::uint64_t maxAverageAbsEloDiff = 256;

            static constexpr bool hasWhiteElo = false;
            static constexpr bool hasBlackElo = false;
            static constexpr std::uint64_t minElo = 0;
            static constexpr std::uint64_t maxElo = 0;
            static constexpr bool hasCountWithElo = false;

            static constexpr bool hasFirstGame = true;
            static constexpr bool hasLastGame = true;

            static constexpr bool allowsFilteringTranspositions = true;
            static constexpr bool hasReverseMove = true;

            static constexpr bool allowsFilteringByEloRange = false;
            static constexpr std::uint64_t eloFilterGranularity = 0;

            static constexpr bool allowsFilteringByMonthRange = false;
            static constexpr std::uint64_t monthFilterGranularity = 0;

            static constexpr std::uint64_t maxBytesPerPosition = 32;
            static constexpr std::optional<double> estimatedAverageBytesPerPosition = 26.0;

            static constexpr util::SemanticVersion version{ 1, 0, 0 };
            static constexpr util::SemanticVersion minimumSupportedVersion{ 1, 0, 0 };
        };

        using Database = persistence::pos_db::OrderedEntrySetPositionDatabase<
            Key,
            Entry,
            Traits
        >;

        extern template struct persistence::pos_db::OrderedEntrySetPositionDatabase<
            Key,
            Entry,
            Traits
        >;

        static_assert(Database::hasEloDiff);
        static_assert(!Database::hasWhiteElo);
        static_assert(!Database::hasBlackElo);
        static_assert(!Database::hasCountWithElo);
        static_assert(Database::hasFirstGameIndex);
        static_assert(Database::hasLastGameIndex);
        static_assert(!Database::hasFirstGameOffset);
        static_assert(!Database::hasLastGameOffset);
        static_assert(Database::hasReverseMove);

        static_assert(!Database::allowsFilteringByEloRange);
        static_assert(!Database::allowsFilteringByMonthRange);
    }
}
