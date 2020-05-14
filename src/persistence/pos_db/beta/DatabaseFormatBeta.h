#pragma once

#include "chess/Chess.h"
#include "chess/Position.h"

#include "persistence/pos_db/OrderedEntrySetPositionDatabase.h"

#include "util/ArithmeticUtility.h"

#include <cstdint>

namespace persistence
{
    namespace db_beta
    {
        static constexpr std::uint64_t invalidGameOffset = std::numeric_limits<std::uint64_t>::max();

        struct Key
        {
            // Hash:96, PackedReverseMove:27, GameLevel:2, GameResult:2, padding:1

            static constexpr std::size_t levelBits = 2;
            static constexpr std::size_t resultBits = 2;

            static constexpr std::uint32_t reverseMoveShift = 32 - PackedReverseMove::numBits;
            static constexpr std::uint32_t levelShift = reverseMoveShift - levelBits;
            static constexpr std::uint32_t resultShift = levelShift - resultBits;

            static constexpr std::uint32_t levelMask = 0b11;
            static constexpr std::uint32_t resultMask = 0b11;

            static_assert(PackedReverseMove::numBits + levelBits + resultBits <= 32);

            using StorageType = std::array<std::uint32_t, 4>;

            Key() = default;

            Key(const PositionWithZobrist& pos, const ReverseMove& reverseMove = ReverseMove{})
            {
                const auto zobrist = pos.zobrist();
                m_hash[0] = zobrist.high >> 32;
                m_hash[1] = zobrist.high & 0xFFFFFFFFull;
                m_hash[2] = zobrist.low >> 32;

                auto packedReverseMove = PackedReverseMove(reverseMove);
                // m_hash[0] is the most significant quad, m_hash[3] is the least significant
                // We want entries ordered with reverse move to also be ordered by just hash
                // so we have to modify the lowest bits.
                m_hash[3] = (packedReverseMove.packed() << reverseMoveShift);
            }

            Key(const PositionWithZobrist& pos, const ReverseMove& reverseMove, GameLevel level, GameResult result) :
                Key(pos, reverseMove)
            {
                m_hash[3] |=
                    ((ordinal(level) & levelMask) << levelShift)
                    | ((ordinal(result) & resultMask) << resultShift);
            }

            Key(const Key&) = default;
            Key(Key&&) = default;
            Key& operator=(const Key&) = default;
            Key& operator=(Key&&) = default;

            [[nodiscard]] const StorageType& hash() const
            {
                return m_hash;
            }

            [[nodiscard]] GameLevel level() const
            {
                return fromOrdinal<GameLevel>((m_hash[3] >> levelShift) & levelMask);
            }

            [[nodiscard]] GameResult result() const
            {
                return fromOrdinal<GameResult>((m_hash[3] >> resultShift) & resultMask);
            }

            [[nodiscard]] ReverseMove reverseMove(const Position& pos) const
            {
                const Color sideThatMoved = !pos.sideToMove();
                std::uint32_t packedInt = (m_hash[3] >> reverseMoveShift) & PackedReverseMove::mask;
                PackedReverseMove packedReverseMove(packedInt);
                return packedReverseMove.unpack(sideThatMoved);
            }

            struct CompareLessWithReverseMove
            {
                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    if (lhs.m_hash[0] < rhs.m_hash[0]) return true;
                    else if (lhs.m_hash[0] > rhs.m_hash[0]) return false;

                    if (lhs.m_hash[1] < rhs.m_hash[1]) return true;
                    else if (lhs.m_hash[1] > rhs.m_hash[1]) return false;

                    if (lhs.m_hash[2] < rhs.m_hash[2]) return true;
                    else if (lhs.m_hash[2] > rhs.m_hash[2]) return false;

                    return ((lhs.m_hash[3] & (PackedReverseMove::mask << reverseMoveShift)) < (rhs.m_hash[3] & (PackedReverseMove::mask << reverseMoveShift)));
                }
            };

            struct CompareLessWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    if (lhs.m_hash[0] < rhs.m_hash[0]) return true;
                    else if (lhs.m_hash[0] > rhs.m_hash[0]) return false;

                    if (lhs.m_hash[1] < rhs.m_hash[1]) return true;
                    else if (lhs.m_hash[1] > rhs.m_hash[1]) return false;

                    return (lhs.m_hash[2] < rhs.m_hash[2]);
                }
            };

            struct CompareLessFull
            {
                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    if (lhs.m_hash[0] < rhs.m_hash[0]) return true;
                    else if (lhs.m_hash[0] > rhs.m_hash[0]) return false;

                    if (lhs.m_hash[1] < rhs.m_hash[1]) return true;
                    else if (lhs.m_hash[1] > rhs.m_hash[1]) return false;

                    if (lhs.m_hash[2] < rhs.m_hash[2]) return true;
                    else if (lhs.m_hash[2] > rhs.m_hash[2]) return false;

                    return (lhs.m_hash[3] < rhs.m_hash[3]);
                }
            };

            struct CompareEqualWithReverseMove
            {
                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return
                        lhs.m_hash[0] == rhs.m_hash[0]
                        && lhs.m_hash[1] == rhs.m_hash[1]
                        && lhs.m_hash[2] == rhs.m_hash[2]
                        && (lhs.m_hash[3] & (PackedReverseMove::mask << reverseMoveShift)) == (rhs.m_hash[3] & (PackedReverseMove::mask << reverseMoveShift));
                }
            };

            struct CompareEqualWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return
                        lhs.m_hash[0] == rhs.m_hash[0]
                        && lhs.m_hash[1] == rhs.m_hash[1]
                        && lhs.m_hash[2] == rhs.m_hash[2];
                }
            };

            struct CompareEqualFull
            {
                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return
                        lhs.m_hash[0] == rhs.m_hash[0]
                        && lhs.m_hash[1] == rhs.m_hash[1]
                        && lhs.m_hash[2] == rhs.m_hash[2]
                        && lhs.m_hash[3] == rhs.m_hash[3];
                }
            };

        private:
            // All bits of the hash are created equal, so we can specify some ordering.
            // Elements ordered from least significant to most significant are [3][2][1][0]
            StorageType m_hash;
        };
        static_assert(sizeof(Key) == 16);

        struct PackedCountAndGameOffset;

        struct SingleGame {};

        struct CountAndGameOffset
        {
            CountAndGameOffset() :
                m_count(0),
                m_gameOffset(invalidGameOffset)
            {
            }

            CountAndGameOffset(std::uint64_t count, std::uint64_t gameOffset) :
                m_count(count),
                m_gameOffset(gameOffset)
            {
            }

            CountAndGameOffset(SingleGame, std::uint64_t gameOffset) :
                m_count(1),
                m_gameOffset(gameOffset)
            {
            }

            CountAndGameOffset& operator+=(std::uint64_t rhs)
            {
                m_count += rhs;
                return *this;
            }

            CountAndGameOffset operator+(std::uint64_t rhs)
            {
                return { m_count + rhs, m_gameOffset };
            }

            void combine(const CountAndGameOffset& rhs)
            {
                m_count += rhs.m_count;
                m_gameOffset = std::min(m_gameOffset, rhs.m_gameOffset);
            }

            void combine(const PackedCountAndGameOffset& rhs);

            [[nodiscard]] std::uint64_t count() const
            {
                return m_count;
            }

            [[nodiscard]] std::uint64_t gameOffset() const
            {
                return m_gameOffset;
            }

        private:
            std::uint64_t m_count;
            std::uint64_t m_gameOffset;
        };

        static_assert(sizeof(CountAndGameOffset) == 16);

        struct PackedCountAndGameOffset
        {
            // game offset is invalid if we don't have enough bits to store it
            // ie. count takes all the bits
            static constexpr std::uint64_t numSizeBits = 6;

            // numCountBits should always be at least 1 to avoid shifting by 64
            static constexpr std::uint64_t numDataBits = 64 - numSizeBits;

            static constexpr std::uint64_t mask = std::numeric_limits<std::uint64_t>::max();
            static constexpr std::uint64_t sizeMask = 0b111111;

            PackedCountAndGameOffset()
            {
                setNone();
            }

            PackedCountAndGameOffset(const CountAndGameOffset& unpacked)
            {
                pack(unpacked);
            }

            PackedCountAndGameOffset(std::uint64_t count, std::uint64_t gameOffset)
            {
                pack(count, gameOffset);
            }

            PackedCountAndGameOffset(SingleGame, std::uint64_t gameOffset)
            {
                pack(SingleGame{}, gameOffset);
            }

            [[nodiscard]] CountAndGameOffset unpack() const
            {
                const std::uint64_t s = countLength();
                const std::uint64_t countMask = mask >> (64 - s);

                const std::uint64_t data = m_packed >> numSizeBits;

                const std::uint64_t count = data & countMask;
                const std::uint64_t gameOffset =
                    (s == numDataBits)
                    ? invalidGameOffset
                    : (data >> s);

                return { count, gameOffset };
            }

            PackedCountAndGameOffset& operator+=(std::uint64_t rhs)
            {
                pack(unpack() + rhs);
                return *this;
            }

            void combine(const PackedCountAndGameOffset& rhs)
            {
                auto unpacked = unpack();

                unpacked.combine(rhs.unpack());

                pack(unpacked);
            }

            void combine(const CountAndGameOffset& rhs)
            {
                auto unpacked = unpack();

                unpacked.combine(rhs);

                pack(unpacked);
            }

            [[nodiscard]] std::uint64_t count() const
            {
                const std::uint64_t countMask = mask >> (64 - countLength());
                return (m_packed >> numSizeBits) & countMask;
            }

            [[nodiscard]] std::uint64_t firstGameOffset() const
            {
                const std::uint64_t s = countLength();
                if (s == numDataBits) return invalidGameOffset;
                return (m_packed >> (numSizeBits + s));
            }

        private:
            // from least significant:
            // 6 bits for number N of count bits, at most 58
            // N bits for count
            // 58-N bits for game offset

            std::uint64_t m_packed;

            void setNone()
            {
                m_packed = numDataBits;
            }

            void pack(std::uint64_t count, std::uint64_t gameOffset)
            {
                const std::uint64_t countSize = count ? intrin::msb(count) + 1 : 1;
                const std::uint64_t gameOffsetSize = gameOffset ? intrin::msb(gameOffset) + 1 : 1;
                if (countSize + gameOffsetSize > numDataBits)
                {
                    // We cannot fit both so we just store count
                    m_packed = (count << numSizeBits) | numDataBits;
                }
                else
                {
                    // We can fit both
                    m_packed = gameOffset;
                    m_packed <<= countSize;
                    m_packed |= count;
                    m_packed <<= numSizeBits;
                    m_packed |= countSize;
                }
            }

            void pack(SingleGame, std::uint64_t gameOffset)
            {
                // We assume that we can fit both.
                // For otherwise to happen gameOffset would be too big anyway.
                m_packed = gameOffset;
                m_packed <<= (numSizeBits + 1);
                m_packed |= ((1 << numSizeBits) | 1);
            }

            void pack(const CountAndGameOffset& rhs)
            {
                pack(rhs.count(), rhs.gameOffset());
            }

            [[nodiscard]] std::uint64_t countLength() const
            {
                return m_packed & sizeMask;
            }
        };

        inline void CountAndGameOffset::combine(const PackedCountAndGameOffset& rhs)
        {
            combine(rhs.unpack());
        }

        static_assert(sizeof(PackedCountAndGameOffset) == 8);

        using CountAndGameOffsetType = PackedCountAndGameOffset;

        struct Entry
        {
            Entry() = default;

            Entry(const PositionWithZobrist& pos, const ReverseMove& reverseMove, GameLevel level, GameResult result, std::uint64_t gameOffset) :
                m_key(pos, reverseMove, level, result),
                m_countAndGameOffset(SingleGame{}, gameOffset)
            {
            }

            Entry(const Entry&) = default;
            Entry(Entry&&) = default;
            Entry& operator=(const Entry&) = default;
            Entry& operator=(Entry&&) = default;

            struct CompareLessWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareLessWithoutReverseMove{}(lhs.m_key, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Entry& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareLessWithoutReverseMove{}(lhs.m_key, rhs);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareLessWithoutReverseMove{}(lhs, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareLessWithoutReverseMove{}(lhs, rhs);
                }
            };

            struct CompareEqualWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareEqualWithoutReverseMove{}(lhs.m_key, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Entry& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareEqualWithoutReverseMove{}(lhs.m_key, rhs);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareEqualWithoutReverseMove{}(lhs, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareEqualWithoutReverseMove{}(lhs, rhs);
                }
            };

            struct CompareLessWithReverseMove
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareLessWithReverseMove{}(lhs.m_key, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Entry& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareLessWithReverseMove{}(lhs.m_key, rhs);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareLessWithReverseMove{}(lhs, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareLessWithReverseMove{}(lhs, rhs);
                }
            };

            struct CompareEqualWithReverseMove
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareEqualWithReverseMove{}(lhs.m_key, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Entry& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareEqualWithReverseMove{}(lhs.m_key, rhs);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareEqualWithReverseMove{}(lhs, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareEqualWithReverseMove{}(lhs, rhs);
                }
            };

            // This behaves like the old operator<
            struct CompareLessFull
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareLessFull{}(lhs.m_key, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Entry& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareLessFull{}(lhs.m_key, rhs);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareLessFull{}(lhs, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareLessFull{}(lhs, rhs);
                }
            };

            struct CompareEqualFull
            {
                [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareEqualFull{}(lhs.m_key, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Entry& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareEqualFull{}(lhs.m_key, rhs);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Entry& rhs) const noexcept
                {
                    return Key::CompareEqualFull{}(lhs, rhs.m_key);
                }

                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return Key::CompareEqualFull{}(lhs, rhs);
                }
            };

            [[nodiscard]] const Key& key() const
            {
                return m_key;
            }

            [[nodiscard]] std::uint64_t count() const
            {
                return m_countAndGameOffset.count();
            }

            [[nodiscard]] std::uint64_t firstGameOffset() const
            {
                return m_countAndGameOffset.firstGameOffset();
            }

            [[nodiscard]] GameLevel level() const
            {
                return m_key.level();
            }

            [[nodiscard]] GameResult result() const
            {
                return m_key.result();
            }

            [[nodiscard]] const CountAndGameOffsetType& countAndGameOffset() const
            {
                return m_countAndGameOffset;
            }

            void combine(const Entry& rhs)
            {
                m_countAndGameOffset.combine(rhs.m_countAndGameOffset);
            }

            [[nodiscard]] ReverseMove reverseMove(const Position& pos) const
            {
                return m_key.reverseMove(pos);
            }

        private:
            Key m_key;
            CountAndGameOffsetType m_countAndGameOffset;
        };

        static_assert(sizeof(Entry) == 24);
        static_assert(std::is_trivially_copyable_v<Entry>);

        struct Traits
        {
            static constexpr const char* name = "db_beta";
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
    }
}
