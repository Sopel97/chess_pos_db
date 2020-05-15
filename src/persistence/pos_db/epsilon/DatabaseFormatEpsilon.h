#pragma once

#include "chess/Chess.h"
#include "chess/Position.h"
#include "chess/MoveIndex.h"

#include "persistence/pos_db/OrderedEntrySetPositionDatabase.h"

#include "util/ArithmeticUtility.h"

#include <cstdint>

namespace persistence
{
    namespace db_epsilon
    {
        static constexpr std::uint64_t invalidGameOffset = std::numeric_limits<std::uint64_t>::max();

        inline uint32_t packReverseMove(const Position& pos, const ReverseMove& rm)
        {
            const Color sideToUnmove = pos.sideToMove();

            uint32_t toSquareIndex;
            uint32_t destinationIndex;
            if (rm.move.type == MoveType::Castle)
            {
                toSquareIndex = 0; // we can set this to zero because destinationIndex is unique

                const bool isKingSide = rm.move.to.file() == fileH;
                destinationIndex = isKingSide ? 30 : 31;
            }
            else if (rm.move.type == MoveType::Promotion)
            {
                toSquareIndex = (bb::before(rm.move.to) & pos.piecesBB(sideToUnmove)).count();
                destinationIndex = std::abs(ordinal(rm.move.to) - ordinal(rm.move.from)) - 7 + 27; // verify
            }
            else
            {
                toSquareIndex = (bb::before(rm.move.to) & pos.piecesBB(sideToUnmove)).count();
                const PieceType pt = pos.pieceAt(rm.move.to).type();
                if (pt == PieceType::Pawn)
                {
                    destinationIndex = move_index::pawnDestinationIndex(rm.move.from, rm.move.to, sideToUnmove, PieceType::None);
                }
                else
                {
                    destinationIndex = move_index::destinationIndex(pt, rm.move.from, rm.move.to);
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

        struct Key
        {
            // Hash:72, ReverseMovePerfectHash:20, GameLevel:2, GameResult:2

            static constexpr std::size_t levelBits = 2;
            static constexpr std::size_t resultBits = 2;

            static constexpr std::uint32_t lastHashPartMask = 0xFF000000u;
            static constexpr std::uint32_t reverseMoveMask = 0x00FFFFF0u;
            static constexpr std::uint32_t levelMask = 0x0000000Cu;
            static constexpr std::uint32_t resultMask = 0x00000003u;

            static constexpr std::uint32_t reverseMoveShift = 4;
            static constexpr std::uint32_t levelShift = 2;

            using StorageType = std::array<std::uint32_t, 3>;

            Key() = default;

            Key(const PositionWithZobrist& pos, const ReverseMove& reverseMove = ReverseMove{})
            {
                const auto zobrist = pos.zobrist();
                m_hash[0] = zobrist.high >> 32;
                m_hash[1] = zobrist.high & 0xFFFFFFFFull;
                m_hash[2] = zobrist.low & lastHashPartMask;
                m_hash[2] |= packReverseMove(pos, reverseMove) << reverseMoveShift;
            }

            Key(const PositionWithZobrist& pos, const ReverseMove& reverseMove, GameLevel level, GameResult result) :
                Key(pos, reverseMove)
            {
                m_hash[2] |=
                    ((ordinal(level) & levelMask) << levelShift)
                    | ((ordinal(result) & resultMask));
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
                return fromOrdinal<GameLevel>((m_hash[2] >> levelShift) & levelMask);
            }

            [[nodiscard]] GameResult result() const
            {
                return fromOrdinal<GameResult>(m_hash[2] & resultMask);
            }

            struct CompareLessWithReverseMove
            {
                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    if (lhs.m_hash[0] < rhs.m_hash[0]) return true;
                    else if (lhs.m_hash[0] > rhs.m_hash[0]) return false;

                    if (lhs.m_hash[1] < rhs.m_hash[1]) return true;
                    else if (lhs.m_hash[1] > rhs.m_hash[1]) return false;

                    return
                        (lhs.m_hash[2] & (reverseMoveMask | lastHashPartMask))
                        < (rhs.m_hash[2] & (reverseMoveMask | lastHashPartMask));
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

                    return ((lhs.m_hash[2] & lastHashPartMask) < (rhs.m_hash[2] & lastHashPartMask));
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

                    return (lhs.m_hash[2] < rhs.m_hash[2]);
                }
            };

            struct CompareEqualWithReverseMove
            {
                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return
                        lhs.m_hash[0] == rhs.m_hash[0]
                        && lhs.m_hash[1] == rhs.m_hash[1]
                        &&
                        (lhs.m_hash[2] & (reverseMoveMask | lastHashPartMask))
                        == (rhs.m_hash[2] & (reverseMoveMask | lastHashPartMask));
                }
            };

            struct CompareEqualWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return
                        lhs.m_hash[0] == rhs.m_hash[0]
                        && lhs.m_hash[1] == rhs.m_hash[1]
                        && (lhs.m_hash[2] & lastHashPartMask) == (rhs.m_hash[2] & lastHashPartMask);
                }
            };

            struct CompareEqualFull
            {
                [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                {
                    return
                        lhs.m_hash[0] == rhs.m_hash[0]
                        && lhs.m_hash[1] == rhs.m_hash[1]
                        && lhs.m_hash[2] == rhs.m_hash[2];
                }
            };

        private:
            // All bits of the hash are created equal, so we can specify some ordering.
            // Elements ordered from least significant to most significant are [2][1][0]
            StorageType m_hash;
        };
        static_assert(sizeof(Key) == 12);

        struct Entry
        {
            Entry() = default;

            Entry(const PositionWithZobrist& pos, const ReverseMove& reverseMove, GameLevel level, GameResult result) :
                m_key(pos, reverseMove, level, result),
                m_count(1)
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

            [[nodiscard]] std::uint32_t count() const
            {
                return m_count;
            }

            [[nodiscard]] GameLevel level() const
            {
                return m_key.level();
            }

            [[nodiscard]] GameResult result() const
            {
                return m_key.result();
            }

            void combine(const Entry& rhs)
            {
                m_count += rhs.m_count;
            }

        private:
            Key m_key;
            std::uint32_t m_count;
        };

        static_assert(sizeof(Entry) == 16);
        static_assert(std::is_trivially_copyable_v<Entry>);

        struct Traits
        {
            static constexpr const char* name = "db_epsilon";
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
