#pragma once

#include "chess/Chess.h"
#include "chess/Position.h"
#include "chess/MoveIndex.h"

#include "persistence/pos_db/EntryConstructionParameters.h"

#include "persistence/pos_db/OrderedEntrySetPositionDatabase.h"

#include "util/ArithmeticUtility.h"

#include <cstdint>

namespace persistence
{
    namespace db_epsilon
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
                m_hash[2] |= detail::packReverseMove(pos, reverseMove) << reverseMoveShift;
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

            [[nodiscard]] ReverseMove reverseMove(const Position& pos) const
            {
                const std::uint32_t packedInt = (m_hash[2] & reverseMoveMask) >> reverseMoveShift;
                return detail::unpackReverseMove(pos, packedInt);
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

            Entry(const EntryConstructionParameters& params) :
                m_key(params.position, params.reverseMove, params.level, params.result),
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

            [[nodiscard]] ReverseMove reverseMove(const Position& pos) const
            {
                return m_key.reverseMove(pos);
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

        static_assert(!Database::hasEloDiff);
        static_assert(!Database::hasWhiteElo);
        static_assert(!Database::hasBlackElo);
        static_assert(!Database::hasCountWithElo);
        static_assert(!Database::hasFirstGameIndex);
        static_assert(!Database::hasLastGameIndex);
        static_assert(!Database::hasFirstGameOffset);
        static_assert(!Database::hasLastGameOffset);
        static_assert(Database::hasReverseMove);
        static_assert(!Database::hasMonthSinceYear0);

        static_assert(!Database::allowsFilteringByEloRange);
        static_assert(!Database::allowsFilteringByMonthRange);
    }
}
