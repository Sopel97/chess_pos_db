#pragma once

#include <algorithm>
#include <cmath>

#include "chess/Chess.h"
#include "chess/Position.h"
#include "chess/MoveIndex.h"

#include "persistence/pos_db/OrderedEntrySetPositionDatabase.h"

#include "util/ArithmeticUtility.h"

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
                - 32 bits hash

                - 32 bits hash

                - 24 bits hash
                - 2 bit result
                - 2 bit level
                - 2 bit count
                - 1 bit first
                - 1 bit elo diff sign

                - 20 bits reverse move
                - 12 bits abs elo diff
            */

            static constexpr std::uint32_t countBits = 2;
            static constexpr std::uint32_t absEloDiffBits = 12;

            static constexpr std::uint32_t countBitsMask = 0x3;
            static constexpr std::uint32_t absEloDiffBitsMask = 0x3FF;

            static constexpr std::int64_t maxAbsEloDiff = 800;

            static constexpr std::uint32_t lastHashPartMask = 0xFFFFFF00u;
            static constexpr std::uint32_t resultMask = 0x0000000C0u;
            static constexpr std::uint32_t levelMask = 0x000000030u;

            static constexpr std::uint32_t lastHashPartShift = 8;
            static constexpr std::uint32_t resultShift = 6;
            static constexpr std::uint32_t levelShift = 4;

            static constexpr std::uint32_t countMask = 0x0000000Cu;
            static constexpr std::uint32_t isFirstMask = 0x00000002u;
            static constexpr std::uint32_t eloDiffSignMask = 0x00000001u;

            static constexpr std::uint32_t countShift = 2;
            static constexpr std::uint32_t isFirstShift = 1;

            static constexpr std::uint32_t reverseMoveMask = 0xFFFFF000u;

            static constexpr std::uint32_t reverseMoveShift = 12;

            static constexpr std::uint32_t absEloDiffMask = 0x00000FFFu;

            friend struct UnsmearedEntry;

            SmearedEntry() :
                m_hash0{},
                m_hash1{},
                m_hashLevelResultCountFlags{isFirstMask},
                m_reverseMoveAndAbsEloDiff{}
            {
            }

            SmearedEntry(const PositionWithZobrist& pos, const ReverseMove& reverseMove = ReverseMove{}) :
                m_hashLevelResultCountFlags(isFirstMask) /* | (0 << countShift) because 0 means one entry*/
            {
                const auto zobrist = pos.zobrist();
                m_hash0 = zobrist.high >> 32;
                m_hash1 = static_cast<std::uint32_t>(zobrist.high);
                m_hashLevelResultCountFlags |= (zobrist.low & lastHashPartMask);

                auto packedReverseMove = detail::packReverseMove(pos, reverseMove);
                // m_hash[0] is the most significant quad, m_hash[3] is the least significant
                // We want entries ordered with reverse move to also be ordered by just hash
                // so we have to modify the lowest bits.
                m_reverseMoveAndAbsEloDiff = packedReverseMove << reverseMoveShift;
            }

            SmearedEntry(
                const PositionWithZobrist& pos,
                const ReverseMove& reverseMove,
                GameLevel level,
                GameResult result,
                std::int64_t eloDiff
            ) :
                m_hashLevelResultCountFlags(
                    isFirstMask 
                    /* | (0 << countShift) because 0 means one entry*/
                    | (ordinal(level) << levelShift)
                    | (ordinal(result) << resultShift)
                )
            {
                const std::uint32_t eloDiffSign = eloDiff < 0;
                const auto zobrist = pos.zobrist();
                m_hash0 = zobrist.high >> 32;
                m_hash1 = static_cast<std::uint32_t>(zobrist.high);
                m_hashLevelResultCountFlags |= 
                    (zobrist.low & lastHashPartMask)
                    | eloDiffSign;

                const std::uint32_t absEloDiff = std::min<std::uint32_t>(static_cast<std::uint32_t>(std::abs(eloDiff)), maxAbsEloDiff);
                auto packedReverseMove = detail::packReverseMove(pos, reverseMove);
                // m_hash[0] is the most significant quad, m_hash[3] is the least significant
                // We want entries ordered with reverse move to also be ordered by just hash
                // so we have to modify the lowest bits.
                m_reverseMoveAndAbsEloDiff = 
                    (packedReverseMove << reverseMoveShift)
                    | absEloDiff;
            }

            SmearedEntry(const SmearedEntry&) = default;
            SmearedEntry(SmearedEntry&&) = default;
            SmearedEntry& operator=(const SmearedEntry&) = default;
            SmearedEntry& operator=(SmearedEntry&&) = default;

            [[nodiscard]] GameLevel level() const
            {
                return fromOrdinal<GameLevel>((m_hashLevelResultCountFlags & levelMask) >> levelShift);
            }

            [[nodiscard]] GameResult result() const
            {
                return fromOrdinal<GameResult>((m_hashLevelResultCountFlags & resultMask) >> resultShift);
            }

            [[nodiscard]] std::uint32_t absEloDiff() const
            {
                return m_reverseMoveAndAbsEloDiff & absEloDiffMask;
            }

            [[nodiscard]] std::array<std::uint64_t, 2> hash() const
            {
                return std::array<std::uint64_t, 2>{
                    (static_cast<std::uint64_t>(m_hash0) << 32) | m_hash0,
                        static_cast<std::uint64_t>(additionalHash())
                };
            }

            [[nodiscard]] SmearedEntry key() const
            {
                return *this;
            }

            [[nodiscard]] std::uint32_t countMinusOne() const
            {
                return (m_hashLevelResultCountFlags & countMask) >> countShift;
            }

            [[nodiscard]] ReverseMove reverseMove(const Position& pos) const
            {
                const std::uint32_t packedInt = (m_reverseMoveAndAbsEloDiff & reverseMoveMask) >> reverseMoveShift;
                return detail::unpackReverseMove(pos, packedInt);
            }

            [[nodiscard]] bool isFirst() const
            {
                return m_hashLevelResultCountFlags & isFirstMask;
            }

            [[nodiscard]] bool isEloNegative() const
            {
                return m_hashLevelResultCountFlags & eloDiffSignMask;
            }

            struct CompareLessWithReverseMove
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    if (lhs.m_hash0 < rhs.m_hash0) return true;
                    else if (lhs.m_hash0 > rhs.m_hash0) return false;

                    if (lhs.m_hash1 < rhs.m_hash1) return true;
                    else if (lhs.m_hash1 > rhs.m_hash1) return false;

                    const auto lhsAdditionalHash = lhs.additionalHash();
                    const auto rhsAdditionalHash = rhs.additionalHash();
                    if (lhsAdditionalHash < rhsAdditionalHash) return true;
                    else if (lhsAdditionalHash > rhsAdditionalHash) return false;

                    return ((lhs.m_reverseMoveAndAbsEloDiff & reverseMoveMask) < (rhs.m_reverseMoveAndAbsEloDiff & reverseMoveMask));
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

                    const auto lhsAdditionalHash = lhs.additionalHash();
                    const auto rhsAdditionalHash = rhs.additionalHash();
                    return lhsAdditionalHash < rhsAdditionalHash;
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

                    const auto lhsAdditionalHash = lhs.additionalHash();
                    const auto rhsAdditionalHash = rhs.additionalHash();
                    if (lhsAdditionalHash < rhsAdditionalHash) return true;
                    else if (lhsAdditionalHash > rhsAdditionalHash) return false;

                    const auto lhsRev = (lhs.m_reverseMoveAndAbsEloDiff & reverseMoveMask);
                    const auto rhsRev = (rhs.m_reverseMoveAndAbsEloDiff & reverseMoveMask);
                    if (lhsRev < rhsRev) return true;
                    else if (lhsRev > rhsRev) return false;

                    const auto lhsRest = lhs.m_hashLevelResultCountFlags & (levelMask | resultMask);
                    const auto rhsRest = rhs.m_hashLevelResultCountFlags & (levelMask | resultMask);
                    return lhsRest < rhsRest;
                }
            };

            struct CompareEqualWithReverseMove
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    return
                        lhs.m_hash0 == rhs.m_hash0
                        && lhs.m_hash1 == rhs.m_hash1
                        && lhs.additionalHash() == rhs.additionalHash()
                        && (lhs.m_reverseMoveAndAbsEloDiff & reverseMoveMask) == (rhs.m_reverseMoveAndAbsEloDiff & reverseMoveMask);
                }
            };

            struct CompareEqualWithoutReverseMove
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    return
                        lhs.m_hash0 == rhs.m_hash0
                        && lhs.m_hash1 == rhs.m_hash1
                        && lhs.additionalHash() == rhs.additionalHash();
                }
            };

            struct CompareEqualFull
            {
                [[nodiscard]] bool operator()(const SmearedEntry& lhs, const SmearedEntry& rhs) const noexcept
                {
                    return
                        lhs.m_hash0 == rhs.m_hash0
                        && lhs.m_hash1 == rhs.m_hash1
                        && lhs.additionalHash() == rhs.additionalHash()
                        && (lhs.m_reverseMoveAndAbsEloDiff & reverseMoveMask) == (rhs.m_reverseMoveAndAbsEloDiff & reverseMoveMask)
                        && (lhs.m_hashLevelResultCountFlags & (levelMask | resultMask)) == (rhs.m_hashLevelResultCountFlags & (levelMask | resultMask));
                }
            };

        private:
            std::uint32_t m_hash0;
            std::uint32_t m_hash1;
            std::uint32_t m_hashLevelResultCountFlags;
            std::uint32_t m_reverseMoveAndAbsEloDiff;

            [[nodiscard]] std::uint32_t additionalHash() const
            {
                return m_hashLevelResultCountFlags & lastHashPartMask;
            }

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
                m_hash0(zobrist.high >> 32),
                m_hash1(static_cast<std::uint32_t>(zobrist.high)),
                m_hashLevelResultCountFlags(
                    static_cast<std::uint32_t>(zobrist.low & lastHashPartMask)
                    | (isFirst << isFirstShift)
                    | (countPart << countShift)
                    | (ordinal(level) << levelShift)
                    | (ordinal(result) << resultShift)
                    | eloSign
                ),
                m_reverseMoveAndAbsEloDiff(
                    (packedReverseMove << reverseMoveShift)
                    | absEloDiffPart
                )
            {

            }

            SmearedEntry(
                const ZobristKey& zobrist,
                std::uint32_t packedReverseMove,
                GameLevel level,
                GameResult result
            ) :
                m_hash0(zobrist.high >> 32),
                m_hash1(static_cast<std::uint32_t>(zobrist.high)),
                m_hashLevelResultCountFlags(
                    static_cast<std::uint32_t>(zobrist.low& lastHashPartMask)
                    | (ordinal(level) << levelShift)
                    | (ordinal(result) << resultShift)
                ),
                m_reverseMoveAndAbsEloDiff(packedReverseMove << reverseMoveShift)
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
                        m_count & SmearedEntry::countBitsMask,
                        m_absEloDiff & SmearedEntry::absEloDiffBitsMask,
                        m_eloDiffSign,
                        m_isFirst
                        );
                }

                Iterator& operator++()
                {
                    m_count >>= SmearedEntry::countBits;
                    m_absEloDiff >>= SmearedEntry::absEloDiffBits;
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

                m_zobrist.high = (static_cast<std::uint64_t>(smeared.m_hash0) << 32) | smeared.m_hash1;
                m_zobrist.low = smeared.m_hashLevelResultCountFlags & SmearedEntry::lastHashPartMask;

                m_count = static_cast<std::uint64_t>(smeared.countMinusOne()) + 1;

                m_eloDiff = smeared.absEloDiff();
                if (smeared.isEloNegative())
                {
                    m_eloDiff = -m_eloDiff;
                }

                m_packedReverseMove = smeared.m_reverseMoveAndAbsEloDiff >> SmearedEntry::reverseMoveShift;

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
                // for adding at position 0 use constructor
                ASSERT(position != 0);

                m_count += static_cast<std::uint64_t>(smeared.countMinusOne()) << (position * SmearedEntry::countBits);
                const auto absEloDiffChange = static_cast<std::int64_t>(smeared.absEloDiff()) << (position * SmearedEntry::absEloDiffBits);
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
    }
}
