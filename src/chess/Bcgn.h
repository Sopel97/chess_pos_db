#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <vector>

#include "Bitboard.h"
#include "Chess.h"
#include "MoveIndex.h"
#include "Pgn.h"
#include "Position.h"
#include "San.h"

#include "enum/EnumArray.h"

#include "external_storage/External.h"

namespace bcgn
{
    // BCGN is a binary format.
    //
    // Basic encoding spec:
    // - multibyte values are encoded in big-endian
    // - strings are encoded as:
    //     - length N  : 1 byte
    //     - text      : N bytes
    //     text is NOT null terminated, encoding is utf-8
    //
    //
    // File header spec:
    // - "BCGN"            : 4 bytes
    // - version           : 1 byte
    //     - 0
    // - compression_level : 1 byte
    //     - 0 = 2 bytes per move, use CompressedMove
    //     - 1 = 1-2 bytes per move (almost always 1), use move index
    // - aux_compression   : 1 byte
    //     - 0 = none
    //     - 1 = lz4, for compression used after, on the whole file
    //     - 2 = lz4_DC, for compression used after, on the whole file
    // - RESERVED          : 25 bytes
    // - TOTAL             : 32 bytes
    //
    //
    // After file header comes a list of game header and movetext pairs:
    // (
    //     BASE_GAME_ADDRESS[i]:
    //     - total_length                    : 2 bytes
    //       total length of this game header + movetext entry in bytes
    //       i.e. BASE_GAME_ADDRESS[i] + total_length == BASE_GAME_ADDRESS[i+1]
    //     - header_length                   : 2 bytes
    //       total length of the header in bytes
    //       i.e. BASE_GAME_ADDRESS[i] + total_header_length == BASE_MOVETEXT_ADDRESS[i]
    //     - combined ply count and result   : 2 bytes
    //         - ply_count                    : 14 highest bits
    //         - result                       : 2 lowest bits
    //         0 unknown, 1 white win, 2 black win, 3 draw
    //     - date                            : 4 bytes
    //         - year                         : 2 bytes
    //         - month                        : 1 byte
    //         - day                          : 1 byte
    //         Any part is allowed to have value 0, meaning unknown
    //     - white_elo                       : 2 bytes
    //     - black_elo                       : 2 bytes
    //     - round                           : 2 bytes
    //     - eco                             : 2 bytes
    //         - [ABCDE]                      : 1 byte
    //         - index                        : 1 byte
    //     - SUBTOTAL                        : 18 bytes
    //
    //     - flags                           : 1 byte
    //         Going from least significant bit to most significant:
    //         - has_custom_start_position   : 1 bit
    //         - has_additional_tags         : 1 bit
    //         - RESERVED                    : 6 bits
    //
    //     If flags[has_custom_start_position]:
    //         - start_position                  : 24 bytes
    //         Details can be found in CompressedPosition
    //
    //     - white_player                    : string 
    //     - black_player                    : string
    //     - event                           : string
    //     - site                            : string
    //
    //     If flags[has_additional_tags]:
    //         - num_additional_tags K       : 1 byte
    //         (
    //             - name                    : string
    //             - value                   : string
    //         )*K
    //
    //     BASE_MOVETEXT_ADDRESS[i]:
    //     Length in bytes can be calculated from total_length - header_length
    //     (
    //         - move                        : depends on compression level
    //     )*ply_count
    // )*M
    //
    //
    // Compression levels:
    // - 0:
    //     Each move takes exactly 2 bytes. Move is encoded as in CompressedMove
    //     and written in big endian.
    //
    // - 1:
    //     We use move_index. Each move takes 1 or 2 bytes.
    //     If long move index is required then we compute a 2 byte move index
    //     and write it in big endian (note: whether long index is required is based
    //     on the position and not on the move index value; so it may happen
    //     that the high byte will be 0 but we still have to write it.)
    //     If long move indes is NOT required then we compute the 1 byte
    //     short move index and write it.
    //

    namespace traits
    {
        constexpr std::size_t maxGameLength = 256 * 256 - 1;
        constexpr std::size_t maxStringLength = 255;
        constexpr std::size_t minBufferSize = 128ull * 1024ull;
        constexpr std::size_t minHeaderLength = 23;
        constexpr std::size_t bcgnFileHeaderLength = 32;

        // Because we always ensure the buffer can take another game
        // even if it would be the longest possible we don't want
        // to flush at every game being written. It would happen any time a
        // game is written when the buffer size was too small because it would
        // be easly pushed past maxGameLength of free space.
        static_assert(minBufferSize > 2 * maxGameLength);


    }

    enum struct BcgnVersion
    {
        Version_0 = 0,
        SIZE
    };

    enum struct BcgnCompressionLevel
    {
        Level_0 = 0,
        Level_1 = 1,
        SIZE
    };

    enum struct BcgnAuxCompression
    {
        None = 0,
        SIZE
    };

    struct BcgnHeader
    {
        BcgnVersion version = BcgnVersion::Version_0;
        BcgnCompressionLevel compressionLevel = BcgnCompressionLevel::Level_0;
        BcgnAuxCompression auxCompression = BcgnAuxCompression::None;

        void readFrom(const char* str)
        {
            if (str[0] != 'B'
                || str[1] != 'C'
                || str[2] != 'G'
                || str[3] != 'N')
            {
                invalidHeader();
            }

            const std::uint8_t version_ = str[4];
            const std::uint8_t compressionLevel_ = str[5];
            const std::uint8_t auxCompression_ = str[6];

            if (version_ >= static_cast<unsigned>(BcgnVersion::SIZE)
                || compressionLevel_ >= static_cast<unsigned>(BcgnCompressionLevel::SIZE)
                || auxCompression_ >= static_cast<unsigned>(BcgnAuxCompression::SIZE))
            {
                invalidHeader();
            }

            for (int i = 7; i < traits::bcgnFileHeaderLength; ++i)
            {
                if (str[i] != '\0')
                {
                    invalidHeader();
                }
            }

            version = static_cast<BcgnVersion>(version_);
            compressionLevel = static_cast<BcgnCompressionLevel>(compressionLevel_);
            auxCompression = static_cast<BcgnAuxCompression>(auxCompression_);
        }

        [[nodiscard]] std::size_t writeTo(unsigned char* data)
        {
            std::memset(data, 0, traits::bcgnFileHeaderLength);

            *data++ = 'B';
            *data++ = 'C';
            *data++ = 'G';
            *data++ = 'N';
            *data++ = static_cast<unsigned char>(version);
            *data++ = static_cast<unsigned char>(compressionLevel);
            *data++ = static_cast<unsigned char>(auxCompression);

            return traits::bcgnFileHeaderLength;
        }

    private:
        [[noreturn]] void invalidHeader() const
        {
            throw std::runtime_error("Invalid header.");
        }
    };

    struct BcgnFlags
    {
        BcgnFlags() :
            m_hasCustomStartPos(false),
            m_hasAdditionalTags(false)
        {
        }

        [[nodiscard]] static BcgnFlags decode(std::uint8_t v)
        {
            return BcgnFlags(
                (v >> 1) & 1,
                v & 1
                );
        }

        void clear()
        {
            m_hasCustomStartPos = false;
            m_hasAdditionalTags = false;
        }

        void setHasCustomStartPos(bool v)
        {
            m_hasCustomStartPos = v;
        }

        void setHasAdditionalTags(bool v)
        {
            m_hasAdditionalTags = v;
        }

        [[nodiscard]] bool hasCustomStartPos() const
        {
            return m_hasCustomStartPos;
        }

        [[nodiscard]] bool hasAdditionalTags() const
        {
            return m_hasAdditionalTags;
        }

        [[nodiscard]] std::uint8_t encode() const
        {
            return
                ((std::uint8_t)m_hasCustomStartPos << 1)
                | (std::uint8_t)m_hasAdditionalTags;
        }

    private:
        bool m_hasCustomStartPos;
        bool m_hasAdditionalTags;

        BcgnFlags(bool hasCustomStartPos, bool hasAdditionalTags) :
            m_hasCustomStartPos(hasCustomStartPos),
            m_hasAdditionalTags(hasAdditionalTags)
        {
        }
    };

    namespace detail
    {
        struct BcgnGameEntryBuffer
        {
            BcgnGameEntryBuffer() :
                m_date{},
                m_whiteElo{},
                m_blackElo{},
                m_round{},
                m_eco{},
                m_customStartPos{},
                m_result{},
                m_additionalTags{},
                m_whiteLength{},
                m_white{},
                m_blackLength{},
                m_black{},
                m_eventLength{},
                m_event{},
                m_siteLength{},
                m_site{},
                m_numPlies{},
                m_flags{},
                m_movetext{}
            {
                m_additionalTags.reserve(8);
                m_movetext.reserve(512);
            }

            void clear()
            {
                m_date = {};
                m_whiteElo = 0;
                m_blackElo = 0;
                m_round = 0;
                m_eco = {};
                m_customStartPos = {};
                m_result = {};
                m_additionalTags.clear();
                m_whiteLength = 0;
                m_blackLength = 0;
                m_eventLength = 0;
                m_siteLength = 0;
                m_numPlies = 0;
                m_flags.clear();
                m_movetext.clear();
            }

            void setDate(const Date& date)
            {
                m_date = date;
            }

            void setWhiteElo(std::uint16_t elo)
            {
                m_whiteElo = elo;
            }

            void setBlackElo(std::uint16_t elo)
            {
                m_blackElo = elo;
            }

            void setRound(std::uint16_t round)
            {
                m_round = round;
            }

            void setEco(Eco eco)
            {
                m_eco = eco;
            }

            void setCustomStartPos(const Position& pos)
            {
                m_customStartPos = pos.compress();
            }

            void resetCustomStartPos()
            {
                m_customStartPos = {};
            }

            void setResult(GameResult result)
            {
                m_result = result;
            }

            void resetResult()
            {
                m_result = {};
            }

            void setAdditionalTag(std::string&& name, std::string&& value)
            {
                if (m_additionalTags.size() >= 255) return;
                m_additionalTags.emplace_back(std::move(name), std::move(value));
            }

            void setAdditionalTag(const std::string& name, const std::string& value)
            {
                if (m_additionalTags.size() >= 255) return;
                m_additionalTags.emplace_back(name, value);
            }

            void setWhitePlayer(const std::string_view sv)
            {
                m_whiteLength = (std::uint8_t)std::min(traits::maxStringLength, sv.size());
                std::memcpy(m_white, sv.data(), m_whiteLength);
            }

            void setBlackPlayer(const std::string_view sv)
            {
                m_blackLength = (std::uint8_t)std::min(traits::maxStringLength, sv.size());
                std::memcpy(m_black, sv.data(), m_blackLength);
            }

            void setEvent(const std::string_view sv)
            {
                m_eventLength = (std::uint8_t)std::min(traits::maxStringLength, sv.size());
                std::memcpy(m_event, sv.data(), m_eventLength);
            }

            void setSite(const std::string_view sv)
            {
                m_siteLength = (std::uint8_t)std::min(traits::maxStringLength, sv.size());
                std::memcpy(m_site, sv.data(), m_siteLength);
            }

            void addShortMove(unsigned char move)
            {
                ++m_numPlies;
                m_movetext.push_back(move);
            }

            void addLongMove(std::uint16_t move)
            {
                ++m_numPlies;
                m_movetext.push_back(move >> 8);
                m_movetext.push_back(move & 0xFF);
            }

            void addCompressedMove(const CompressedMove& move)
            {
                unsigned char c[2];
                move.writeToBigEndian(c);
                m_movetext.push_back(c[0]);
                m_movetext.push_back(c[1]);
                ++m_numPlies;
            }

            // returns number of bytes written
            [[nodiscard]] std::size_t writeTo(unsigned char* buffer)
            {
                const std::size_t headerLength = computeHeaderLength();
                const std::size_t movetextLength = m_movetext.size();
                const std::size_t totalLength = headerLength + movetextLength;
                if (totalLength >= std::numeric_limits<std::uint16_t>::max())
                {
                    throw std::runtime_error("Game text must not be longer than 65535 bytes.");
                }

                m_flags.setHasAdditionalTags(!m_additionalTags.empty());
                m_flags.setHasCustomStartPos(m_customStartPos.has_value());

                writeBigEndian(buffer, (std::uint16_t)totalLength);
                writeBigEndian(buffer, (std::uint16_t)headerLength);

                *buffer++ = m_numPlies >> 6; // 8 highest (of 14) bits
                *buffer++ = (m_numPlies << 2) | mapResultToInt();

                writeBigEndian(buffer, m_date.year());
                *buffer++ = m_date.month();
                *buffer++ = m_date.day();

                writeBigEndian(buffer, m_whiteElo);
                writeBigEndian(buffer, m_blackElo);
                writeBigEndian(buffer, m_round);
                *buffer++ = m_eco.category();
                *buffer++ = m_eco.index();

                *buffer++ = m_flags.encode();

                if (m_customStartPos.has_value())
                {
                    m_customStartPos->writeToBigEndian(buffer);
                    buffer += sizeof(CompressedPosition);
                }

                writeString(buffer, m_white, m_whiteLength);
                writeString(buffer, m_black, m_blackLength);
                writeString(buffer, m_event, m_eventLength);
                writeString(buffer, m_site, m_siteLength);

                if (!m_additionalTags.empty())
                {
                    *buffer++ = (std::uint8_t)m_additionalTags.size();
                    for (auto&& [name, value] : m_additionalTags)
                    {
                        writeString(buffer, name);
                        writeString(buffer, value);
                    }
                }

                writeMovetext(buffer);

                return totalLength;
            }

        private:
            Date m_date;
            std::uint16_t m_whiteElo;
            std::uint16_t m_blackElo;
            std::uint16_t m_round;
            Eco m_eco;
            std::optional<CompressedPosition> m_customStartPos;
            std::optional<GameResult> m_result;
            std::vector<std::pair<std::string, std::string>> m_additionalTags;
            std::uint8_t m_whiteLength;
            char m_white[256];
            std::uint8_t m_blackLength;
            char m_black[256];
            std::uint8_t m_eventLength;
            char m_event[256];
            std::uint8_t m_siteLength;
            char m_site[256];
            std::uint16_t m_numPlies;
            BcgnFlags m_flags;
            std::vector<unsigned char> m_movetext;

            void writeMovetext(unsigned char*& buffer)
            {
                const std::size_t length = m_movetext.size();;
                std::memcpy(buffer, m_movetext.data(), length);
                buffer += length;
            }

            void writeString(unsigned char*& buffer, const std::string& str) const
            {
                const std::size_t length = std::min(traits::maxStringLength, str.size());
                *buffer++ = (std::uint8_t)length;
                std::memcpy(buffer, str.c_str(), length);
                buffer += length;
            }

            void writeString(unsigned char*& buffer, const char* str, std::uint8_t length) const
            {
                *buffer++ = length;
                std::memcpy(buffer, str, length);
                buffer += length;
            }

            [[nodiscard]] unsigned mapResultToInt() const
            {
                if (!m_result.has_value())
                {
                    return 0;
                }
                else
                {
                    switch (m_result.value())
                    {
                    case GameResult::WhiteWin:
                        return 1;
                    case GameResult::BlackWin:
                        return 2;
                    case GameResult::Draw:
                        return 3;
                    }
                }

                ASSERT(false);
                return 0;
            }

            FORCEINLINE void writeBigEndian(unsigned char*& buffer, std::uint16_t value)
            {
                *buffer++ = value >> 8;
                *buffer++ = value & 0xFF;
            }

            [[nodiscard]] std::size_t computeHeaderLength() const
            {
                constexpr std::size_t lengthOfMandatoryFixedLengthFields =
                    2 + 2 + // lengths
                    2 + // ply + result
                    4 + // date
                    2 + 2 + 2 + 2 + // white elo, black elo, round, eco
                    1 + // flags
                    4; // lengths of 4 mandatory strings

                std::size_t length = lengthOfMandatoryFixedLengthFields;

                if (m_customStartPos.has_value())
                {
                    static_assert(sizeof(CompressedPosition) == 24);
                    length += sizeof(CompressedPosition);
                }

                length += m_whiteLength;
                length += m_blackLength;
                length += m_eventLength;
                length += m_siteLength;

                if (!m_additionalTags.empty())
                {
                    length += 1;
                    for (auto&& [name, value] : m_additionalTags)
                    {
                        length += 2; // for two length specifications
                        length += std::min(traits::maxStringLength, name.size());
                        length += std::min(traits::maxStringLength, value.size());
                    }
                }

                return length;
            }
        };
    }

    struct BcgnWriter
    {
        enum struct FileOpenMode
        {
            Truncate,
            Append
        };

        BcgnWriter(
            const std::filesystem::path& path, 
            BcgnHeader options, 
            FileOpenMode mode = FileOpenMode::Truncate, 
            std::size_t bufferSize = traits::minBufferSize
            ) :
            m_options(options),
            m_game(std::make_unique<detail::BcgnGameEntryBuffer>()),
            m_file(nullptr, &std::fclose),
            m_path(path),
            m_buffer(std::max(bufferSize, traits::minBufferSize)),
            m_numBytesUsedInFrontBuffer(0),
            m_numBytesBeingWritten(0),
            m_future{}
        {
            const bool needsHeader = (mode != FileOpenMode::Append) || !std::filesystem::exists(path);

            auto strPath = path.string();
            m_file.reset(std::fopen(
                strPath.c_str(),
                mode == FileOpenMode::Append ? "ab" : "wb"
                ));

            writeHeader();
        }

        void beginGame()
        {
            m_game->clear();
        }

        void resetGame()
        {
            m_game->clear();
        }

        void setDate(const Date& date)
        {
            m_game->setDate(date);
        }

        void setWhiteElo(std::uint16_t elo)
        {
            m_game->setWhiteElo(elo);
        }

        void setBlackElo(std::uint16_t elo)
        {
            m_game->setBlackElo(elo);
        }

        void setRound(std::uint16_t round)
        {
            m_game->setRound(round);
        }

        void setEco(Eco eco)
        {
            m_game->setEco(eco);
        }

        void setCustomStartPos(const Position& pos)
        {
            m_game->setCustomStartPos(pos);
        }

        void resetCustomStartPos()
        {
            m_game->resetCustomStartPos();
        }

        void setResult(GameResult result)
        {
            m_game->setResult(result);
        }

        void resetResult()
        {
            m_game->resetResult();
        }

        void setAdditionalTag(std::string&& name, std::string&& value)
        {
            m_game->setAdditionalTag(std::move(name), std::move(value));
        }

        void setAdditionalTag(const std::string& name, const std::string& value)
        {
            m_game->setAdditionalTag(name, value);
        }

        void setWhitePlayer(const std::string_view sv)
        {
            m_game->setWhitePlayer(sv);
        }

        void setBlackPlayer(const std::string_view sv)
        {
            m_game->setBlackPlayer(sv);
        }

        void setEvent(const std::string_view sv)
        {
            m_game->setEvent(sv);
        }

        void setSite(const std::string_view sv)
        {
            m_game->setSite(sv);
        }

        void addMove(const Position& pos, const Move& move)
        {
            switch (m_options.compressionLevel)
            {
            case BcgnCompressionLevel::Level_0:
                m_game->addCompressedMove(move.compress());
                break;

            case BcgnCompressionLevel::Level_1:
                if (move_index::requiresLongMoveIndex(pos))
                {
                    m_game->addLongMove(move_index::moveToLongIndex(pos, move));
                }
                else
                {
                    m_game->addShortMove(move_index::moveToShortIndex(pos, move));
                }

                break;
            }
        }

        void endGame()
        {
            writeCurrentGame();

            // We don't know how much the next game will take
            // and we don't want to compute the size before writing.
            // So we ensure that we always have enough space in the buffer.
            // The buffer is not big anyway so this shouldn't be an issue.
            if (!enoughSpaceForNextGame())
            {
                swapAndPersistFrontBuffer();
            }
        }

        void flush()
        {
            swapAndPersistFrontBuffer();

            if (m_future.valid())
            {
                m_future.get();
            }
        }

        ~BcgnWriter()
        {
            flush();
        }

    private:
        BcgnHeader m_options;
        std::unique_ptr<detail::BcgnGameEntryBuffer> m_game;
        std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
        std::filesystem::path m_path;
        ext::DoubleBuffer<unsigned char> m_buffer;
        std::size_t m_numBytesUsedInFrontBuffer;
        std::size_t m_numBytesBeingWritten;
        std::future<std::size_t> m_future;

        void writeHeader()
        {
            unsigned char* data = m_buffer.data();
            m_numBytesUsedInFrontBuffer += m_options.writeTo(data);
        }

        void writeCurrentGame()
        {
            const auto bytesWritten = m_game->writeTo(m_buffer.data() + m_numBytesUsedInFrontBuffer);
            m_numBytesUsedInFrontBuffer += bytesWritten;
        }

        [[nodiscard]] bool enoughSpaceForNextGame() const
        {
            return m_buffer.size() - m_numBytesUsedInFrontBuffer >= traits::maxGameLength;
        }

        void swapAndPersistFrontBuffer()
        {
            if (!m_numBytesUsedInFrontBuffer)
            {
                return;
            }

            if (m_future.valid())
            {
                m_future.get();
            }

            m_buffer.swap();
            m_numBytesBeingWritten = m_numBytesUsedInFrontBuffer;
            m_numBytesUsedInFrontBuffer = 0;

            m_future = std::async(std::launch::async, [this]() {
                return std::fwrite(m_buffer.back_data(), 1, m_numBytesBeingWritten, m_file.get());
                });
        }
    };

    struct UnparsedBcgnGameMoves
    {
        UnparsedBcgnGameMoves(BcgnHeader options, std::string_view movetext) noexcept :
            m_options(options),
            m_encodedMovetext(movetext)
        {
        }

        [[nodiscard]] bool hasNext() const
        {
            return !m_encodedMovetext.empty();
        }

        [[nodiscard]] Move next(const Position& pos)
        {
            switch (m_options.compressionLevel)
            {
            case BcgnCompressionLevel::Level_0:
            {
                const CompressedMove compressedMove = CompressedMove::readFromBigEndian(m_encodedMovetext.data());
                m_encodedMovetext.remove_prefix(2);
                return compressedMove.decompress();
            }

            case BcgnCompressionLevel::Level_1:
            {
                if (move_index::requiresLongMoveIndex(pos))
                {
                    const std::uint16_t index = ((at(0)) << 8) | at(1);
                    const Move move = move_index::longIndexToMove(pos, index);
                    m_encodedMovetext.remove_prefix(2);
                    return move;
                }
                else
                {
                    const std::uint8_t index = at(0);
                    const Move move = move_index::shortIndexToMove(pos, index);
                    m_encodedMovetext.remove_prefix(1);
                    return move;
                }
            }
            }

            ASSERT(false);
            return Move::null();
        }

    private:
        BcgnHeader m_options;
        std::string_view m_encodedMovetext;

        [[nodiscard]] FORCEINLINE std::uint8_t at(std::size_t i) const
        {
            return m_encodedMovetext[i];
        }
    };

    struct UnparsedBcgnGamePositions
    {
        struct UnparsedBcgnGamePositionsIterator
        {
            struct Sentinel {};

            using value_type = Position;
            using difference_type = std::ptrdiff_t;
            using reference = const Position&;
            using iterator_category = std::input_iterator_tag;
            using pointer = const Position*;

            UnparsedBcgnGamePositionsIterator(BcgnHeader options, std::string_view movetext) noexcept :
                m_position(Position::startPosition()),
                m_moveProvider(options, movetext)
            {
            }

            UnparsedBcgnGamePositionsIterator(BcgnHeader options, const Position& pos, std::string_view movetext) noexcept :
                m_position(pos),
                m_moveProvider(options, movetext)
            {
            }

            const UnparsedBcgnGamePositionsIterator& operator++()
            {
                const auto move = m_moveProvider.next(m_position);
                m_position.doMove(move);
                return *this;
            }

            bool friend operator==(const UnparsedBcgnGamePositionsIterator& lhs, Sentinel rhs) noexcept
            {
                return !lhs.m_moveProvider.hasNext();
            }

            bool friend operator!=(const UnparsedBcgnGamePositionsIterator& lhs, Sentinel rhs) noexcept
            {
                return lhs.m_moveProvider.hasNext();
            }

            [[nodiscard]] const Position& operator*() const
            {
                return m_position;
            }

            [[nodiscard]] const Position* operator->() const
            {
                return &m_position;
            }

        private:
            Position m_position;
            UnparsedBcgnGameMoves m_moveProvider;
        };

        using iterator = UnparsedBcgnGamePositionsIterator;
        using const_iterator = UnparsedBcgnGamePositionsIterator;

        UnparsedBcgnGamePositions(BcgnHeader options, std::string_view movetext) noexcept :
            m_options(options),
            m_startpos(Position::startPosition()),
            m_encodedMovetext(movetext)
        {

        }

        UnparsedBcgnGamePositions(BcgnHeader options, const Position& startpos, std::string_view movetext) noexcept :
            m_options(options),
            m_startpos(startpos),
            m_encodedMovetext(movetext)
        {

        }

        [[nodiscard]] UnparsedBcgnGamePositionsIterator begin()
        {
            return UnparsedBcgnGamePositionsIterator(m_options, m_startpos, m_encodedMovetext);
        }

        [[nodiscard]] UnparsedBcgnGamePositionsIterator::Sentinel end() const
        {
            return {};
        }

    private:
        BcgnHeader m_options;
        Position m_startpos;
        std::string_view m_encodedMovetext;
    };

    struct UnparsedBcgnGame
    {
        UnparsedBcgnGame() = default;

        void setOptions(BcgnHeader header)
        {
            m_options = header;
        }

        void setGameData(std::string_view sv)
        {
            m_data = sv;
            prereadData();
        }

        [[nodiscard]] std::uint16_t numPlies() const
        {
            return m_numPlies;
        }

        [[nodiscard]] std::optional<GameResult> result() const
        {
            return m_result;
        }

        [[nodiscard]] const Date& date() const
        {
            return m_date;
        }

        [[nodiscard]] std::uint16_t whiteElo() const
        {
            return m_whiteElo;
        }

        [[nodiscard]] std::uint16_t blackElo() const
        {
            return m_blackElo;
        }

        [[nodiscard]] std::uint16_t round() const
        {
            return m_round;
        }

        [[nodiscard]] Eco eco() const
        {
            return m_eco;
        }

        [[nodiscard]] std::string_view whitePlayer() const
        {
            return getMandatoryString<0>();
        }

        [[nodiscard]] std::string_view blackPlayer() const
        {
            return getMandatoryString<1>();
        }

        [[nodiscard]] std::string_view event() const
        {
            return getMandatoryString<2>();
        }

        [[nodiscard]] std::string_view site() const
        {
            return getMandatoryString<3>();
        }

        [[nodiscard]] bool hasCustomStartPosition() const
        {
            return m_flags.hasCustomStartPos();
        }

        [[nodiscard]] std::string_view getAdditionalTagValue(std::string_view name) const
        {
            if (!m_flags.hasAdditionalTags())
            {
                return {};
            }
            
            std::size_t offset = getAdditionalTagSectionOffset();
            const std::uint8_t numAdditionalTags = at(offset);
            offset += 1;
            for (int i = 0; i < numAdditionalTags; ++i)
            {
                const std::uint8_t nameLength = at(offset);
                const auto currentName = m_data.substr(offset + 1, nameLength);
                const auto valueLength = at(offset + 1 + nameLength);
                
                if (currentName == name)
                {
                    return m_data.substr(offset + 1 + nameLength + 1, valueLength);
                }

                offset += 2 + nameLength + valueLength;
            }

            return {};
        }

        [[nodiscard]] Position startPosition() const
        {
            if (m_flags.hasCustomStartPos())
            {
                return getCustomStartPos();
            }
            else
            {
                return Position::startPosition();
            }
        }

        [[nodiscard]] UnparsedBcgnGameMoves moves() const
        {
            return UnparsedBcgnGameMoves(m_options, encodedMovetext());
        }

        [[nodiscard]] UnparsedBcgnGamePositions positions() const
        {
            return UnparsedBcgnGamePositions(m_options, encodedMovetext());
        }

    private:
        BcgnHeader m_options;
        std::string_view m_data;

        // We read and store the mandatory data that's cheap to decode.
        // Everything else is lazy.
        std::uint16_t m_headerLength;
        std::uint16_t m_numPlies;
        std::optional<GameResult> m_result;
        Date m_date;
        std::uint16_t m_whiteElo;
        std::uint16_t m_blackElo;
        std::uint16_t m_round;
        Eco m_eco;
        BcgnFlags m_flags;

        [[nodiscard]] FORCEINLINE std::uint8_t at(std::size_t i) const
        {
            return m_data[i];
        }

        [[nodiscard]] std::size_t getStringsOffset() const
        {
            return 19 + 24 * m_flags.hasCustomStartPos();
        }

        [[nodiscard]] Position getCustomStartPos() const
        {
            CompressedPosition pos = CompressedPosition::readFromBigEndian(m_data.data() + 19);
            return pos.decompress();
        }

        template <std::size_t IndexV>
        [[nodiscard]] std::string_view getMandatoryString() const
        {
            static_assert(IndexV < 4);

            std::size_t offset = getStringsOffset();
            if constexpr (IndexV > 0) offset += at(offset) + 1;
            if constexpr (IndexV > 1) offset += at(offset) + 1;
            if constexpr (IndexV > 2) offset += at(offset) + 1;

            const std::uint8_t length = at(offset);

            return m_data.substr(offset + 1, length);
        }

        [[nodiscard]] std::size_t getAdditionalTagSectionOffset() const
        {
            std::size_t offset = getStringsOffset();
            offset += at(offset) + 1;
            offset += at(offset) + 1;
            offset += at(offset) + 1;
            offset += at(offset) + 1;
            return offset;
        }

        void prereadData()
        {
            m_headerLength = readHeaderLength();
            // we convert to unsigned char to prevent sign extension.
            m_numPlies = (at(4) << 6) | (at(5) >> 2);
            m_result = mapIntToResult(at(5) & 3);
            m_date = Date((at(6) << 8) | at(7), at(8), at(9));

            m_whiteElo = (at(10) << 8) | at(11);
            m_blackElo = (at(12) << 8) | at(13);
            m_round = (at(14) << 8) | at(15);

            m_eco = Eco(at(16), at(17));

            m_flags = BcgnFlags::decode(at(18));
        }

        [[nodiscard]] std::string_view encodedMovetext() const
        {
            return m_data.substr(m_headerLength);
        }

        [[nodiscard]] std::uint16_t readHeaderLength() const
        {
            return (at(2) << 8) | at(3);
        }

        [[nodiscard]] std::optional<GameResult> mapIntToResult(unsigned v) const
        {
            switch (v)
            {
            case 0:
                return {};
            case 1:
                return GameResult::WhiteWin;
            case 2:
                return GameResult::BlackWin;
            case 3:
                return GameResult::Draw;
            }

            ASSERT(false);
            return {};
        }
    };

    struct BcgnReader
    {
        struct LazyBcgnReaderIterator
        {
            struct Sentinel {};

            using value_type = UnparsedBcgnGame;
            using difference_type = std::ptrdiff_t;
            using reference = const UnparsedBcgnGame&;
            using iterator_category = std::input_iterator_tag;
            using pointer = const UnparsedBcgnGame*;

            LazyBcgnReaderIterator(const std::filesystem::path& path, std::size_t bufferSize) :
                m_options{},
                m_file(nullptr, &std::fclose),
                m_path(path),
                m_buffer(bufferSize),
                m_bufferView{},
                m_numBytesLeftInAuxBuffer(0),
                m_future{},
                m_game{},
                m_isEnd(false)
            {
                auto strPath = path.string();
                m_file.reset(std::fopen(strPath.c_str(), "rb"));

                if (m_file == nullptr)
                {
                    m_isEnd = true;
                    return;
                }

                refillBuffer();

                if (!isEnd())
                {
                    fillOptions();

                    m_game.setOptions(m_options);

                    prepareFirstGame();
                }
            }

            const LazyBcgnReaderIterator& operator++()
            {
                prepareNextGame();
                return *this;
            }

            bool friend operator==(const LazyBcgnReaderIterator& lhs, Sentinel rhs) noexcept
            {
                return lhs.isEnd();
            }

            bool friend operator!=(const LazyBcgnReaderIterator& lhs, Sentinel rhs) noexcept 
            {
                return !lhs.isEnd();
            }

            [[nodiscard]] const UnparsedBcgnGame& operator*() const
            {
                return m_game;
            }

            [[nodiscard]] const UnparsedBcgnGame* operator->() const
            {
                return &m_game;
            }

        private:
            BcgnHeader m_options;
            std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
            std::filesystem::path m_path;
            ext::DoubleBuffer<char> m_buffer;
            std::string_view m_bufferView;
            std::size_t m_numBytesLeftInAuxBuffer;
            std::future<std::size_t> m_future;
            UnparsedBcgnGame m_game;
            bool m_isEnd;

            void refillBuffer()
            {
                // We know that the biggest possible unprocessed amount of bytes is traits::maxGameLength - 1.
                // Using this information we can only fill the buffer starting from 
                // position traits::maxGameLength and prepend any unprocessed data
                // in front of it.
                // This way we minimize copying between buffers.

                const std::size_t usableReadBufferSpace = m_buffer.size() - traits::maxGameLength;

                const std::size_t numUnprocessedBytes = m_bufferView.size();
                if (numUnprocessedBytes >= traits::maxGameLength)
                {
                    // This should never happen. There should always be a game in there.
                    throw std::runtime_error("Unprocessed block longer than maxGameLength.");
                }

                const std::size_t freeSpace = traits::maxGameLength - numUnprocessedBytes;
                if (numUnprocessedBytes)
                {
                    // memcpy is safe because the buffers are disjoint.
                    std::memcpy(m_buffer.back_data() + freeSpace, m_bufferView.data(), numUnprocessedBytes);
                }

                // If this is the first read then we read data to back_data,
                // swap the buffers, and schedule async read to new back_data.
                // If this is a subsequent read then we wait for write to back_data
                // to finish, swap the buffers, and scherule a read to the new back_data.

                const auto numBytesRead = 
                    m_future.valid()
                    ? m_future.get()
                    : std::fread(m_buffer.back_data() + traits::maxGameLength, 1, usableReadBufferSpace, m_file.get());

                if (numBytesRead == 0)
                {
                    m_isEnd = true;
                    return;
                }

                m_buffer.swap();

                m_future = std::async(std::launch::async, [this, usableReadBufferSpace]() {
                    return std::fread(m_buffer.back_data() + traits::maxGameLength, 1, usableReadBufferSpace, m_file.get());
                    });

                m_bufferView = std::string_view(m_buffer.data() + freeSpace, numBytesRead + numUnprocessedBytes);
            }

            void fillOptions()
            {
                if (m_bufferView.size() < traits::bcgnFileHeaderLength)
                {
                    m_isEnd = true;
                }
                else
                {
                    m_options.readFrom(m_bufferView.data());
                    m_bufferView.remove_prefix(traits::bcgnFileHeaderLength);
                }
            }

            void prepareFirstGame()
            {
                // If we fail here we can just set isEnd and don't bother.
                // The buffer should always be big enough to have at least one game.

                if (m_bufferView.size() < traits::minHeaderLength)
                {
                    m_isEnd = true;
                    return;
                }

                const auto size = readNextGameEntrySize();
                if (m_bufferView.size() < size)
                {
                    m_isEnd = true;
                    return;
                }

                m_game.setGameData(m_bufferView.substr(0, size));
                m_bufferView.remove_prefix(size);
            }

            void prepareNextGame()
            {
                while(!isEnd())
                {
                    if (m_bufferView.size() < 2)
                    {
                        // we cannot read the entry size, request more data
                        refillBuffer();
                        continue;
                    }

                    const auto size = readNextGameEntrySize();
                    if (m_bufferView.size() < size)
                    {
                        refillBuffer();
                        continue;
                    }

                    // Here we are guaranteed to have the whole game in the buffer.
                    m_game.setGameData(m_bufferView.substr(0, size));
                    m_bufferView.remove_prefix(size);
                    return;
                }
            }

            [[nodiscard]] bool isEnd() const
            {
                return m_isEnd;
            }

            [[nodiscard]] std::size_t readNextGameEntrySize() const
            {
                // We assume here that there are 2 bytes in the buffer.
                return ((std::uint8_t)m_bufferView[0] << 8) | (std::uint8_t)m_bufferView[1];
            }
        };

        using iterator = LazyBcgnReaderIterator;
        using const_iterator = LazyBcgnReaderIterator;

        BcgnReader(const std::filesystem::path& path, std::size_t bufferSize = traits::minBufferSize) :
            m_file(nullptr, &std::fclose),
            m_path(path),
            m_bufferSize(bufferSize)
        {
            auto strPath = path.string();
            m_file.reset(std::fopen(strPath.c_str(), "rb"));
        }

        [[nodiscard]] bool isOpen() const
        {
            return m_file != nullptr;
        }

        [[nodiscard]] LazyBcgnReaderIterator begin()
        {
            return LazyBcgnReaderIterator(m_path, m_bufferSize);
        }

        [[nodiscard]] LazyBcgnReaderIterator::Sentinel end() const
        {
            return {};
        }

    private:
        std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
        std::filesystem::path m_path;
        std::size_t m_bufferSize;
    };
}
