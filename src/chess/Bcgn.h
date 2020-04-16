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
        Version_0 = 0
    };

    enum struct BcgnCompressionLevel
    {
        Level_0 = 0,
        Level_1 = 1
    };

    enum struct BcgnAuxCompression
    {
        None = 0
    };

    struct BcgnOptions
    {
        BcgnVersion version = BcgnVersion::Version_0;
        BcgnCompressionLevel compressionLevel = BcgnCompressionLevel::Level_0;
        BcgnAuxCompression auxCompression = BcgnAuxCompression::None;
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
                m_additionalTags.emplace_back(std::move(name), std::move(value));
            }

            void setAdditionalTag(const std::string& name, const std::string& value)
            {
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

            void setEventPlayer(const std::string_view sv)
            {
                m_eventLength = (std::uint8_t)std::min(traits::maxStringLength, sv.size());
                std::memcpy(m_event, sv.data(), m_eventLength);
            }

            void setSitePlayer(const std::string_view sv)
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

                writeBigEndian(buffer, (std::uint16_t)totalLength);
                writeBigEndian(buffer, (std::uint16_t)headerLength);

                *buffer++ = m_numPlies << 6; // 8 highest (of 14) bits
                *buffer++ = (m_numPlies << 2) | mapResultToInt();

                writeBigEndian(buffer, m_date.year());
                *buffer++ = m_date.month();
                *buffer++ = m_date.day();

                writeBigEndian(buffer, m_whiteElo);
                writeBigEndian(buffer, m_blackElo);
                writeBigEndian(buffer, m_round);
                *buffer++ = m_eco.category();
                *buffer++ = m_eco.index();

                *buffer++ = gatherFlags();

                writeString(buffer, m_white, m_whiteLength);
                writeString(buffer, m_black, m_blackLength);
                writeString(buffer, m_event, m_eventLength);
                writeString(buffer, m_site, m_siteLength);

                for (auto&& [name, value] : m_additionalTags)
                {
                    writeString(buffer, name);
                    writeString(buffer, value);
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

            [[nodiscard]] std::uint8_t gatherFlags() const
            {
                return
                    (m_customStartPos.has_value() << 1)
                    | (!m_additionalTags.empty());
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

                for (auto&& [name, value] : m_additionalTags)
                {
                    length += 2; // for two length specifications
                    length += std::min(traits::maxStringLength, name.size());
                    length += std::min(traits::maxStringLength, value.size());
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
            BcgnOptions options, 
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
            m_game.reset();
        }

        void resetGame()
        {
            m_game.reset();
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

        void setEventPlayer(const std::string_view sv)
        {
            m_game->setEventPlayer(sv);
        }

        void setSitePlayer(const std::string_view sv)
        {
            m_game->setSitePlayer(sv);
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
        BcgnOptions m_options;
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

            std::memset(data, 0, traits::bcgnFileHeaderLength);

            *data++ = 'B';
            *data++ = 'C';
            *data++ = 'G';
            *data++ = 'N';
            *data++ = static_cast<unsigned char>(m_options.version);
            *data++ = static_cast<unsigned char>(m_options.compressionLevel);
            *data++ = static_cast<unsigned char>(m_options.auxCompression);

            m_numBytesUsedInFrontBuffer = traits::bcgnFileHeaderLength;
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
}
