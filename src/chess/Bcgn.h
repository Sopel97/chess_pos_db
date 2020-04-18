#pragma once

#include "Bitboard.h"
#include "Chess.h"
#include "Date.h"
#include "Eco.h"
#include "GameClassification.h"
#include "Position.h"

#include "enum/EnumArray.h"

#include "util/UnsignedCharBufferView.h"
#include "util/Buffer.h"

#include <array>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <vector>

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

    struct BcgnFileHeader
    {
        BcgnVersion version = BcgnVersion::Version_0;
        BcgnCompressionLevel compressionLevel = BcgnCompressionLevel::Level_0;
        BcgnAuxCompression auxCompression = BcgnAuxCompression::None;

        void readFrom(const unsigned char* str);

        [[nodiscard]] std::size_t writeTo(unsigned char* data);

    private:
        [[noreturn]] void invalidHeader() const;
    };

    struct BcgnFlags
    {
        BcgnFlags();

        [[nodiscard]] static BcgnFlags decode(std::uint8_t v);

        void clear();

        void setHasCustomStartPos(bool v);

        void setHasAdditionalTags(bool v);

        [[nodiscard]] bool hasCustomStartPos() const;

        [[nodiscard]] bool hasAdditionalTags() const;

        [[nodiscard]] std::uint8_t encode() const;

    private:
        bool m_hasCustomStartPos;
        bool m_hasAdditionalTags;

        BcgnFlags(bool hasCustomStartPos, bool hasAdditionalTags);
    };

    namespace detail
    {
        struct BcgnGameEntryBuffer
        {
            BcgnGameEntryBuffer();

            void clear();

            void setDate(const Date& date);

            void setWhiteElo(std::uint16_t elo);

            void setBlackElo(std::uint16_t elo);

            void setRound(std::uint16_t round);

            void setEco(Eco eco);

            void setCustomStartPos(const Position& pos);

            void resetCustomStartPos();

            void setResult(GameResult result);

            void resetResult();

            void setAdditionalTag(std::string&& name, std::string&& value);

            void setAdditionalTag(const std::string& name, const std::string& value);

            void setWhitePlayer(const std::string_view sv);

            void setBlackPlayer(const std::string_view sv);

            void setEvent(const std::string_view sv);

            void setSite(const std::string_view sv);

            void addShortMove(unsigned char move);

            void addLongMove(std::uint16_t move);

            void addCompressedMove(const CompressedMove& move);

            // returns number of bytes written
            [[nodiscard]] std::size_t writeTo(unsigned char* buffer);

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

            void writeMovetext(unsigned char*& buffer);

            void writeString(unsigned char*& buffer, const std::string& str) const;

            void writeString(unsigned char*& buffer, const char* str, std::uint8_t length) const;

            [[nodiscard]] unsigned mapResultToInt() const;

            FORCEINLINE void writeBigEndian(unsigned char*& buffer, std::uint16_t value);

            [[nodiscard]] std::size_t computeHeaderLength() const;
        };
    }

    struct BcgnFileWriter
    {
        enum struct FileOpenMode
        {
            Truncate,
            Append
        };

        BcgnFileWriter(
            const std::filesystem::path& path,
            BcgnFileHeader header,
            FileOpenMode mode = FileOpenMode::Truncate,
            std::size_t bufferSize = traits::minBufferSize
            );

        void beginGame();

        void resetGame();

        void setDate(const Date& date);

        void setWhiteElo(std::uint16_t elo);

        void setBlackElo(std::uint16_t elo);

        void setRound(std::uint16_t round);

        void setEco(Eco eco);

        void setCustomStartPos(const Position& pos);

        void resetCustomStartPos();

        void setResult(GameResult result);

        void resetResult();

        void setAdditionalTag(std::string&& name, std::string&& value);

        void setAdditionalTag(const std::string& name, const std::string& value);

        void setWhitePlayer(const std::string_view sv);

        void setBlackPlayer(const std::string_view sv);

        void setEvent(const std::string_view sv);

        void setSite(const std::string_view sv);

        void addMove(const Position& pos, const Move& move);

        void endGame();

        void flush();

        ~BcgnFileWriter();

    private:
        BcgnFileHeader m_header;
        std::unique_ptr<detail::BcgnGameEntryBuffer> m_game;
        std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
        std::filesystem::path m_path;
        util::DoubleBuffer<unsigned char> m_buffer;
        std::size_t m_numBytesUsedInFrontBuffer;
        std::size_t m_numBytesBeingWritten;
        std::future<std::size_t> m_future;

        void writeHeader();

        void writeCurrentGame();

        [[nodiscard]] bool enoughSpaceForNextGame() const;

        void swapAndPersistFrontBuffer();
    };

    struct UnparsedBcgnGameMoves
    {
        UnparsedBcgnGameMoves(
            BcgnFileHeader header, 
            util::UnsignedCharBufferView movetext
            ) noexcept;

        [[nodiscard]] bool hasNext() const;

        [[nodiscard]] Move next(const Position& pos);

    private:
        BcgnFileHeader m_header;
        util::UnsignedCharBufferView m_encodedMovetext;
    };

    struct UnparsedBcgnGamePositions
    {
        struct iterator
        {
            struct sentinel {};

            using value_type = Position;
            using difference_type = std::ptrdiff_t;
            using reference = const Position&;
            using iterator_category = std::input_iterator_tag;
            using pointer = const Position*;

            iterator(
                BcgnFileHeader header, 
                util::UnsignedCharBufferView movetext
                ) noexcept;

            iterator(
                BcgnFileHeader header, 
                const Position& pos, 
                util::UnsignedCharBufferView movetext
                ) noexcept;

            const iterator& operator++();

            bool friend operator==(const iterator& lhs, sentinel rhs) noexcept;

            bool friend operator!=(const iterator& lhs, sentinel rhs) noexcept;

            [[nodiscard]] const Position& operator*() const;

            [[nodiscard]] const Position* operator->() const;

        private:
            Position m_position;
            UnparsedBcgnGameMoves m_moveProvider;
            bool m_isEnd;
        };

        using const_iterator = iterator;

        UnparsedBcgnGamePositions(
            BcgnFileHeader header, 
            util::UnsignedCharBufferView movetext
            ) noexcept;

        UnparsedBcgnGamePositions(
            BcgnFileHeader header,
            const Position& startpos, 
            util::UnsignedCharBufferView movetext
            ) noexcept;

        [[nodiscard]] iterator begin();

        [[nodiscard]] iterator::sentinel end() const;

    private:
        BcgnFileHeader m_header;
        Position m_startpos;
        util::UnsignedCharBufferView m_encodedMovetext;
    };

    // m_data starts at the count
    // if no tags then empty
    struct UnparsedBcgnAdditionalTags
    {
        struct iterator
        {
            struct sentinel {};

            using value_type = std::pair<std::string_view, std::string_view>;
            using difference_type = std::ptrdiff_t;
            using reference = const value_type&;
            using iterator_category = std::input_iterator_tag;
            using pointer = const value_type*;

            iterator(const char* data);

            const iterator& operator++();

            bool friend operator==(const iterator& lhs, sentinel rhs) noexcept;

            bool friend operator!=(const iterator& lhs, sentinel rhs) noexcept;

            [[nodiscard]] reference operator*() const;

            [[nodiscard]] pointer operator->() const;

        private:
            const char* m_data;
            std::size_t m_countLeft;
            std::pair<std::string_view, std::string_view> m_kv;
        };

        using const_iterator = iterator;

        UnparsedBcgnAdditionalTags(const unsigned char* data);
        
        [[nodiscard]] iterator begin() const;

        [[nodiscard]] iterator::sentinel end() const;

    private:
        const char* m_data;
    };

    struct UnparsedBcgnGame
    {
        UnparsedBcgnGame() = default;

        void setFileHeader(BcgnFileHeader header);

        void setGameData(util::UnsignedCharBufferView sv);

        [[nodiscard]] std::uint16_t numPlies() const;

        [[nodiscard]] std::optional<GameResult> result() const;

        [[nodiscard]] const Date& date() const;

        [[nodiscard]] std::uint16_t whiteElo() const;

        [[nodiscard]] std::uint16_t blackElo() const;

        [[nodiscard]] std::uint16_t round() const;

        [[nodiscard]] Eco eco() const;

        [[nodiscard]] std::string_view whitePlayer() const;

        [[nodiscard]] std::string_view blackPlayer() const;

        [[nodiscard]] std::string_view event() const;

        [[nodiscard]] std::string_view site() const;

        [[nodiscard]] bool hasCustomStartPosition() const;

        [[nodiscard]] std::string_view getAdditionalTagValue(std::string_view namesv) const;

        [[nodiscard]] Position startPosition() const;

        [[nodiscard]] UnparsedBcgnGameMoves moves() const;

        [[nodiscard]] UnparsedBcgnGamePositions positions() const;

        [[nodiscard]] UnparsedBcgnAdditionalTags additionalTags() const;

    private:
        BcgnFileHeader m_header;
        util::UnsignedCharBufferView m_data;

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
        std::string_view m_whitePlayer;
        std::string_view m_blackPlayer;
        std::string_view m_event;
        std::string_view m_site;
        std::size_t m_additionalTagsOffset;

        [[nodiscard]] std::size_t getStringsOffset() const;

        [[nodiscard]] Position getCustomStartPos() const;

        void prereadData();

        [[nodiscard]] util::UnsignedCharBufferView encodedMovetext() const;

        [[nodiscard]] std::uint16_t readHeaderLength() const;

        [[nodiscard]] std::optional<GameResult> mapIntToResult(unsigned v) const;
    };

    struct BcgnFileReader
    {
        struct iterator
        {
            struct sentinel {};

            using value_type = UnparsedBcgnGame;
            using difference_type = std::ptrdiff_t;
            using reference = const UnparsedBcgnGame&;
            using iterator_category = std::input_iterator_tag;
            using pointer = const UnparsedBcgnGame*;

            iterator(const std::filesystem::path& path, std::size_t bufferSize);

            const iterator& operator++();

            bool friend operator==(const iterator& lhs, sentinel rhs) noexcept;

            bool friend operator!=(const iterator& lhs, sentinel rhs) noexcept;

            [[nodiscard]] const UnparsedBcgnGame& operator*() const;

            [[nodiscard]] const UnparsedBcgnGame* operator->() const;

        private:
            BcgnFileHeader m_header;
            std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
            std::filesystem::path m_path;
            util::DoubleBuffer<unsigned char> m_buffer;
            util::UnsignedCharBufferView m_bufferView;
            std::future<std::size_t> m_future;
            UnparsedBcgnGame m_game;
            bool m_isEnd;

            void refillBuffer();

            void readFileHeader();

            void prepareFirstGame();

            void prepareNextGame();

            [[nodiscard]] bool isEnd() const;

            [[nodiscard]] std::size_t readNextGameEntrySize() const;
        };

        using const_iterator = iterator;

        BcgnFileReader(
            const std::filesystem::path& path, 
            std::size_t bufferSize = traits::minBufferSize
            );

        [[nodiscard]] bool isOpen() const;

        [[nodiscard]] iterator begin();

        [[nodiscard]] iterator::sentinel end() const;

    private:
        std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
        std::filesystem::path m_path;
        std::size_t m_bufferSize;
    };
}
