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
    // See docs for the specification.

    namespace traits
    {
        constexpr std::size_t maxGameLength = 256 * 256 - 1;
        constexpr std::size_t maxStringLength = 255;
        constexpr std::size_t minBufferSize = 128ull * 1024ull;
        constexpr std::size_t minHeaderLength = 5; // in headerless
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
        Level_2 = 2,
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
        bool isHeaderless = false;

        void readFrom(const unsigned char* str);

        [[nodiscard]] std::size_t writeTo(unsigned char* data);

    private:
        [[noreturn]] void invalidHeader() const;
    };

    struct BcgnGameFlags
    {
        BcgnGameFlags();

        [[nodiscard]] static BcgnGameFlags decode(std::uint8_t v);

        void clear();

        void setHasCustomStartPos(bool v);

        void setHasAdditionalTags(bool v);

        [[nodiscard]] bool hasCustomStartPos() const;

        [[nodiscard]] bool hasAdditionalTags() const;

        [[nodiscard]] std::uint8_t encode() const;

    private:
        bool m_hasCustomStartPos;
        bool m_hasAdditionalTags;

        BcgnGameFlags(bool hasCustomStartPos, bool hasAdditionalTags);
    };

    namespace detail
    {
        struct BcgnGameEntryBuffer
        {
            BcgnGameEntryBuffer(BcgnFileHeader header);

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

            void addBitsLE8x2(std::uint8_t bits0, std::size_t count0, std::uint8_t bits1, std::size_t count1);

            // returns number of bytes written
            [[nodiscard]] std::size_t writeTo(unsigned char* buffer);

        private:
            BcgnFileHeader m_header;
            std::size_t m_bitsLeft;
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
            BcgnGameFlags m_flags;
            std::vector<unsigned char> m_movetext;

            void writeMovetext(unsigned char*& buffer);

            void writeString(unsigned char*& buffer, const std::string& str) const;

            void writeString(unsigned char*& buffer, const char* str, std::uint8_t length) const;

            [[nodiscard]] unsigned mapResultToInt() const;

            FORCEINLINE void writeBigEndian(unsigned char*& buffer, std::uint16_t value);

            [[nodiscard]] std::size_t computeHeaderLength() const;

            void addBitsLE8(std::uint8_t bits, std::size_t count);
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

        void writeFileHeader();

        void writeCurrentGame();

        [[nodiscard]] bool enoughSpaceForNextGame() const;

        void swapAndPersistFrontBuffer();
    };

    struct UnparsedBcgnGameMoves
    {
        UnparsedBcgnGameMoves(
            BcgnFileHeader header, 
            util::UnsignedCharBufferView movetext,
            std::size_t numMovesLeft
            ) noexcept;

        [[nodiscard]] bool hasNext() const;

        [[nodiscard]] Move next(const Position& pos);

    private:
        BcgnFileHeader m_header;
        util::UnsignedCharBufferView m_encodedMovetext;
        std::size_t m_bitsLeft;
        std::size_t m_numMovesLeft;

        [[nodiscard]] std::uint8_t extractBitsLE8(std::size_t count);
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
                util::UnsignedCharBufferView movetext,
                std::size_t numMoves
                ) noexcept;

            iterator(
                BcgnFileHeader header, 
                const Position& pos, 
                util::UnsignedCharBufferView movetext,
                std::size_t numMoves
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
            util::UnsignedCharBufferView movetext,
            std::size_t numMoves
            ) noexcept;

        UnparsedBcgnGamePositions(
            BcgnFileHeader header,
            const Position& startpos, 
            util::UnsignedCharBufferView movetext,
            std::size_t numMoves
            ) noexcept;

        [[nodiscard]] iterator begin();

        [[nodiscard]] iterator::sentinel end() const;

    private:
        BcgnFileHeader m_header;
        Position m_startpos;
        util::UnsignedCharBufferView m_encodedMovetext;
        std::size_t m_numMoves;
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

    struct UnparsedBcgnGameHeader
    {
        UnparsedBcgnGameHeader(util::UnsignedCharBufferView sv);

        void setFileHeader(BcgnFileHeader header);

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

        [[nodiscard]] PositionWithZobrist startPositionWithZobrist() const;

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
        BcgnGameFlags m_flags;
        std::string_view m_whitePlayer;
        std::string_view m_blackPlayer;
        std::string_view m_event;
        std::string_view m_site;
        std::size_t m_additionalTagsOffset;

        [[nodiscard]] std::size_t getStringsOffset() const;

        [[nodiscard]] Position getCustomStartPos() const;

        void prereadData();

        [[nodiscard]] std::uint16_t readHeaderLength() const;

        [[nodiscard]] std::optional<GameResult> mapIntToResult(unsigned v) const;
    };

    struct UnparsedBcgnGame
    {
        UnparsedBcgnGame() = default;

        void setFileHeader(BcgnFileHeader header);

        void setGameData(util::UnsignedCharBufferView sv);

        [[nodiscard]] UnparsedBcgnGameHeader gameHeader() const;

        [[nodiscard]] bool hasGameHeader() const;

        [[nodiscard]] UnparsedBcgnGameMoves moves() const;

        [[nodiscard]] UnparsedBcgnGamePositions positions() const;

        [[nodiscard]] Position startPosition() const;

        [[nodiscard]] PositionWithZobrist startPositionWithZobrist() const;

        [[nodiscard]] bool hasCustomStartPosition() const;

        [[nodiscard]] std::uint16_t numPlies() const;

        [[nodiscard]] std::optional<GameResult> result() const;

    private:
        BcgnFileHeader m_header;
        util::UnsignedCharBufferView m_data;
        std::uint16_t m_headerLength;
        std::uint16_t m_numPlies;
        std::optional<GameResult> m_result;
        BcgnGameFlags m_flags;

        void prereadData();

        [[nodiscard]] util::UnsignedCharBufferView encodedMovetext() const;

        [[nodiscard]] Position getCustomStartPos() const;

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
