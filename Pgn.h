#pragma once

#include <vector>
#include <memory>
#include <optional>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string_view>

#include "Assert.h"
#include "Date.h"
#include "Eco.h"
#include "Position.h"
#include "San.h"

namespace pgn
{
    using namespace std::literals;

    struct TagView
    {
        std::string_view key;
        std::string_view value;
    };

    namespace detail
    {
        [[nodiscard]] std::uint16_t parseUInt16(std::string_view sv)
        {
            ASSERT(sv.size() > 0);
            ASSERT(sv.size() <= 5);

            std::uint16_t v = 0;

            std::size_t idx = 0;
            switch (sv.size())
            {
            case 5:
                v += (sv[idx++] - '0') * 10000;
            case 4:
                v += (sv[idx++] - '0') * 1000;
            case 3:
                v += (sv[idx++] - '0') * 100;
            case 2:
                v += (sv[idx++] - '0') * 10;
            case 1:
                v += sv[idx] - '0';
                break;

            default:
                ASSERT(false);
            }

            return v;
        }

        // Date parsing is a bit lenient - it accepts yyyy, yyyy.mm, yyyy.mm.dd
        [[nodiscard]] Date parseDate(std::string_view sv)
        {
            ASSERT(sv.size() >= 4);

            std::uint16_t year = parseUInt16(sv.substr(0, 4));
            std::uint8_t month = 0;
            std::uint8_t day = 0;

            if (sv.size() >= 7)
            {
                month = (sv[5] - '0') * 10 + (sv[6] - '0');
            }

            if (sv.size() >= 10)
            {
                day = (sv[8] - '0') * 10 + (sv[9] - '0');
            }

            return Date(year, month, day);
        }

        [[nodiscard]] constexpr bool isCommentBegin(char c)
        {
            return c == '{' || c == ';';
        }

        [[nodiscard]] constexpr bool isVariationBegin(char c)
        {
            return c == '(';
        }

        // Readjusts `s` to start at the first character after comment ends.
        // If the comment doesn't end then makes `s` empty
        // Comments cannot be recursive.
        inline void skipComment(std::string_view& s)
        {
            const char first = s[0];
            ASSERT(isCommentBegin(first));

            const std::size_t shift = s.find(first == '{' ? '}' : '\n');
            if (shift == std::string::npos)
            {
                s.remove_prefix(s.size());
            }
            else
            {
                s.remove_prefix(shift);
            }
        }

        // Readjusts `s` to start at the first character after variation ends.
        // If the variation doesn't end then makes `s` empty
        // Variations can be recursive.
        inline void skipVariation(std::string_view& s)
        {
            ASSERT(isVariationBegin(s[0]));

            int numUnclosedParens = 1;

            while (numUnclosedParens)
            {
                // a safer version with bounds would be nice
                // but std::string_view::find_first_of is horrendously slow
                const char* event = std::strpbrk(s.data() + 1u, "(){;");
                if (event == nullptr || static_cast<std::size_t>(event - s.data()) > s.size())
                {
                    s.remove_prefix(s.size());
                    return;
                }

                s.remove_prefix(event - s.data());

                switch (s[0])
                {
                case '{':
                case ';':
                    skipComment(s);
                    if (s.empty())
                    {
                        return;
                    }
                    break;

                case '(':
                    numUnclosedParens += 1;
                    break;

                case ')':
                    numUnclosedParens -= 1;
                    break;
                }
            }
        }

        namespace lookup::seekNextMove
        {
            constexpr std::array<bool, 256> skip = []() {
                std::array<bool, 256> skip{};

                skip['0'] = true;
                skip['1'] = true;
                skip['2'] = true;
                skip['3'] = true;
                skip['4'] = true;
                skip['5'] = true;
                skip['6'] = true;
                skip['7'] = true;
                skip['8'] = true;
                skip['9'] = true;
                skip['.'] = true;
                skip['$'] = true;
                skip['\n'] = true;
                skip[' '] = true;

                return skip;
            }();
        }

        inline void seekNextMove(std::string_view& s)
        {
            // We know that after a move there is at least one space.
            // Or at the beginning of movetext there is one digit.
            std::size_t idx = 1;

            for (;;)
            {
                // skip characters we don't care about
                {
                    while (lookup::seekNextMove::skip[static_cast<unsigned char>(s[idx])])
                    {
                        ++idx;
                        // We don't guard against going past the end because:
                        // 1. It is not needed for valid pgns.
                        // 2. In case of invalid pgns we will go to the
                        //    end of the buffer which is null-terminated.
                    }

                    if (idx >= s.size())
                    {
                        // This only happens when the movetext is malformed.
                        s.remove_prefix(s.size());
                        return;
                    }
                    else
                    {
                        s.remove_prefix(idx);
                    }
                }

                if (san::isValidSanMoveStart(s[0]))
                {
                    return;
                }
                
                if (isCommentBegin(s[0]))
                {
                    skipComment(s);
                }
                else if (isVariationBegin(s[0]))
                {
                    skipVariation(s);
                }
                else
                {
                    s.remove_prefix(s.size());
                    return;
                }

                // After a variation or a comment there
                // may be no space before the san.
                idx = 0;
            }
        }

        inline void seekNextTag(std::string_view& s)
        {
            const std::size_t idx = s.find('[');
            if (idx == std::string::npos)
            {
                s.remove_prefix(s.size());
            }
            else
            {
                s.remove_prefix(idx);
            }
        }

        inline TagView extractTagAdvance(std::string_view& s)
        {
            ASSERT(s.size() > 0);
            ASSERT(s[0] == '[');

            // Shortest valid tag is [A ""] which has length of 6
            // It is assumed that there is no space after [,
            // there is only one space between key and value, 
            // and that there is no space after ",
            // and that it ends with a ].
            if (s.size() < 6)
            {
                return{};
            }

            const std::size_t space = s.find(' ', 1);
            if (space == std::string::npos)
            {
                return {};
            }
            std::string_view key = s.substr(1, space - 1);

            const std::size_t end = s.find('\"', space + 2);
            if (end == std::string::npos)
            {
                return {};
            }
            std::string_view value = s.substr(space + 2, end - (space + 2));

            s.remove_prefix(end + 2);

            return { key, value };
        }

        namespace lookup::extractMove
        {
            constexpr std::array<bool, 256> skip = []() {
                std::array<bool, 256> skip{};
                for (auto& v : skip) v = true;

                skip['\t'] = false;
                skip['\n'] = false;
                skip[' '] = false;

                return skip;
            }();
        }

        inline std::string_view extractMoveAdvance(std::string_view& s)
        {
            constexpr std::size_t minSanLength = 2;

            ASSERT(san::isValidSanMoveStart(s[0]));
            ASSERT(s.size() > minSanLength);

            std::size_t idx = minSanLength;
            while (lookup::extractMove::skip[static_cast<unsigned char>(s[idx])])
            {
                ++idx;
            }

            if (idx > s.size())
            {
                s.remove_prefix(s.size());
                return s;
            }

            auto ret = s.substr(0, idx);
            s.remove_prefix(idx);

            return ret;
        }

        // NOTE: We don't support escaping quotation marks inside a tag value.
        [[nodiscard]] inline std::string_view findTagValue(std::string_view s, std::string_view tagName)
        {
            constexpr std::size_t maxTagNameLength = 256;

            ASSERT(tagName.size() + 2u <= maxTagNameLength);

            char tagMatch[maxTagNameLength];
            tagMatch[0] = '[';
            tagName.copy(tagMatch + 1u, tagName.size());
            tagMatch[tagName.size() + 1u] = ' ';
            std::string_view tag(tagMatch, tagName.size() + 2u);

            const std::size_t shift = s.find(tag);
            if (shift == std::string::npos)
            {
                return {};
            }

            const std::size_t valueStart = s.find('\"', shift);
            if (valueStart == std::string::npos)
            {
                return {};
            }

            s.remove_prefix(valueStart + 1u);

            const std::size_t valueLength = s.find('\"');
            if (valueLength == std::string::npos)
            {
                return {};
            }

            return s.substr(0, valueLength);
        }

        // `tag` is the string between quotation marks
        // It is assumed that the result value is correct
        [[nodiscard]] inline std::optional<GameResult> parseGameResult(std::string_view tag)
        {
            // tag is one of the following:
            // 1-0
            // 0-1
            // 1/2-1/2
            // *

            if (tag.size() < 3)
            {
                return {};
            }

            switch (tag[2])
            {
            case '0':
                return GameResult::WhiteWin;
            case '1':
                return GameResult::BlackWin;
            case '2':
                return GameResult::Draw;
            default:
                return {};
            }
        }
    }

    constexpr std::string_view tagSectionEndSequence = "]\n\n";
    constexpr std::string_view moveSectionEndSequence = "\n\n";

    struct UnparsedGamePositions
    {
    private:

        struct UnparsedPositionsIterator
        {
            struct Sentinel {};

            using value_type = Position;
            using difference_type = std::ptrdiff_t;
            using reference = const Position &;
            using iterator_category = std::input_iterator_tag;
            using pointer = const Position*;

            UnparsedPositionsIterator(std::string_view moveSection) noexcept :
                m_position(Position::startPosition()),
                m_moveSection(moveSection)
            {
                ASSERT(m_moveSection.front() == '1');
            }

            const UnparsedPositionsIterator& operator++()
            {
                // TODO: Indicate somehow that there was an error and the position
                //       stream is ending abruptly.
                //       For example when a move is missing "22.Ba3 -- 23.a6 b4"
                //       we want to propagate that to the importer so the game can be skipped.

                detail::seekNextMove(m_moveSection);
                if (m_moveSection.empty())
                {
                    return *this;
                }

                const std::string_view san = detail::extractMoveAdvance(m_moveSection);
                if (san.empty())
                {
                    return *this;
                }

                const Move move = san::sanToMove(m_position, san);
                m_position.doMove(move);
            
                return *this;
            }

            [[nodiscard]] bool friend operator==(const UnparsedPositionsIterator& lhs, Sentinel rhs) noexcept
            {
                return lhs.m_moveSection.empty();
            }

            [[nodiscard]] bool friend operator!=(const UnparsedPositionsIterator& lhs, Sentinel rhs) noexcept
            {
                return !(lhs == rhs);
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
            std::string_view m_moveSection;
        };

    public:

        using iterator = UnparsedPositionsIterator;
        using const_iterator = UnparsedPositionsIterator;

        UnparsedGamePositions(std::string_view moveSection) noexcept :
            m_moveSection(moveSection)
        {
            ASSERT(!m_moveSection.empty());
        }

        [[nodiscard]] UnparsedPositionsIterator begin()
        {
            return { m_moveSection };
        }

        [[nodiscard]] UnparsedPositionsIterator::Sentinel end() const
        {
            return {};
        }

    private:
        std::string_view m_moveSection;
    };

    struct UnparsedGameTags
    {
    private:

        struct UnparsedTagsIterator
        {
            struct Sentinel {};

            using value_type = TagView;
            using difference_type = std::ptrdiff_t;
            using reference = const TagView &;
            using iterator_category = std::input_iterator_tag;
            using pointer = const TagView*;

            UnparsedTagsIterator(std::string_view tagSection) noexcept :
                m_tagSection(tagSection)
            {
                ASSERT(m_tagSection.front() == '[');
            }

            const UnparsedTagsIterator& operator++()
            {
                detail::seekNextTag(m_tagSection);
                if (m_tagSection.empty())
                {
                    return *this;
                }

                m_tag = detail::extractTagAdvance(m_tagSection);

                return *this;
            }

            [[nodiscard]] bool friend operator==(const UnparsedTagsIterator& lhs, Sentinel rhs) noexcept
            {
                return lhs.m_tagSection.empty();
            }

            [[nodiscard]] bool friend operator!=(const UnparsedTagsIterator& lhs, Sentinel rhs) noexcept
            {
                return !(lhs == rhs);
            }

            [[nodiscard]] const TagView& operator*() const
            {
                return m_tag;
            }

            [[nodiscard]] const TagView* operator->() const
            {
                return &m_tag;
            }

        private:
            std::string_view m_tagSection;
            TagView m_tag;
        };

    public:

        using iterator = UnparsedTagsIterator;
        using const_iterator = UnparsedTagsIterator;

        UnparsedGameTags(std::string_view tagSection) noexcept :
            m_tagSection(tagSection)
        {
            ASSERT(!m_tagSection.empty());
        }

        [[nodiscard]] UnparsedTagsIterator begin()
        {
            return { m_tagSection };
        }

        [[nodiscard]] UnparsedTagsIterator::Sentinel end() const
        {
            return {};
        }

    private:
        std::string_view m_tagSection;
    };

    struct UnparsedGame
    {
        explicit UnparsedGame() :
            m_tagSection{},
            m_moveSection{}
        {
        }

        UnparsedGame(std::string_view tagSection, std::string_view moveSection) noexcept :
            m_tagSection(tagSection),
            m_moveSection(moveSection)
        {
            ASSERT(m_tagSection.front() == '[');
            ASSERT(m_moveSection.front() == '1');
        }

        void getResultDateEcoEventWhiteBlack(
            std::optional<GameResult>& result,
            Date& date,
            Eco& eco,
            std::string_view& event,
            std::string_view& white,
            std::string_view& black
        ) const
        {
            for (auto&& tag : tags())
            {
                if (tag.key == "Event"sv)
                {
                    event = tag.value;
                }
                else if (tag.key == "White"sv)
                {
                    white = tag.value;
                }
                else if (tag.key == "Black"sv)
                {
                    black = tag.value;
                }
                else if (tag.key == "Date"sv || tag.key == "UTCDate"sv)
                {
                    date = detail::parseDate(tag.value);
                }
                else if (tag.key == "ECO"sv)
                {
                    eco = Eco(tag.value);
                }
                else if (tag.key == "Result"sv)
                {
                    result = detail::parseGameResult(tag.value);
                }
            }
        }

        void getResultDateEcoEventWhiteBlackPlyCount(
            std::optional<GameResult>& result,
            Date& date,
            Eco& eco,
            std::string_view& event,
            std::string_view& white,
            std::string_view& black,
            std::uint16_t& plyCount
        ) const
        {
            for (auto&& tag : tags())
            {
                if (tag.key == "Event"sv)
                {
                    event = tag.value;
                }
                else if (tag.key == "White"sv)
                {
                    white = tag.value;
                }
                else if (tag.key == "Black"sv)
                {
                    black = tag.value;
                }
                else if (tag.key == "Date"sv || tag.key == "UTCDate"sv)
                {
                    date = detail::parseDate(tag.value);
                }
                else if (tag.key == "ECO"sv)
                {
                    eco = Eco(tag.value);
                }
                else if (tag.key == "Result"sv)
                {
                    result = detail::parseGameResult(tag.value);
                }
                else if (tag.key == "PlyCount"sv)
                {
                    plyCount = detail::parseUInt16(tag.value);
                }
            }
        }

        [[nodiscard]] std::optional<GameResult> result() const
        {
            const std::string_view tag = detail::findTagValue(m_tagSection, "Result"sv);
            return detail::parseGameResult(tag);
        }

        [[nodiscard]] Date date() const
        {
            std::string_view tag = detail::findTagValue(m_tagSection, "Date"sv);
            if (tag.empty())
            {
                // lichess database uses this - non standard - tag
                tag = detail::findTagValue(m_tagSection, "UTCDate"sv);
            }

            if (tag.empty())
            {
                return {};
            }
            return detail::parseDate(tag);
        }

        [[nodiscard]] Eco eco() const
        {
            return { detail::findTagValue(m_tagSection, "ECO"sv) };
        }

        [[nodiscard]] std::uint16_t plyCount() const
        {
            const std::string_view tag = detail::findTagValue(m_tagSection, "PlyCount"sv);
            return detail::parseUInt16(tag);
        }

        [[nodiscard]] std::uint16_t plyCount(std::uint16_t def) const
        {
            const std::string_view tag = detail::findTagValue(m_tagSection, "PlyCount"sv);
            if (tag.empty())
            {
                return def;
            }
            return detail::parseUInt16(tag);
        }

        [[nodiscard]] std::string_view tag(std::string_view tag) const
        {
            return detail::findTagValue(m_tagSection, tag);
        }

        [[nodiscard]] std::string_view tagSection() const
        {
            return m_tagSection;
        }

        [[nodiscard]] std::string_view moveSection() const
        {
            return m_moveSection;
        }

        [[nodiscard]] UnparsedGamePositions positions() const
        {
            return UnparsedGamePositions(m_moveSection);
        }

        [[nodiscard]] UnparsedGameTags tags() const
        {
            return UnparsedGameTags(m_tagSection);
        }

    private:
        std::string_view m_tagSection;
        std::string_view m_moveSection;
    };

    // is supposed to work as a game iterator
    // stores the current game
    struct LazyPgnFileReader
    {
    private:
        // currently bufferSize must be bigger than the maximum number of bytes taken by a single game
        // TODO: resize buffer when didn't process anything
        static constexpr std::size_t m_minBufferSize = 128ull * 1024ull;

        struct LazyPgnFileReaderIterator
        {
            struct Sentinel {};

            using value_type = UnparsedGame;
            using difference_type = std::ptrdiff_t;
            using reference = const UnparsedGame &;
            using iterator_category = std::input_iterator_tag;
            using pointer = const UnparsedGame*;

            LazyPgnFileReaderIterator(const std::filesystem::path& path, std::size_t bufferSize) :
                m_file(nullptr, &std::fclose),
                m_bufferSize(bufferSize),
                m_buffer(bufferSize + 1), // one spot for '\0',
                m_auxBuffer(bufferSize),
                m_auxBufferLeft(0),
                m_bufferView(m_buffer.data(), bufferSize),
                m_game{}
            {
                auto strPath = path.string();
                m_file.reset(std::fopen(strPath.c_str(), "r"));

                if (m_file == nullptr)
                {
                    m_buffer[0] = '\0';
                    return;
                }

                refillBuffer();

                // find the first game
                moveToNextGame();
            }

            const LazyPgnFileReaderIterator& operator++()
            {
                moveToNextGame();
                return *this;
            }

            [[nodiscard]] bool friend operator==(const LazyPgnFileReaderIterator& lhs, Sentinel rhs) noexcept
            {
                return lhs.isEnd();
            }

            [[nodiscard]] bool friend operator!=(const LazyPgnFileReaderIterator& lhs, Sentinel rhs) noexcept
            {
                return !(lhs == rhs);
            }

            [[nodiscard]] const UnparsedGame& operator*() const
            {
                return m_game;
            }

            [[nodiscard]] const UnparsedGame* operator->() const
            {
                return &m_game;
            }

        private:
            std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
            std::size_t m_bufferSize;
            std::vector<char> m_buffer;
            std::vector<char> m_auxBuffer;
            std::future<std::size_t> m_future;
            std::size_t m_auxBufferLeft;
            std::string_view m_bufferView; // what is currently being processed
            UnparsedGame m_game;

            [[nodiscard]] bool isEnd() const
            {
                return m_buffer.front() == '\0';
            }

            void moveToNextGame()
            {
                while (m_buffer.front() != '\0')
                {
                    // We look for a sequence:
                    // 1. any number of empty lines
                    // 2. any number of non-empty lines - tag section
                    // 3. any number of empty lines
                    // 4. any number of non-empty lines - move section
                    // 5. any number of empty lines
                    //
                    // If we cannot find such a sequence then more data is fetched.
                    // If we cannot find such a sequence after looking through the whole buffer
                    // then we scrap the buffer (TODO: buffer resizing)

                    const std::size_t tagStart = m_bufferView.find_first_not_of('\n');
                    if (tagStart == std::string::npos)
                    {
                        refillBuffer();
                        continue;
                    }

                    const std::size_t tagEnd = m_bufferView.find(tagSectionEndSequence, tagStart);
                    if (tagEnd == std::string::npos)
                    {
                        refillBuffer();
                        continue;
                    }

                    const std::size_t moveStart = m_bufferView.find_first_not_of('\n', tagEnd + tagSectionEndSequence.size());
                    if (moveStart == std::string::npos)
                    {
                        refillBuffer();
                        continue;
                    }

                    const std::size_t moveEnd = m_bufferView.find(moveSectionEndSequence, moveStart);
                    if (moveEnd == std::string::npos)
                    {
                        refillBuffer();
                        continue;
                    }

                    std::size_t nextGameStart = m_bufferView.find_first_not_of('\n', moveEnd + moveSectionEndSequence.size());
                    if (nextGameStart == std::string::npos)
                    {
                        nextGameStart = m_bufferView.size();
                    }

                    // We only extract one game at the time.

                    m_game = UnparsedGame(
                        m_bufferView.substr(tagStart, tagEnd - tagStart + 1), // because tag end sequence contains ]
                        m_bufferView.substr(moveStart, moveEnd - moveStart)
                    );

                    m_bufferView.remove_prefix(nextGameStart);

                    return;
                }
            }

            void refillBuffer()
            {
                // copies unprocessed data to the beginning
                // and fills the rest with new data

                // copy to the beginning everything that was not yet read (commited)
                std::size_t numBytesProcessed = m_bufferView.data() - m_buffer.data();
                if (numBytesProcessed == 0)
                {
                    // if we were unable to process anything then scrap the whole buffer
                    numBytesProcessed = m_bufferSize;
                }
                else if (numBytesProcessed != m_bufferSize)
                {
                    std::memmove(m_buffer.data(), m_buffer.data() + numBytesProcessed, m_bufferSize - numBytesProcessed);
                }

                // fill the buffer and put '\0' at the end
                const std::size_t numBytesLeft = m_bufferSize - numBytesProcessed;
                std::size_t numBytesRead;
                if (m_future.valid())
                {
                    std::size_t actualRead = m_future.get();
                    numBytesRead = std::min(numBytesProcessed, actualRead + m_auxBufferLeft);
                    std::memcpy(m_buffer.data() + numBytesLeft, m_auxBuffer.data(), numBytesRead);
                    std::memmove(m_auxBuffer.data(), m_auxBuffer.data() + numBytesRead, m_auxBuffer.size() - numBytesRead);
                    m_auxBufferLeft = (m_auxBuffer.size() - numBytesRead);
                    if (numBytesRead == numBytesProcessed)
                    {
                        m_future = std::async(std::launch::async, [this]() {return std::fread(m_auxBuffer.data() + m_auxBufferLeft, 1, m_auxBuffer.size() - m_auxBufferLeft, m_file.get()); });
                    }
                }
                else
                {
                    numBytesRead = std::fread(m_buffer.data() + numBytesLeft, 1, numBytesProcessed, m_file.get());
                    if (numBytesRead == numBytesProcessed)
                    {
                        m_future = std::async(std::launch::async, [this]() {return std::fread(m_auxBuffer.data(), 1, m_auxBuffer.size(), m_file.get()); });
                    }
                }

                // If we hit the end of file we make sure that it ends with at least two new lines.
                // One is there by the PGN standard, the second one may not.
                // We need it for easier search for empty lines. We can just search for "\n\n"
                // If the buffer is empty we don't add anything so it is recognized as empty by the parser.
                if (numBytesLeft + numBytesRead && numBytesRead < numBytesProcessed)
                {
                    m_buffer[numBytesLeft + numBytesRead] = '\n';
                    m_buffer[numBytesLeft + numBytesRead + 1u] = '\0';
                    m_bufferView = std::string_view(m_buffer.data(), numBytesLeft + numBytesRead + 1u);
                }
                else
                {
                    m_buffer[numBytesLeft + numBytesRead] = '\0';
                    m_bufferView = std::string_view(m_buffer.data(), numBytesLeft + numBytesRead);
                }
            }
        };

    public:

        using iterator = LazyPgnFileReaderIterator;
        using const_iterator = LazyPgnFileReaderIterator;

        // We keep the file opened. That way we weakly enforce that a created iterator
        // (that reopens the file to have it's own cursor)
        // is valid after a successful call to isOpen()
        LazyPgnFileReader(const std::filesystem::path& path, std::size_t bufferSize = m_minBufferSize) :
            m_file(nullptr, &std::fclose),
            m_path(path),
            m_bufferSize(std::max(m_minBufferSize, bufferSize))
        {
            auto strPath = path.string();
            m_file.reset(std::fopen(strPath.c_str(), "r"));
        }

        [[nodiscard]] bool isOpen() const
        {
            return m_file != nullptr;
        }

        [[nodiscard]] LazyPgnFileReaderIterator begin()
        {
            return { m_path, m_bufferSize };
        }

        [[nodiscard]] LazyPgnFileReaderIterator::Sentinel end() const
        {
            return {};
        }

    private:
        std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
        std::filesystem::path m_path;
        std::size_t m_bufferSize;
    };
}
