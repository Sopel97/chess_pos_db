#include "Pgn.h"

#include <vector>
#include <memory>
#include <optional>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
#include <string_view>

#include "util/Assert.h"
#include "Date.h"
#include "Eco.h"
#include "chess/GameClassification.h"
#include "detail/ParserBits.h"
#include "Position.h"
#include "San.h"

namespace pgn
{
    using namespace std::literals;

    namespace detail
    {
        // Date parsing is a bit lenient - it accepts yyyy, yyyy.mm, yyyy.mm.dd
        [[nodiscard]] static Date parseDate(std::string_view sv)
        {
            return Date(sv);
        }

        [[nodiscard]] static constexpr bool isCommentBegin(char c)
        {
            return c == '{' || c == ';';
        }

        [[nodiscard]] static constexpr bool isVariationBegin(char c)
        {
            return c == '(';
        }

        // Readjusts `s` to start at the first character after comment ends.
        // If the comment doesn't end then makes `s` empty
        // Comments cannot be recursive.
        // NOINLINE because it's rarely entered.
        static NOINLINE void skipComment(std::string_view& s)
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
        // NOINLINE because it's rarely entered.
        static NOINLINE void skipVariation(std::string_view& s)
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
            static constexpr std::array<bool, 256> skip = []() {
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


        static void seekNextMove(std::string_view& s)
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

                // Handle null move
                if (s[0] == '-')
                {
                    // But we have to be careful not to match result at the end.
                    if (s.size() < 2 || s[1] != '-')
                    {
                        s.remove_prefix(s.size());
                        return;
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

        static void seekNextTag(std::string_view& s)
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

        [[nodiscard]] static TagView extractTagAdvance(std::string_view& s)
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

            const std::size_t end = s.find('\"', space + 2);
            if (end == std::string::npos)
            {
                return {};
            }

            std::string_view key = s.substr(1, space - 1);
            std::string_view value = s.substr(space + 2, end - (space + 2));

            s.remove_prefix(end + 2);

            return { key, value };
        }

        namespace lookup::extractMove
        {
            static constexpr std::array<bool, 256> skip = []() {
                std::array<bool, 256> skip{};
                for (auto& v : skip) v = true;

                skip['\t'] = false;
                skip['\n'] = false;
                skip[' '] = false;
                skip['\0'] = false;

                return skip;
            }();
        }

        [[nodiscard]] static std::string_view extractMoveAdvance(std::string_view& s)
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
        [[nodiscard]] static std::string_view findTagValue(std::string_view s, std::string_view tagName)
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
        [[nodiscard]] static std::optional<GameResult> parseGameResult(std::string_view tag)
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

    UnparsedGamePositions::UnparsedPositionsIterator::UnparsedPositionsIterator(std::string_view moveSection) noexcept :
        m_position(Position::startPosition()),
        m_moveSection(moveSection)
    {
    }

    const UnparsedGamePositions::UnparsedPositionsIterator& UnparsedGamePositions::UnparsedPositionsIterator::operator++()
    {
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
        if (move == Move::null())
        {
            m_moveSection.remove_prefix(m_moveSection.size());
            return *this;
        }

        m_position.doMove(move);

        return *this;
    }

    [[nodiscard]] bool operator==(const UnparsedGamePositions::UnparsedPositionsIterator& lhs, UnparsedGamePositions::UnparsedPositionsIterator::Sentinel rhs) noexcept
    {
        return lhs.m_moveSection.empty();
    }

    [[nodiscard]] bool operator!=(const UnparsedGamePositions::UnparsedPositionsIterator& lhs, UnparsedGamePositions::UnparsedPositionsIterator::Sentinel rhs) noexcept
    {
        return !(lhs == rhs);
    }

    [[nodiscard]] const Position& UnparsedGamePositions::UnparsedPositionsIterator::operator*() const
    {
        return m_position;
    }

    [[nodiscard]] const Position* UnparsedGamePositions::UnparsedPositionsIterator::operator->() const
    {
        return &m_position;
    }

    UnparsedGamePositions::UnparsedGamePositions(std::string_view moveSection) noexcept :
        m_moveSection(moveSection)
    {
        ASSERT(!m_moveSection.empty());
    }

    [[nodiscard]] UnparsedGamePositions::UnparsedPositionsIterator UnparsedGamePositions::begin()
    {
        return { m_moveSection };
    }

    [[nodiscard]] UnparsedGamePositions::UnparsedPositionsIterator::Sentinel UnparsedGamePositions::end() const
    {
        return {};
    }

    UnparsedGameMoves::UnparsedMovesIterator::UnparsedMovesIterator(std::string_view moveSection) noexcept :
        m_san{},
        m_moveSection(moveSection)
    {
        // ASSERT(m_moveSection.front() == '1'); // The move list may be empty...
        ++(*this);
    }

    const UnparsedGameMoves::UnparsedMovesIterator& UnparsedGameMoves::UnparsedMovesIterator::operator++()
    {
        detail::seekNextMove(m_moveSection);
        if (m_moveSection.empty())
        {
            return *this;
        }

        m_san = detail::extractMoveAdvance(m_moveSection);

        return *this;
    }

    [[nodiscard]] bool operator==(const UnparsedGameMoves::UnparsedMovesIterator& lhs, UnparsedGameMoves::UnparsedMovesIterator::Sentinel rhs) noexcept
    {
        return lhs.m_moveSection.empty();
    }

    [[nodiscard]] bool operator!=(const UnparsedGameMoves::UnparsedMovesIterator& lhs, UnparsedGameMoves::UnparsedMovesIterator::Sentinel rhs) noexcept
    {
        return !(lhs == rhs);
    }

    [[nodiscard]] const std::string_view& UnparsedGameMoves::UnparsedMovesIterator::operator*() const
    {
        return m_san;
    }

    [[nodiscard]] const std::string_view* UnparsedGameMoves::UnparsedMovesIterator::operator->() const
    {
        return &m_san;
    }

    UnparsedGameMoves::UnparsedGameMoves(std::string_view moveSection) noexcept :
        m_moveSection(moveSection)
    {
        ASSERT(!m_moveSection.empty());
    }

    [[nodiscard]] UnparsedGameMoves::UnparsedMovesIterator UnparsedGameMoves::begin()
    {
        return { m_moveSection };
    }

    [[nodiscard]] UnparsedGameMoves::UnparsedMovesIterator::Sentinel UnparsedGameMoves::end() const
    {
        return {};
    }

    UnparsedGameTags::UnparsedTagsIterator::UnparsedTagsIterator(std::string_view tagSection) noexcept :
        m_tagSection(tagSection)
    {
        ASSERT(m_tagSection.front() == '[');
    }

    const UnparsedGameTags::UnparsedTagsIterator& UnparsedGameTags::UnparsedTagsIterator::operator++()
    {
        detail::seekNextTag(m_tagSection);
        if (m_tagSection.empty())
        {
            return *this;
        }

        m_tag = detail::extractTagAdvance(m_tagSection);

        return *this;
    }

    [[nodiscard]] bool operator==(const UnparsedGameTags::UnparsedTagsIterator& lhs, UnparsedGameTags::UnparsedTagsIterator::Sentinel rhs) noexcept
    {
        return lhs.m_tagSection.empty();
    }

    [[nodiscard]] bool operator!=(const UnparsedGameTags::UnparsedTagsIterator& lhs, UnparsedGameTags::UnparsedTagsIterator::Sentinel rhs) noexcept
    {
        return !(lhs == rhs);
    }

    [[nodiscard]] const TagView& UnparsedGameTags::UnparsedTagsIterator::operator*() const
    {
        return m_tag;
    }

    [[nodiscard]] const TagView* UnparsedGameTags::UnparsedTagsIterator::operator->() const
    {
        return &m_tag;
    }

    UnparsedGameTags::UnparsedGameTags(std::string_view tagSection) noexcept :
        m_tagSection(tagSection)
    {
        ASSERT(!m_tagSection.empty());
    }

    [[nodiscard]] UnparsedGameTags::UnparsedTagsIterator UnparsedGameTags::begin()
    {
        return { m_tagSection };
    }

    [[nodiscard]] UnparsedGameTags::UnparsedTagsIterator::Sentinel UnparsedGameTags::end() const
    {
        return {};
    }

    UnparsedGame::UnparsedGame() :
        m_tagSection{},
        m_moveSection{}
    {
    }

    UnparsedGame::UnparsedGame(std::string_view tagSection, std::string_view moveSection) noexcept :
        m_tagSection(tagSection),
        m_moveSection(moveSection)
    {
        ASSERT(m_tagSection.front() == '[');
        // ASSERT(m_moveSection.front() == '1'); // The game may have no moves...
    }

    void UnparsedGame::getResultDateEcoEventWhiteBlack(
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
                if (tag.value != "?"sv)
                {
                    eco = Eco(tag.value);
                }
            }
            else if (tag.key == "Result"sv)
            {
                result = detail::parseGameResult(tag.value);
            }
        }
    }

    void UnparsedGame::getResultDateEcoEventWhiteBlackPlyCount(
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
                if (tag.value != "?"sv)
                {
                    eco = Eco(tag.value);
                }
            }
            else if (tag.key == "Result"sv)
            {
                result = detail::parseGameResult(tag.value);
            }
            else if (tag.key == "PlyCount"sv)
            {
                plyCount = parser_bits::parseUInt16(tag.value);
            }
        }
    }

    [[nodiscard]] std::optional<GameResult> UnparsedGame::result() const
    {
        const std::string_view tag = detail::findTagValue(m_tagSection, "Result"sv);
        return detail::parseGameResult(tag);
    }

    [[nodiscard]] Date UnparsedGame::date() const
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

    [[nodiscard]] Eco UnparsedGame::eco() const
    {
        return { detail::findTagValue(m_tagSection, "ECO"sv) };
    }

    [[nodiscard]] std::uint16_t UnparsedGame::plyCount() const
    {
        const std::string_view tag = detail::findTagValue(m_tagSection, "PlyCount"sv);
        return parser_bits::parseUInt16(tag);
    }

    [[nodiscard]] std::uint16_t UnparsedGame::plyCount(std::uint16_t def) const
    {
        const std::string_view tag = detail::findTagValue(m_tagSection, "PlyCount"sv);
        if (tag.empty())
        {
            return def;
        }
        return parser_bits::parseUInt16(tag);
    }

    [[nodiscard]] std::string_view UnparsedGame::tag(std::string_view tag) const
    {
        return detail::findTagValue(m_tagSection, tag);
    }

    [[nodiscard]] std::string_view UnparsedGame::tagSection() const
    {
        return m_tagSection;
    }

    [[nodiscard]] std::string_view UnparsedGame::moveSection() const
    {
        return m_moveSection;
    }

    [[nodiscard]] UnparsedGamePositions UnparsedGame::positions() const
    {
        return UnparsedGamePositions(m_moveSection);
    }

    [[nodiscard]] UnparsedGameMoves UnparsedGame::moves() const
    {
        return UnparsedGameMoves(m_moveSection);
    }

    [[nodiscard]] UnparsedGameTags UnparsedGame::tags() const
    {
        return UnparsedGameTags(m_tagSection);
    }

    LazyPgnFileReader::LazyPgnFileReaderIterator::LazyPgnFileReaderIterator(const std::filesystem::path& path, std::size_t bufferSize) :
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

    const LazyPgnFileReader::LazyPgnFileReaderIterator& LazyPgnFileReader::LazyPgnFileReaderIterator::operator++()
    {
        moveToNextGame();
        return *this;
    }

    [[nodiscard]] bool operator==(const LazyPgnFileReader::LazyPgnFileReaderIterator& lhs, LazyPgnFileReader::LazyPgnFileReaderIterator::Sentinel rhs) noexcept
    {
        return lhs.isEnd();
    }

    [[nodiscard]] bool operator!=(const LazyPgnFileReader::LazyPgnFileReaderIterator& lhs, LazyPgnFileReader::LazyPgnFileReaderIterator::Sentinel rhs) noexcept
    {
        return !(lhs == rhs);
    }

    [[nodiscard]] const UnparsedGame& LazyPgnFileReader::LazyPgnFileReaderIterator::operator*() const
    {
        return m_game;
    }

    [[nodiscard]] const UnparsedGame* LazyPgnFileReader::LazyPgnFileReaderIterator::operator->() const
    {
        return &m_game;
    }

    [[nodiscard]] bool LazyPgnFileReader::LazyPgnFileReaderIterator::isEnd() const
    {
        return m_buffer.front() == '\0';
    }

    void LazyPgnFileReader::LazyPgnFileReaderIterator::moveToNextGame()
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

    // NOINLINE because it is rarely called
    NOINLINE void LazyPgnFileReader::LazyPgnFileReaderIterator::refillBuffer()
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

    // We keep the file opened. That way we weakly enforce that a created iterator
    // (that reopens the file to have it's own cursor)
    // is valid after a successful call to isOpen()
    LazyPgnFileReader::LazyPgnFileReader(const std::filesystem::path& path, std::size_t bufferSize) :
        m_file(nullptr, &std::fclose),
        m_path(path),
        m_bufferSize(std::max(m_minBufferSize, bufferSize))
    {
        auto strPath = path.string();
        m_file.reset(std::fopen(strPath.c_str(), "r"));
    }

    [[nodiscard]] bool LazyPgnFileReader::isOpen() const
    {
        return m_file != nullptr;
    }

    [[nodiscard]] LazyPgnFileReader::LazyPgnFileReaderIterator LazyPgnFileReader::begin()
    {
        return { m_path, m_bufferSize };
    }

    [[nodiscard]] LazyPgnFileReader::LazyPgnFileReaderIterator::Sentinel LazyPgnFileReader::end() const
    {
        return {};
    }
}
