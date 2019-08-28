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
#include "Position.h"
#include "San.h"

namespace pgn
{
    using namespace std::literals;

    enum struct GameResult : std::uint8_t
    {
        WhiteWin,
        BlackWin,
        Draw,
        Unknown
    };

    namespace detail
    {
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
                if (event == nullptr || event - s.data() > s.size())
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

        inline void seekNextMove(std::string_view& s)
        {
            for (;;)
            {
                // skip characters we don't care about
                while ("01234567890. $"sv.find(s[0])) s.remove_prefix(1u);

                if (isCommentBegin(s[0]))
                {
                    skipComment(s);
                }
                else if (isVariationBegin(s[0]))
                {
                    skipVariation(s);
                }
                else if (san::isValidSanMoveStart(s[0]))
                {
                    return;
                }
                else
                {
                    s.remove_prefix(s.size());
                    return;
                }
            }
        }

        inline std::string_view extractMove(std::string_view s)
        {
            ASSERT(san::isValidSanMoveStart(s[0]));

            const std::size_t length = s.find(' ');
            if (length == std::string::npos)
            {
                return {};
            }

            return s.substr(0, length);
        }

        [[nodiscard]] inline std::string_view findTagValue(std::string_view s, std::string_view tag)
        {
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
        [[nodiscard]] inline GameResult parseGameResult(std::string_view tag)
        {
            // tag is one of the following:
            // 1-0
            // 0-1
            // 1/2-1/2
            // *

            if (tag.size() < 3)
            {
                return GameResult::Unknown;
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
                return GameResult::Unknown;
            }
        }
    }

    constexpr std::string_view tagRegionEndSequence = "\n\n";
    constexpr std::string_view moveRegionEndSequence = "\n\n";

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

            UnparsedPositionsIterator(std::string_view moveRegion) noexcept :
                m_moveRegion(moveRegion),
                m_position(Position::startPosition())
            {
                ASSERT(m_moveRegion.front() == '1');
            }

            const UnparsedPositionsIterator& operator++()
            {
                detail::seekNextMove(m_moveRegion);
                if (m_moveRegion.empty())
                {
                    m_moveRegion.remove_prefix(m_moveRegion.size());
                    return *this;
                }

                const std::string_view san = detail::extractMove(m_moveRegion);
                if (san.empty())
                {
                    m_moveRegion.remove_prefix(m_moveRegion.size());
                    return *this;
                }

                const Move move = san::sanToMove(m_position, san);
                m_position.doMove(move);

                m_moveRegion.remove_prefix(san.size());
            
                return *this;
            }

            [[nodiscard]] bool friend operator==(const UnparsedPositionsIterator& lhs, Sentinel rhs) noexcept
            {
                return lhs.m_moveRegion.empty();
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
            std::string_view m_moveRegion;
            Position m_position;
        };

    public:

        using iterator = UnparsedPositionsIterator;
        using const_iterator = UnparsedPositionsIterator;

        UnparsedGamePositions(std::string_view moveRegion) noexcept :
            m_moveRegion(moveRegion)
        {
            ASSERT(!m_moveRegion.empty());
        }

        [[nodiscard]] UnparsedPositionsIterator begin()
        {
            return { m_moveRegion };
        }

        [[nodiscard]] UnparsedPositionsIterator::Sentinel end() const
        {
            return {};
        }

    private:
        std::string_view m_moveRegion;
    };

    struct UnparsedGame
    {
        explicit UnparsedGame() :
            m_tagRegion{},
            m_moveRegion{}
        {
        }

        UnparsedGame(std::string_view tagRegion, std::string_view moveRegion) noexcept :
            m_tagRegion(tagRegion),
            m_moveRegion(moveRegion)
        {
            ASSERT(m_tagRegion.front() == '[');
            ASSERT(m_moveRegion.front() == '1');
        }

        [[nodiscard]] GameResult result() const
        {
            const std::string_view tag = detail::findTagValue(m_tagRegion, "Result"sv);
            return detail::parseGameResult(tag);
        }

        [[nodiscard]] std::string_view tagRegion() const
        {
            return m_tagRegion;
        }

        [[nodiscard]] std::string_view moveRegion() const
        {
            return m_moveRegion;
        }

        [[nodiscard]] UnparsedGamePositions positions() const
        {
            return UnparsedGamePositions(m_moveRegion);
        }

    private:
        std::string_view m_tagRegion;
        std::string_view m_moveRegion;
    };

    // is supposed to work as a game iterator
    // stores the current game
    struct LazyPgnFileReader
    {
    private:

        struct LazyPgnFileReaderIterator
        {
            struct Sentinel {};

            using value_type = UnparsedGame;
            using difference_type = std::ptrdiff_t;
            using reference = const UnparsedGame &;
            using iterator_category = std::input_iterator_tag;
            using pointer = const UnparsedGame*;

            LazyPgnFileReaderIterator(LazyPgnFileReader& fr) noexcept :
                m_fileReader(&fr)
            {
            }

            const LazyPgnFileReaderIterator& operator++()
            {
                m_fileReader->moveToNextGame();
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
                return m_fileReader->m_game;
            }

            [[nodiscard]] const UnparsedGame* operator->() const
            {
                return &m_fileReader->m_game;
            }

        private:
            LazyPgnFileReader* m_fileReader;

            [[nodiscard]] bool isEnd() const
            {
                return m_fileReader->m_buffer.front() == '\0';
            }
        };

    public:

        using iterator = LazyPgnFileReaderIterator;
        using const_iterator = LazyPgnFileReaderIterator;

        // currently bufferSize must be bigger than the maximum number of bytes taken by a single game
        // TODO: resize buffer when didn't process anything
        static constexpr std::size_t bufferSize = 1024 * 32;

        LazyPgnFileReader(const std::filesystem::path& path) :
            m_file(nullptr, &std::fclose),
            m_buffer(bufferSize + 1), // one spot for '\0',
            m_game{}
        {
            auto strPath = path.string();
            m_file.reset(std::fopen(strPath.c_str(), "r"));

            if (m_file == nullptr)
            {
                m_buffer[0] = '\0';
                return;
            }

            const std::size_t numBytesRead = std::fread(m_buffer.data(), 1, bufferSize, m_file.get());
            m_buffer[numBytesRead] = '\0';
            m_bufferView = std::string_view(m_buffer.data(), numBytesRead);

            // find the first game
            moveToNextGame();
        }

        [[nodiscard]] bool isOpen() const
        {
            return m_file != nullptr;
        }

        [[nodiscard]] LazyPgnFileReaderIterator begin()
        {
#if !defined(NDEBUG)
            // only one iterator can be constructed
            // because the file can only be traversed once
            ASSERT(!m_iteratorConstructed);
            m_iteratorConstructed = true;
#endif

            return { *this };
        }

        [[nodiscard]] LazyPgnFileReaderIterator::Sentinel end() const
        {
            return {};
        }

    private:
        void moveToNextGame()
        {
            while(m_buffer.front() != '\0')
            {
                // try find region bounds
                const std::size_t tagEnd = m_bufferView.find(tagRegionEndSequence);
                if (tagEnd == std::string::npos)
                {
                    // fetch more data if needed
                    refillBuffer();
                    continue;
                }

                const std::size_t moveStart = tagEnd + tagRegionEndSequence.size();
                const std::size_t moveEnd = m_bufferView.find(moveRegionEndSequence, moveStart);
                if (moveEnd == std::string::npos)
                {
                    refillBuffer();
                    continue;
                }

                // we only extract one game at the time

                m_game = UnparsedGame(
                    m_bufferView.substr(0, tagEnd),
                    m_bufferView.substr(moveStart, moveEnd - moveStart)
                );

                m_bufferView.remove_prefix(moveEnd + moveRegionEndSequence.size());

                return;
            }
        }

    private:
        std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
        std::vector<char> m_buffer;
        std::string_view m_bufferView; // what is currently being processed
        UnparsedGame m_game;
#if !defined(NDEBUG)
        bool m_iteratorConstructed = false;
#endif

        void refillBuffer()
        {
            // copies unprocessed data to the beginning
            // and fills the rest with new data

            // copy to the beginning everything that was not yet read (commited)
            std::size_t numBytesProcessed = m_bufferView.data() - m_buffer.data();
            if (numBytesProcessed == 0)
            {
                // if we were unable to process anything then scrap the whole buffer
                numBytesProcessed = bufferSize;
            }
            else if (numBytesProcessed != bufferSize)
            {
                std::memmove(m_buffer.data(), m_buffer.data() + numBytesProcessed, bufferSize - numBytesProcessed);
            }

            // fill the buffer and put '\0' at the end
            const std::size_t numBytesLeft = bufferSize - numBytesProcessed;
            const std::size_t numBytesRead = std::fread(m_buffer.data() + numBytesLeft, 1, numBytesProcessed, m_file.get());
            m_buffer[numBytesLeft + numBytesRead] = '\0';

            m_bufferView = std::string_view(m_buffer.data(), numBytesLeft + numBytesRead);
        }
    };


}
