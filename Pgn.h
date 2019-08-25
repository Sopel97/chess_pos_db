#pragma once

#include <vector>
#include <memory>
#include <optional>
#include <cstdio>
#include <cstdint>
#include <filesystem>

#include "Position.h"

namespace pgn
{
    constexpr const char* tagRegionEndSequence = "\n\n";
    constexpr std::size_t tagRegionEndSequenceLength = 2;
    constexpr const char* moveRegionEndSequence = "\n\n";
    constexpr std::size_t moveRegionEndSequenceLength = 2;

    struct UnparsedRegion
    {
        UnparsedRegion(const char* begin, const char* end) :
            m_begin(begin),
            m_end(end)
        {
        }

        const char* begin() const
        {
            return m_begin;
        }

        const char* end() const
        {
            return m_end;
        }

    private:
        const char* m_begin;
        const char* m_end;
    };

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

            UnparsedPositionsIterator(UnparsedGamePositions& positions) :
                m_positions(&positions)
            {
            }

            const UnparsedPositionsIterator& operator++()
            {
                // TODO:
                return *this;
            }

            bool friend operator==(const UnparsedPositionsIterator& lhs, Sentinel rhs)
            {
                return true;
            }

            bool friend operator!=(const UnparsedPositionsIterator& lhs, Sentinel rhs)
            {
                return !(lhs == rhs);
            }

            const Position& operator*() const
            {
                return m_positions->m_position;
            }

            const Position* operator->() const
            {
                return &m_positions->m_position;
            }

        private:
            UnparsedGamePositions* m_positions;
        };

    public:

        using iterator = UnparsedPositionsIterator;
        using const_iterator = UnparsedPositionsIterator;

        UnparsedGamePositions(UnparsedRegion moveRegion) :
            m_moveRegion(moveRegion),
            m_position(Position::startPosition())
        {
        }

        UnparsedPositionsIterator begin() &
        {
            return { *this };
        }

        UnparsedPositionsIterator::Sentinel end() const &
        {
            return {};
        }

    private:
        UnparsedRegion m_moveRegion;
        Position m_position;
    };

    struct UnparsedGame
    {
        UnparsedGame(UnparsedRegion tagRegion, UnparsedRegion moveRegion) :
            m_tagRegion(tagRegion),
            m_moveRegion(moveRegion)
        {
        }

        UnparsedRegion tagRegion() const
        {
            return m_tagRegion;
        }

        UnparsedRegion moveRegion() const
        {
            return m_moveRegion;
        }

        UnparsedGamePositions positions() const
        {
            return UnparsedGamePositions(m_moveRegion);
        }

    private:
        UnparsedRegion m_tagRegion;
        UnparsedRegion m_moveRegion;
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

            LazyPgnFileReaderIterator(LazyPgnFileReader& fr) :
                m_fileReader(&fr)
            {
            }

            const LazyPgnFileReaderIterator& operator++()
            {
                m_fileReader->moveToNextGame();
                return *this;
            }

            bool friend operator==(const LazyPgnFileReaderIterator& lhs, Sentinel rhs)
            {
                return lhs.isEnd();
            }

            bool friend operator!=(const LazyPgnFileReaderIterator& lhs, Sentinel rhs)
            {
                return !(lhs == rhs);
            }

            const UnparsedGame& operator*() const
            {
                return m_fileReader->m_game;
            }

            const UnparsedGame* operator->() const
            {
                return &m_fileReader->m_game;
            }

        private:
            LazyPgnFileReader* m_fileReader;

            bool isEnd() const
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
            m_game({ nullptr, nullptr }, { nullptr, nullptr }),
            m_firstUnprocessed(m_buffer.data())
        {
            auto strPath = path.string();
            m_file.reset(std::fopen(strPath.c_str(), "r"));

            const std::size_t numBytesRead = std::fread(m_buffer.data(), 1, bufferSize, m_file.get());
            m_buffer[numBytesRead] = '\0';

            // find the first game
            moveToNextGame();
        }

        LazyPgnFileReaderIterator begin() &
        {
            return { *this };
        }

        LazyPgnFileReaderIterator::Sentinel end() const &
        {
            return {};
        }

    private:
        void moveToNextGame()
        {
            while(m_buffer.front() != '\0')
            {
                // try find region bounds
                const char* tagend = std::strstr(m_firstUnprocessed, tagRegionEndSequence);
                if (tagend == nullptr)
                {
                    // fetch more data if needed
                    refillBuffer();
                    continue;
                }

                const char* moveend = std::strstr(tagend + tagRegionEndSequenceLength, moveRegionEndSequence);
                if (moveend == nullptr)
                {
                    refillBuffer();
                    continue;
                }

                // we only extract one game at the time

                m_game = UnparsedGame(
                    { m_firstUnprocessed, tagend }, 
                    { tagend + tagRegionEndSequenceLength, moveend }
                );

                m_firstUnprocessed = moveend + moveRegionEndSequenceLength;

                return;
            }
        }

    private:
        std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
        std::vector<char> m_buffer;
        UnparsedGame m_game;
        const char* m_firstUnprocessed; // we don't go back to anything before that

        void refillBuffer()
        {
            // copies unprocessed data to the beginning
            // and fills the rest with new data

            // copy to the beginning everything that was not yet read (commited)
            std::size_t numBytesProcessed = m_firstUnprocessed - m_buffer.data();
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

            m_firstUnprocessed = m_buffer.data();
        }
    };


}
