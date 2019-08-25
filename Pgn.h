#pragma once

#include <vector>
#include <memory>
#include <optional>
#include <cstdio>
#include <cstdint>
#include <filesystem>

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

    private:
        UnparsedRegion m_tagRegion;
        UnparsedRegion m_moveRegion;
    };

    struct LazyPgnFileReader
    {
        // currently bufferSize must be bigger than the maximum number of bytes taken by a single game
        // TODO: resize buffer when didn't process anything
        static constexpr std::size_t bufferSize = 1024 * 32;

        LazyPgnFileReader(std::filesystem::path path) :
            m_file(nullptr, &std::fclose),
            m_buffer(bufferSize + 1), // one spot for '\0',
            m_firstUnprocessed(m_buffer.data())
        {
            auto strPath = path.string();
            m_file.reset(std::fopen(strPath.c_str(), "r"));

            const std::size_t numBytesRead = std::fread(m_buffer.data(), 1, bufferSize, m_file.get());
            m_buffer[numBytesRead] = '\0';
        }

        std::optional<UnparsedGame> nextGame()
        {
            while(m_buffer.front() != '\0')
            {
                // try find region bounds
                const char* tagend = std::strstr(m_firstUnprocessed, tagRegionEndSequence);
                if (tagend == nullptr)
                {
                    // fetch more data if needed
                    refill();
                    continue;
                }

                const char* moveend = std::strstr(tagend + tagRegionEndSequenceLength, moveRegionEndSequence);
                if (moveend == nullptr)
                {
                    refill();
                    continue;
                }

                // we only extract one game at the time

                UnparsedGame game(
                    { m_firstUnprocessed, tagend }, 
                    { tagend + tagRegionEndSequenceLength, moveend }
                );

                m_firstUnprocessed = moveend + moveRegionEndSequenceLength;

                return game;
            }

            return std::nullopt;
        }

    private:
        std::unique_ptr<FILE, decltype(&std::fclose)> m_file;
        std::vector<char> m_buffer;
        const char* m_firstUnprocessed; // we don't go back to anything before that

        void refill()
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
