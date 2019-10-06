#pragma once

#include "Date.h"
#include "Eco.h"
#include "External.h"
#include "Pgn.h"

#include <cstdint>
#include <filesystem>
#include <limits>
#include <string_view>

#include "lib/json/json.hpp"

namespace persistence
{
    struct PackedGameHeader
    {
        static constexpr std::uint16_t unknownPlyCount = std::numeric_limits<std::uint16_t>::max();

        PackedGameHeader() = default;

        PackedGameHeader(ext::Vector<char>& headers, std::size_t offset) :
            m_size{},
            m_result{},
            m_date{},
            m_eco{},
            m_plyCount{},
            m_packedStrings{}
        {
            // there may be garbage at the end
            // we don't care because we have sizes serialized
            const std::size_t read = headers.read(reinterpret_cast<char*>(this), offset, sizeof(PackedGameHeader));
            ASSERT(m_size <= read);
            (void)read;
        }

        PackedGameHeader(const pgn::UnparsedGame& game, std::uint16_t plyCount) :
            m_plyCount(plyCount)
        {
            std::string_view event;
            std::string_view white;
            std::string_view black;
            std::optional<GameResult> result;
            game.getResultDateEcoEventWhiteBlack(result, m_date, m_eco, event, white, black);
            m_result = *result;
            fillPackedStrings(event, white, black);
        }

        PackedGameHeader(const pgn::UnparsedGame& game)
        {
            std::string_view event;
            std::string_view white;
            std::string_view black;
            std::optional<GameResult> result;
            game.getResultDateEcoEventWhiteBlackPlyCount(result, m_date, m_eco, event, white, black, m_plyCount);
            m_result = *result;
            fillPackedStrings(event, white, black);
        }

        [[nodiscard]] const char* data() const
        {
            return reinterpret_cast<const char*>(this);
        }

        [[nodiscard]] std::size_t size() const
        {
            return m_size;
        }

        [[nodiscard]] GameResult result() const
        {
            return m_result;
        }

        [[nodiscard]] Date date() const
        {
            return m_date;
        }

        [[nodiscard]] Eco eco() const
        {
            return m_eco;
        }

        [[nodiscard]] std::uint16_t plyCount() const
        {
            return m_plyCount;
        }

        [[nodiscard]] std::string_view event() const
        {
            const std::uint8_t length = m_packedStrings[0];
            return std::string_view(reinterpret_cast<const char*>(&m_packedStrings[1]), length);
        }

        [[nodiscard]] std::string_view white() const
        {
            const std::uint8_t length0 = m_packedStrings[0];
            const std::uint8_t length = m_packedStrings[length0 + 1];
            return std::string_view(reinterpret_cast<const char*>(&m_packedStrings[length0 + 2]), length);
        }

        [[nodiscard]] std::string_view black() const
        {
            const std::uint8_t length0 = m_packedStrings[0];
            const std::uint8_t length1 = m_packedStrings[length0 + 1];
            const std::uint8_t length = m_packedStrings[length0 + length1 + 2];
            return std::string_view(reinterpret_cast<const char*>(&m_packedStrings[length0 + length1 + 3]), length);
        }

    private:
        static constexpr std::size_t maxStringLength = 255;
        static constexpr std::size_t numPackedStrings = 3;

        static_assert(maxStringLength < 256); // it's nice to require only one byte for length

        // We just read sizeof(PackedGameHeader), we don't touch anything
        // in packed strings that would be considered 'garbage'
        std::uint16_t m_size;

        GameResult m_result;
        Date m_date;
        Eco m_eco;
        std::uint16_t m_plyCount;

        // strings for event, white, black
        // strings are preceeded with length
        std::uint8_t m_packedStrings[(maxStringLength + 1) * numPackedStrings];

        void fillPackedStrings(std::string_view event, std::string_view white, std::string_view black)
        {
            using namespace std::literals;

            const std::uint8_t eventSize = static_cast<std::uint8_t>(std::min(event.size(), maxStringLength));
            const std::uint8_t whiteSize = static_cast<std::uint8_t>(std::min(white.size(), maxStringLength));
            const std::uint8_t blackSize = static_cast<std::uint8_t>(std::min(black.size(), maxStringLength));

            std::uint8_t i = 0;
            m_packedStrings[i++] = eventSize;
            event.copy(reinterpret_cast<char*>(&m_packedStrings[i]), eventSize);
            i += eventSize;

            m_packedStrings[i++] = whiteSize;
            white.copy(reinterpret_cast<char*>(&m_packedStrings[i]), whiteSize);
            i += whiteSize;

            m_packedStrings[i++] = blackSize;
            black.copy(reinterpret_cast<char*>(&m_packedStrings[i]), blackSize);
            i += blackSize;

            m_size = sizeof(PackedGameHeader) - sizeof(m_packedStrings) + i;
        }
    };
    static_assert(sizeof(PackedGameHeader) == 2 + 2 + 4 + 2 + 2 + 768);

    struct GameHeader
    {
        GameHeader() = default;

        GameHeader(
            GameResult result,
            Date date,
            Eco eco,
            std::uint16_t plyCount,
            std::string event,
            std::string white,
            std::string black
        ) :
            m_result(result),
            m_date(date),
            m_eco(eco),
            m_plyCount(plyCount),
            m_event(std::move(event)),
            m_white(std::move(white)),
            m_black(std::move(black))
        {
        }

        explicit GameHeader(const PackedGameHeader& header) :
            m_result(header.result()),
            m_date(header.date()),
            m_eco(header.eco()),
            m_plyCount(std::nullopt),
            m_event(header.event()),
            m_white(header.white()),
            m_black(header.black())
        {
            if (header.plyCount() != PackedGameHeader::unknownPlyCount)
            {
                m_plyCount = header.plyCount();
            }
        }

        [[nodiscard]] GameResult result() const
        {
            return m_result;
        }

        [[nodiscard]] Date date() const
        {
            return m_date;
        }

        [[nodiscard]] Eco eco() const
        {
            return m_eco;
        }

        [[nodiscard]] std::optional<std::uint16_t> plyCount() const
        {
            return m_plyCount;
        }

        [[nodiscard]] const std::string& event() const
        {
            return m_event;
        }

        [[nodiscard]] const std::string& white() const
        {
            return m_white;
        }

        [[nodiscard]] const std::string& black() const
        {
            return m_black;
        }

        friend void to_json(nlohmann::json& j, const GameHeader& data)
        {
            j = nlohmann::json{
                { "result", toString(GameResultPgnFormat{}, data.m_result) },
                { "date", data.m_date.toString() },
                { "eco", data.m_eco.toString() },
                { "event", data.m_event },
                { "white", data.m_white },
                { "black", data.m_black }
            };

            if (data.m_plyCount.has_value())
            {
                j["ply_count"] = *data.m_plyCount;
            }
        }

    private:
        GameResult m_result;
        Date m_date;
        Eco m_eco;
        std::optional<std::uint16_t> m_plyCount;
        std::string m_event;
        std::string m_white;
        std::string m_black;
    };

    struct HeaderEntryLocation
    {
        std::uint64_t offset;
        std::uint32_t index;
    };

    struct Header
    {
        static inline const std::filesystem::path headerPath = "header";
        static inline const std::filesystem::path indexPath = "index";

        static constexpr std::size_t defaultMemory = 4 * 1024 * 1024;
        static constexpr std::size_t minMemory = 1024;

        Header(std::filesystem::path path, std::size_t memory = defaultMemory) :
            // here we use operator, to create directories before we try to
            // create files there
            m_path((std::filesystem::create_directories(path), std::move(path))),
            m_header({ m_path / headerPath, ext::OutputMode::Append }, ext::DoubleBuffer<char>(ext::numObjectsPerBufferUnit<char>(std::max(memory, minMemory), 4))),
            m_index({ m_path / indexPath, ext::OutputMode::Append }, ext::DoubleBuffer<std::size_t>(ext::numObjectsPerBufferUnit<std::size_t>(std::max(memory, minMemory), 4)))
        {
        }

        [[nodiscard]] HeaderEntryLocation addGame(const pgn::UnparsedGame& game)
        {
            return addHeader(PackedGameHeader(game));
        }

        [[nodiscard]] HeaderEntryLocation addGameNoLock(const pgn::UnparsedGame& game)
        {
            return addHeaderNoLock(PackedGameHeader(game));
        }

        [[nodiscard]] HeaderEntryLocation addGame(const pgn::UnparsedGame& game, std::uint16_t plyCount)
        {
            return addHeader(PackedGameHeader(game, plyCount));
        }

        [[nodiscard]] HeaderEntryLocation addGameNoLock(const pgn::UnparsedGame& game, std::uint16_t plyCount)
        {
            return addHeaderNoLock(PackedGameHeader(game, plyCount));
        }

        [[nodiscard]] std::uint32_t nextGameId() const
        {
            return static_cast<std::uint32_t>(m_index.size());
        }

        [[nodiscard]] std::uint32_t nextGameOffset() const
        {
            return static_cast<std::uint32_t>(m_header.size());
        }

        void flush()
        {
            m_header.flush();
            m_index.flush();
        }

        void clear()
        {
            m_header.clear();
            m_index.clear();
        }

        void replicateTo(const std::filesystem::path& path) const
        {
            std::filesystem::copy_file(m_path / headerPath, path / headerPath, std::filesystem::copy_options::overwrite_existing);
            std::filesystem::copy_file(m_path / indexPath, path / indexPath, std::filesystem::copy_options::overwrite_existing);
        }

        [[nodiscard]] std::vector<PackedGameHeader> query(const std::vector<std::uint32_t>& keys)
        {
            // TODO: think about a good abstraction for iterating in a sorted order
            //       with ability to retrieve original indices

            const std::size_t numKeys = keys.size();

            std::vector<std::uint32_t> orderedKeys;
            std::vector<std::size_t> originalIds;
            std::vector<std::pair<std::uint32_t, std::size_t>> compound;
            orderedKeys.reserve(numKeys);
            originalIds.reserve(numKeys);
            compound.reserve(numKeys);
            for (std::size_t i = 0; i < numKeys; ++i)
            {
                compound.emplace_back(keys[i], i);
            }
            std::sort(compound.begin(), compound.end(), [](auto&& lhs, auto&& rhs) { return lhs.first < rhs.first; });
            for (auto&& [key, id] : compound)
            {
                orderedKeys.emplace_back(key);
                originalIds.emplace_back(id);
            }

            std::vector<std::size_t> offsets;
            offsets.reserve(numKeys);
            for (auto& key : orderedKeys)
            {
                offsets.emplace_back(m_index[key]);
            }

            std::vector<PackedGameHeader> headers;
            headers.resize(numKeys);
            for (std::size_t i = 0; i < numKeys; ++i)
            {
                headers[originalIds[i]] = PackedGameHeader(m_header, offsets[i]);
            }

            return headers;
        }

    private:
        std::filesystem::path m_path;
        ext::Vector<char> m_header;
        ext::Vector<std::size_t> m_index;

        std::mutex m_mutex;

        // returns the index of the header (not the address)
        [[nodiscard]] HeaderEntryLocation addHeaderNoLock(const PackedGameHeader& entry)
        {
            const std::uint64_t headerSizeBytes = m_header.size();
            m_header.append(entry.data(), entry.size());
            m_index.emplace_back(headerSizeBytes);
            return { headerSizeBytes, static_cast<std::uint32_t>(m_index.size() - 1u) };
        }

        // returns the index of the header (not the address)
        [[nodiscard]] HeaderEntryLocation addHeader(const PackedGameHeader& entry)
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            return addHeaderNoLock(entry);
        }
    };
}
