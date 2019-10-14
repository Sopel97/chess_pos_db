#pragma once

#include "Date.h"
#include "Eco.h"
#include "External.h"
#include "Pgn.h"
#include "Unsort.h"

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
            m_gameIdx{},
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

        PackedGameHeader(const pgn::UnparsedGame& game, std::uint32_t gameIdx, std::uint16_t plyCount) :
            m_gameIdx(gameIdx),
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

        PackedGameHeader(const pgn::UnparsedGame& game, std::uint32_t gameIdx) :
            m_gameIdx(gameIdx)
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

        [[nodiscard]] std::uint32_t gameIdx() const
        {
            return m_gameIdx;
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

        std::uint32_t m_gameIdx;

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
    static_assert(sizeof(PackedGameHeader) == 4 + 2 + 2 + 4 + 2 + 2 + 768);

    struct GameHeader
    {
        GameHeader() = default;

        GameHeader(
            std::uint32_t gameIdx,
            GameResult result,
            Date date,
            Eco eco,
            std::uint16_t plyCount,
            std::string event,
            std::string white,
            std::string black
        ) :
            m_gameIdx(gameIdx),
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
            m_gameIdx(header.gameIdx()),
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

        GameHeader& operator=(const PackedGameHeader& header)
        {
            m_gameIdx = header.gameIdx();
            m_result = header.result();
            m_date = header.date();
            m_eco = header.eco();
            m_plyCount = header.plyCount();
            if (m_plyCount == PackedGameHeader::unknownPlyCount)
            {
                m_plyCount.reset();
            }
            m_event = header.event();
            m_white = header.white();
            m_black = header.black();

            return *this;
        }

        [[nodiscard]] std::uint32_t gameIdx() const
        {
            return m_gameIdx;
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
                { "game_id", data.m_gameIdx },
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

        friend void from_json(const nlohmann::json& j, GameHeader& data)
        {
            j["game_id"].get_to(data.m_gameIdx);

            auto resultOpt = fromString<GameResult>(GameResultPgnFormat{}, j["result"]);
            if (resultOpt.has_value())
            {
                // TODO: throw otherwise?
                data.m_result = *resultOpt;
            }

            auto dateOpt = Date::tryParse(j["date"]);
            if (dateOpt.has_value())
            {
                data.m_date = *dateOpt;
            }

            auto ecoOpt = Eco::tryParse(j["eco"]);
            if (ecoOpt.has_value())
            {
                data.m_eco = *ecoOpt;
            }

            if (j.contains("ply_count"))
            {
                auto plyCountOpt = detail::tryParseUInt16(j["ply_count"]);
                if (plyCountOpt.has_value())
                {
                    data.m_plyCount = *plyCountOpt;
                }
            }

            j["event"].get_to(data.m_event);
            j["white"].get_to(data.m_white);
            j["black"].get_to(data.m_black);
        }

    private:
        std::uint32_t m_gameIdx;
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

        Header(std::filesystem::path path, std::size_t memory = defaultMemory, std::string name = "") :
            // here we use operator, to create directories before we try to
            // create files there
            m_name(std::move(name)),
            m_path((std::filesystem::create_directories(path), std::move(path))),
            m_headerPath(std::move((m_path / headerPath) += m_name)),
            m_indexPath(std::move((m_path / indexPath) += m_name)),
            m_header({ m_headerPath, ext::OutputMode::Append }, ext::DoubleBuffer<char>(ext::numObjectsPerBufferUnit<char>(std::max(memory, minMemory), 4))),
            m_index({ m_indexPath, ext::OutputMode::Append }, ext::DoubleBuffer<std::size_t>(ext::numObjectsPerBufferUnit<std::size_t>(std::max(memory, minMemory), 4)))
        {
        }

        [[nodiscard]] HeaderEntryLocation addGame(const pgn::UnparsedGame& game)
        {
            return addHeader(game);
        }

        [[nodiscard]] HeaderEntryLocation addGameNoLock(const pgn::UnparsedGame& game)
        {
            return addHeaderNoLock(game);
        }

        [[nodiscard]] HeaderEntryLocation addGame(const pgn::UnparsedGame& game, std::uint16_t plyCount)
        {
            return addHeader(game, plyCount);
        }

        [[nodiscard]] HeaderEntryLocation addGameNoLock(const pgn::UnparsedGame& game, std::uint16_t plyCount)
        {
            return addHeaderNoLock(game, plyCount);
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
            std::filesystem::path newHeaderPath = path / headerPath;
            newHeaderPath += m_name;
            std::filesystem::path newIndexPath = path / indexPath;
            newIndexPath += m_name;
            std::filesystem::copy_file(m_headerPath, newHeaderPath, std::filesystem::copy_options::overwrite_existing);
            std::filesystem::copy_file(m_indexPath, newIndexPath, std::filesystem::copy_options::overwrite_existing);
        }

        [[nodiscard]] std::vector<PackedGameHeader> queryByOffsets(std::vector<std::uint64_t> offsets)
        {
            const std::size_t numKeys = offsets.size();

            auto unsort = reversibleSort(offsets);

            std::vector<PackedGameHeader> headers;
            headers.reserve(numKeys);
            for (std::size_t i = 0; i < numKeys; ++i)
            {
                headers.emplace_back(m_header, offsets[i]);
            }

            unsort(headers);

            return headers;
        }

        [[nodiscard]] std::vector<PackedGameHeader> queryByIndices(std::vector<std::uint32_t> keys)
        {
            const std::size_t numKeys = keys.size();

            auto unsort = reversibleSort(keys);

            std::vector<std::size_t> offsets;
            offsets.reserve(numKeys);
            for (auto& key : keys)
            {
                offsets.emplace_back(m_index[key]);
            }

            unsort(offsets);

            return queryByOffsets(offsets);
        }

        // returns the index of the header (not the address)
        [[nodiscard]] HeaderEntryLocation addHeaderNoLock(const PackedGameHeader& header)
        {
            const std::uint32_t gameIdx = static_cast<std::uint32_t>(m_index.size());
            const std::uint64_t headerSizeBytes = m_header.size();
            m_header.append(header.data(), header.size());
            m_index.emplace_back(headerSizeBytes);
            return { headerSizeBytes, gameIdx };
        }

        // returns the index of the header (not the address)
        [[nodiscard]] HeaderEntryLocation addHeader(const PackedGameHeader& header)
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            return addHeaderNoLock(header);
        }

        [[nodiscard]] std::uint32_t numGames() const
        {
            return static_cast<std::uint32_t>(m_index.size());
        }

    private:
        std::string m_name;
        std::filesystem::path m_path;
        std::filesystem::path m_headerPath;
        std::filesystem::path m_indexPath;
        ext::Vector<char> m_header;
        ext::Vector<std::size_t> m_index;

        std::mutex m_mutex;

        // returns the index of the header (not the address)
        [[nodiscard]] HeaderEntryLocation addHeaderNoLock(const pgn::UnparsedGame& game, std::uint16_t plyCount)
        {
            const std::uint32_t gameIdx = static_cast<std::uint32_t>(m_index.size());
            const auto entry = PackedGameHeader(game, gameIdx, plyCount);

            const std::uint64_t headerSizeBytes = m_header.size();
            m_header.append(entry.data(), entry.size());
            m_index.emplace_back(headerSizeBytes);
            return { headerSizeBytes, gameIdx };
        }

        // returns the index of the header (not the address)
        [[nodiscard]] HeaderEntryLocation addHeaderNoLock(const pgn::UnparsedGame& game)
        {
            const std::uint32_t gameIdx = static_cast<std::uint32_t>(m_index.size());
            const auto entry = PackedGameHeader(game, gameIdx);

            const std::uint64_t headerSizeBytes = m_header.size();
            m_header.append(entry.data(), entry.size());
            m_index.emplace_back(headerSizeBytes);
            return { headerSizeBytes, gameIdx };
        }

        // returns the index of the header (not the address)
        [[nodiscard]] HeaderEntryLocation addHeader(const pgn::UnparsedGame& game, std::uint16_t plyCount)
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            return addHeaderNoLock(game, plyCount);
        }

        // returns the index of the header (not the address)
        [[nodiscard]] HeaderEntryLocation addHeader(const pgn::UnparsedGame& game)
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            return addHeaderNoLock(game);
        }
    };
}
