#include "GameHeader.h"

#include "algorithm/Unsort.h"

#include "chess/detail/ParserBits.h"

#include "chess/Date.h"
#include "chess/Eco.h"
#include "chess/Pgn.h"

#include "util/Buffer.h"

#include <cstdint>
#include <filesystem>
#include <limits>
#include <string_view>

#include "json/json.hpp"

namespace persistence
{
    template <typename GameIndexT>
    PackedGameHeader<GameIndexT>::PackedGameHeader(ext::Vector<char>& headers, std::size_t offset) :
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

    template <typename GameIndexT>
    PackedGameHeader<GameIndexT>::PackedGameHeader(const pgn::UnparsedGame& game, GameIndexT gameIdx, std::uint16_t plyCount) :
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

    template <typename GameIndexT>
    PackedGameHeader<GameIndexT>::PackedGameHeader(const pgn::UnparsedGame& game, GameIndexT gameIdx) :
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

    template <typename GameIndexT>
    PackedGameHeader<GameIndexT>::PackedGameHeader(const bcgn::UnparsedBcgnGame& game, GameIndexT gameIdx, std::uint16_t plyCount) :
        m_gameIdx(gameIdx),
        m_plyCount(plyCount)
    {
        auto header = game.gameHeader();
        m_date = header.date();
        m_eco = header.eco();
        std::string_view event = header.event();
        std::string_view white = header.whitePlayer();
        std::string_view black = header.blackPlayer();
        m_result = *header.result();
        fillPackedStrings(event, white, black);
    }

    template <typename GameIndexT>
    PackedGameHeader<GameIndexT>::PackedGameHeader(const bcgn::UnparsedBcgnGame& game, GameIndexT gameIdx) :
        m_gameIdx(gameIdx)
    {
        auto header = game.gameHeader();
        m_date = header.date();
        m_eco = header.eco();
        std::string_view event = header.event();
        std::string_view white = header.whitePlayer();
        std::string_view black = header.blackPlayer();
        m_result = *header.result();
        fillPackedStrings(event, white, black);
    }

    template <typename GameIndexT>
    [[nodiscard]] const char* PackedGameHeader<GameIndexT>::data() const
    {
        return reinterpret_cast<const char*>(this);
    }

    template <typename GameIndexT>
    [[nodiscard]] std::size_t PackedGameHeader<GameIndexT>::size() const
    {
        return m_size;
    }

    template <typename GameIndexT>
    [[nodiscard]] GameIndexT PackedGameHeader<GameIndexT>::gameIdx() const
    {
        return m_gameIdx;
    }

    template <typename GameIndexT>
    [[nodiscard]] GameResult PackedGameHeader<GameIndexT>::result() const
    {
        return m_result;
    }

    template <typename GameIndexT>
    [[nodiscard]] Date PackedGameHeader<GameIndexT>::date() const
    {
        return m_date;
    }

    template <typename GameIndexT>
    [[nodiscard]] Eco PackedGameHeader<GameIndexT>::eco() const
    {
        return m_eco;
    }

    template <typename GameIndexT>
    [[nodiscard]] std::uint16_t PackedGameHeader<GameIndexT>::plyCount() const
    {
        return m_plyCount;
    }

    template <typename GameIndexT>
    [[nodiscard]] std::string_view PackedGameHeader<GameIndexT>::event() const
    {
        const std::uint8_t length = m_packedStrings[0];
        return std::string_view(reinterpret_cast<const char*>(&m_packedStrings[1]), length);
    }

    template <typename GameIndexT>
    [[nodiscard]] std::string_view PackedGameHeader<GameIndexT>::white() const
    {
        const std::uint8_t length0 = m_packedStrings[0];
        const std::uint8_t length = m_packedStrings[length0 + 1];
        return std::string_view(reinterpret_cast<const char*>(&m_packedStrings[length0 + 2]), length);
    }

    template <typename GameIndexT>
    [[nodiscard]] std::string_view PackedGameHeader<GameIndexT>::black() const
    {
        const std::uint8_t length0 = m_packedStrings[0];
        const std::uint8_t length1 = m_packedStrings[length0 + 1];
        const std::uint8_t length = m_packedStrings[length0 + length1 + 2];
        return std::string_view(reinterpret_cast<const char*>(&m_packedStrings[length0 + length1 + 3]), length);
    }

    template <typename GameIndexT>
    void PackedGameHeader<GameIndexT>::fillPackedStrings(std::string_view event, std::string_view white, std::string_view black)
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

    template struct PackedGameHeader<std::uint32_t>;
    template struct PackedGameHeader<std::uint64_t>;

    GameHeader::GameHeader(
        std::uint64_t gameIdx,
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

    [[nodiscard]] std::uint64_t GameHeader::gameIdx() const
    {
        return m_gameIdx;
    }

    [[nodiscard]] GameResult GameHeader::result() const
    {
        return m_result;
    }

    [[nodiscard]] Date GameHeader::date() const
    {
        return m_date;
    }

    [[nodiscard]] Eco GameHeader::eco() const
    {
        return m_eco;
    }

    [[nodiscard]] std::optional<std::uint16_t> GameHeader::plyCount() const
    {
        return m_plyCount;
    }

    [[nodiscard]] const std::string& GameHeader::event() const
    {
        return m_event;
    }

    [[nodiscard]] const std::string& GameHeader::white() const
    {
        return m_white;
    }

    [[nodiscard]] const std::string& GameHeader::black() const
    {
        return m_black;
    }

    void to_json(nlohmann::json& j, const GameHeader& data)
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

    void from_json(const nlohmann::json& j, GameHeader& data)
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
            auto plyCountOpt = parser_bits::tryParseUInt16(j["ply_count"]);
            if (plyCountOpt.has_value())
            {
                data.m_plyCount = *plyCountOpt;
            }
        }

        j["event"].get_to(data.m_event);
        j["white"].get_to(data.m_white);
        j["black"].get_to(data.m_black);
    }

    template <typename PackedGameHeaderT>
    IndexedGameHeaderStorage<PackedGameHeaderT>::IndexedGameHeaderStorage(std::filesystem::path path, MemoryAmount memory, std::string name) :
        // here we use operator, to create directories before we try to
        // create files there
        m_name(std::move(name)),
        m_path((std::filesystem::create_directories(path), std::move(path))),
        m_headerPath(std::move((m_path / headerPath) += m_name)),
        m_indexPath(std::move((m_path / indexPath) += m_name)),
        m_header({ m_headerPath, ext::OutputMode::Append }, util::DoubleBuffer<char>(ext::numObjectsPerBufferUnit<char>(std::max(memory.bytes(), minMemory.bytes()), 4))),
        m_index({ m_indexPath, ext::OutputMode::Append }, util::DoubleBuffer<std::size_t>(ext::numObjectsPerBufferUnit<std::size_t>(std::max(memory.bytes(), minMemory.bytes()), 4)))
    {
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] HeaderEntryLocation IndexedGameHeaderStorage<PackedGameHeaderT>::addGame(const pgn::UnparsedGame& game)
    {
        return addHeader(game);
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] HeaderEntryLocation IndexedGameHeaderStorage<PackedGameHeaderT>::addGame(const pgn::UnparsedGame& game, std::uint16_t plyCount)
    {
        return addHeader(game, plyCount);
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] HeaderEntryLocation IndexedGameHeaderStorage<PackedGameHeaderT>::addGame(const bcgn::UnparsedBcgnGame& game)
    {
        return addHeader(game);
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] HeaderEntryLocation IndexedGameHeaderStorage<PackedGameHeaderT>::addGame(const bcgn::UnparsedBcgnGame& game, std::uint16_t plyCount)
    {
        return addHeader(game, plyCount);
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] std::uint64_t IndexedGameHeaderStorage<PackedGameHeaderT>::nextGameId() const
    {
        return static_cast<std::uint64_t>(m_index.size());
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] std::uint64_t IndexedGameHeaderStorage<PackedGameHeaderT>::nextGameOffset() const
    {
        return static_cast<std::uint64_t>(m_header.size());
    }

    template <typename PackedGameHeaderT>
    void IndexedGameHeaderStorage<PackedGameHeaderT>::flush()
    {
        m_header.flush();
        m_index.flush();
    }

    template <typename PackedGameHeaderT>
    void IndexedGameHeaderStorage<PackedGameHeaderT>::clear()
    {
        m_header.clear();
        m_index.clear();
    }

    template <typename PackedGameHeaderT>
    void IndexedGameHeaderStorage<PackedGameHeaderT>::replicateTo(const std::filesystem::path& path) const
    {
        std::filesystem::path newHeaderPath = path / headerPath;
        newHeaderPath += m_name;
        std::filesystem::path newIndexPath = path / indexPath;
        newIndexPath += m_name;
        std::filesystem::copy_file(m_headerPath, newHeaderPath, std::filesystem::copy_options::overwrite_existing);
        std::filesystem::copy_file(m_indexPath, newIndexPath, std::filesystem::copy_options::overwrite_existing);
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] std::vector<PackedGameHeaderT> IndexedGameHeaderStorage<PackedGameHeaderT>::queryByOffsets(std::vector<std::uint64_t> offsets)
    {
        const std::size_t numKeys = offsets.size();

        auto unsort = reversibleSort(offsets);

        std::vector<PackedGameHeaderT> headers;
        headers.reserve(numKeys);
        for (std::size_t i = 0; i < numKeys; ++i)
        {
            headers.emplace_back(m_header, offsets[i]);
        }

        unsort(headers);

        return headers;
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] std::vector<PackedGameHeaderT> IndexedGameHeaderStorage<PackedGameHeaderT>::queryByIndices(std::vector<std::uint64_t> keys)
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

    template <typename PackedGameHeaderT>
    [[nodiscard]] std::uint64_t IndexedGameHeaderStorage<PackedGameHeaderT>::numGames() const
    {
        return static_cast<std::uint32_t>(m_index.size());
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] HeaderEntryLocation IndexedGameHeaderStorage<PackedGameHeaderT>::addHeader(const pgn::UnparsedGame& game)
    {
        return addHeader(PackedGameHeaderT(game, static_cast<GameIndexType>(nextId())));
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] HeaderEntryLocation IndexedGameHeaderStorage<PackedGameHeaderT>::addHeader(const pgn::UnparsedGame& game, std::uint16_t plyCount)
    {
        return addHeader(PackedGameHeaderT(game, static_cast<GameIndexType>(nextId()), plyCount));
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] HeaderEntryLocation IndexedGameHeaderStorage<PackedGameHeaderT>::addHeader(const bcgn::UnparsedBcgnGame& game)
    {
        return addHeader(PackedGameHeaderT(game, static_cast<GameIndexType>(nextId())));
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] HeaderEntryLocation IndexedGameHeaderStorage<PackedGameHeaderT>::addHeader(const bcgn::UnparsedBcgnGame& game, std::uint16_t plyCount)
    {
        return addHeader(PackedGameHeaderT(game, static_cast<GameIndexType>(nextId()), plyCount));
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] HeaderEntryLocation IndexedGameHeaderStorage<PackedGameHeaderT>::addHeader(const PackedGameHeaderT& entry)
    {
        const std::uint64_t gameIdx = entry.gameIdx();
        const std::uint64_t headerSizeBytes = m_header.size();
        m_header.append(entry.data(), entry.size());
        m_index.emplace_back(headerSizeBytes);
        return { headerSizeBytes, gameIdx };
    }

    template <typename PackedGameHeaderT>
    [[nodiscard]] std::uint64_t IndexedGameHeaderStorage<PackedGameHeaderT>::nextId() const
    {
        return numGames();
    }

    template struct IndexedGameHeaderStorage<PackedGameHeader32>;
    template struct IndexedGameHeaderStorage<PackedGameHeader64>;
}
