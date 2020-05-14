#pragma once

#include "chess/Bcgn.h"
#include "chess/Date.h"
#include "chess/Eco.h"
#include "chess/GameClassification.h"
#include "chess/Pgn.h"

#include "external_storage/External.h"

#include "util/MemoryAmount.h"

#include <cstdint>
#include <filesystem>
#include <limits>
#include <string_view>

#include "json/json.hpp"

namespace persistence
{
    struct PackedGameHeader
    {
        static constexpr std::uint16_t unknownPlyCount = std::numeric_limits<std::uint16_t>::max();

        PackedGameHeader() = default;

        PackedGameHeader(ext::Vector<char>& headers, std::size_t offset);

        PackedGameHeader(const pgn::UnparsedGame& game, std::uint32_t gameIdx, std::uint16_t plyCount);

        PackedGameHeader(const pgn::UnparsedGame& game, std::uint32_t gameIdx);

        PackedGameHeader(const bcgn::UnparsedBcgnGame& game, std::uint32_t gameIdx, std::uint16_t plyCount);

        PackedGameHeader(const bcgn::UnparsedBcgnGame& game, std::uint32_t gameIdx);

        [[nodiscard]] const char* data() const;

        [[nodiscard]] std::size_t size() const;

        [[nodiscard]] std::uint32_t gameIdx() const;

        [[nodiscard]] GameResult result() const;

        [[nodiscard]] Date date() const;

        [[nodiscard]] Eco eco() const;

        [[nodiscard]] std::uint16_t plyCount() const;

        [[nodiscard]] std::string_view event() const;

        [[nodiscard]] std::string_view white() const;

        [[nodiscard]] std::string_view black() const;

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

        void fillPackedStrings(std::string_view event, std::string_view white, std::string_view black);
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
        );

        explicit GameHeader(const PackedGameHeader& header);

        GameHeader& operator=(const PackedGameHeader& header);

        [[nodiscard]] std::uint32_t gameIdx() const;

        [[nodiscard]] GameResult result() const;

        [[nodiscard]] Date date() const;

        [[nodiscard]] Eco eco() const;

        [[nodiscard]] std::optional<std::uint16_t> plyCount() const;

        [[nodiscard]] const std::string& event() const;

        [[nodiscard]] const std::string& white() const;

        [[nodiscard]] const std::string& black() const;

        friend void to_json(nlohmann::json& j, const GameHeader& data);

        friend void from_json(const nlohmann::json& j, GameHeader& data);

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

        static constexpr MemoryAmount defaultMemory = MemoryAmount::mebibytes(4);
        static constexpr MemoryAmount minMemory = MemoryAmount::kibibytes(1);

        Header(std::filesystem::path path, MemoryAmount memory = defaultMemory, std::string name = "");

        Header(const Header&) = delete;
        Header(Header&&) noexcept = default;

        Header& operator=(const Header&) = delete;
        Header& operator=(Header&&) noexcept = default;

        [[nodiscard]] HeaderEntryLocation addGame(const pgn::UnparsedGame& game);
        [[nodiscard]] HeaderEntryLocation addGame(const pgn::UnparsedGame& game, std::uint16_t plyCount);
        [[nodiscard]] HeaderEntryLocation addGame(const bcgn::UnparsedBcgnGame& game);
        [[nodiscard]] HeaderEntryLocation addGame(const bcgn::UnparsedBcgnGame& game, std::uint16_t plyCount);

        [[nodiscard]] std::uint32_t nextGameId() const;

        [[nodiscard]] std::uint32_t nextGameOffset() const;

        void flush();

        void clear();

        void replicateTo(const std::filesystem::path& path) const;

        [[nodiscard]] std::vector<PackedGameHeader> queryByOffsets(std::vector<std::uint64_t> offsets);

        [[nodiscard]] std::vector<PackedGameHeader> queryByIndices(std::vector<std::uint32_t> keys);

        [[nodiscard]] std::uint32_t numGames() const;

    private:
        std::string m_name;
        std::filesystem::path m_path;
        std::filesystem::path m_headerPath;
        std::filesystem::path m_indexPath;
        ext::Vector<char> m_header;
        ext::Vector<std::size_t> m_index;

        [[nodiscard]] HeaderEntryLocation addHeader(const pgn::UnparsedGame& game, std::uint16_t plyCount);
        [[nodiscard]] HeaderEntryLocation addHeader(const pgn::UnparsedGame& game);
        [[nodiscard]] HeaderEntryLocation addHeader(const bcgn::UnparsedBcgnGame& game);
        [[nodiscard]] HeaderEntryLocation addHeader(const bcgn::UnparsedBcgnGame& game, std::uint16_t plyCount);

        [[nodiscard]] HeaderEntryLocation addHeader(const PackedGameHeader& entry);

        [[nodiscard]] std::uint32_t nextId() const;
    };
}
