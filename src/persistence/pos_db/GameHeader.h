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
    template <typename GameIndexT>
    struct PackedGameHeader
    {
        static_assert(std::is_unsigned_v<GameIndexT>);

        using GameIndexType = GameIndexT;

        static constexpr std::uint16_t unknownPlyCount = std::numeric_limits<std::uint16_t>::max();

        PackedGameHeader() = default;

        PackedGameHeader(ext::Vector<char>& headers, std::size_t offset);

        PackedGameHeader(const pgn::UnparsedGame& game, GameIndexType gameIdx, std::uint16_t plyCount);

        PackedGameHeader(const pgn::UnparsedGame& game, GameIndexType gameIdx);

        PackedGameHeader(const bcgn::UnparsedBcgnGame& game, GameIndexType gameIdx, std::uint16_t plyCount);

        PackedGameHeader(const bcgn::UnparsedBcgnGame& game, GameIndexType gameIdx);

        [[nodiscard]] const char* data() const;

        [[nodiscard]] std::size_t size() const;

        [[nodiscard]] GameIndexType gameIdx() const;

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

        GameIndexType m_gameIdx;

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

    using PackedGameHeader32 = PackedGameHeader<std::uint32_t>;
    using PackedGameHeader64 = PackedGameHeader<std::uint64_t>;

    static_assert(sizeof(PackedGameHeader32) == 4 + 2 + 2 + 4 + 2 + 2 + 768);
    static_assert(sizeof(PackedGameHeader64) == 8 + 2 + 2 + 4 + 2 + 2 + 768 + 4 /* padding */);

    static_assert(std::is_trivially_copyable_v<PackedGameHeader32>);
    static_assert(std::is_trivially_copyable_v<PackedGameHeader64>);

    extern template struct PackedGameHeader<std::uint32_t>;
    extern template struct PackedGameHeader<std::uint64_t>;

    struct GameHeader
    {
        GameHeader() = default;

        GameHeader(
            std::uint64_t gameIdx,
            GameResult result,
            Date date,
            Eco eco,
            std::uint16_t plyCount,
            std::string event,
            std::string white,
            std::string black
        );

        GameHeader(const GameHeader& other) = default;
        GameHeader(GameHeader&& other) = default;

        GameHeader& operator=(const GameHeader& other) = default;
        GameHeader& operator=(GameHeader&& other) = default;

        template <typename PackedGameHeaderT>
        explicit GameHeader(const PackedGameHeaderT& header) :
            m_gameIdx(header.gameIdx()),
            m_result(header.result()),
            m_date(header.date()),
            m_eco(header.eco()),
            m_plyCount(std::nullopt),
            m_event(header.event()),
            m_white(header.white()),
            m_black(header.black())
        {
            if (header.plyCount() != PackedGameHeaderT::unknownPlyCount)
            {
                m_plyCount = header.plyCount();
            }
        }

        template <typename PackedGameHeaderT>
        GameHeader& operator=(const PackedGameHeaderT& header)
        {
            m_gameIdx = header.gameIdx();
            m_result = header.result();
            m_date = header.date();
            m_eco = header.eco();
            m_plyCount = header.plyCount();
            if (m_plyCount == PackedGameHeaderT::unknownPlyCount)
            {
                m_plyCount.reset();
            }
            m_event = header.event();
            m_white = header.white();
            m_black = header.black();

            return *this;
        }

        [[nodiscard]] std::uint64_t gameIdx() const;

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
        std::uint64_t m_gameIdx;
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
        std::uint64_t index;
    };

    template <typename PackedGameHeaderT>
    struct IndexedGameHeaderStorage
    {
        using GameIndexType = typename PackedGameHeaderT::GameIndexType;
        using PackedGameHeaderType = PackedGameHeaderT;

        static inline const std::filesystem::path headerPath = "header";
        static inline const std::filesystem::path indexPath = "index";

        static constexpr MemoryAmount defaultMemory = MemoryAmount::mebibytes(4);
        static constexpr MemoryAmount minMemory = MemoryAmount::kibibytes(1);

        IndexedGameHeaderStorage(std::filesystem::path path, MemoryAmount memory = defaultMemory, std::string name = "");

        IndexedGameHeaderStorage(const IndexedGameHeaderStorage&) = delete;
        IndexedGameHeaderStorage(IndexedGameHeaderStorage&&) noexcept = default;

        IndexedGameHeaderStorage& operator=(const IndexedGameHeaderStorage&) = delete;
        IndexedGameHeaderStorage& operator=(IndexedGameHeaderStorage&&) noexcept = default;

        [[nodiscard]] HeaderEntryLocation addGame(const pgn::UnparsedGame& game);
        [[nodiscard]] HeaderEntryLocation addGame(const pgn::UnparsedGame& game, std::uint16_t plyCount);
        [[nodiscard]] HeaderEntryLocation addGame(const bcgn::UnparsedBcgnGame& game);
        [[nodiscard]] HeaderEntryLocation addGame(const bcgn::UnparsedBcgnGame& game, std::uint16_t plyCount);

        [[nodiscard]] std::uint64_t nextGameId() const;

        [[nodiscard]] std::uint64_t nextGameOffset() const;

        void flush();

        void clear();

        void replicateTo(const std::filesystem::path& path) const;

        [[nodiscard]] std::vector<PackedGameHeaderType> queryByOffsets(std::vector<std::uint64_t> offsets);

        [[nodiscard]] std::vector<PackedGameHeaderType> queryByIndices(std::vector<std::uint64_t> keys);

        [[nodiscard]] std::uint64_t numGames() const;

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

        [[nodiscard]] HeaderEntryLocation addHeader(const PackedGameHeaderType& entry);

        [[nodiscard]] std::uint64_t nextId() const;
    };

    extern template struct IndexedGameHeaderStorage<PackedGameHeader32>;
    extern template struct IndexedGameHeaderStorage<PackedGameHeader64>;
}
