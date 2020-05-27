#pragma once

#include "PackedGameHeader.h"

#include "chess/Bcgn.h"
#include "chess/Pgn.h"

#include "external_storage/External.h"

#include "util/MemoryAmount.h"

#include <cstdint>
#include <filesystem>
#include <type_traits>

namespace persistence
{
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

        HeaderEntryLocation addGame(const pgn::UnparsedGame& game);
        HeaderEntryLocation addGame(const pgn::UnparsedGame& game, std::uint16_t plyCount);
        HeaderEntryLocation addGame(const bcgn::UnparsedBcgnGame& game);
        HeaderEntryLocation addGame(const bcgn::UnparsedBcgnGame& game, std::uint16_t plyCount);

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

        HeaderEntryLocation addHeader(const pgn::UnparsedGame& game, std::uint16_t plyCount);
        HeaderEntryLocation addHeader(const pgn::UnparsedGame& game);
        HeaderEntryLocation addHeader(const bcgn::UnparsedBcgnGame& game);
        HeaderEntryLocation addHeader(const bcgn::UnparsedBcgnGame& game, std::uint16_t plyCount);

        HeaderEntryLocation addHeader(const PackedGameHeaderType& entry);

        [[nodiscard]] std::uint64_t nextId() const;
    };

    extern template struct IndexedGameHeaderStorage<PackedGameHeader32>;
    extern template struct IndexedGameHeaderStorage<PackedGameHeader64>;
}
