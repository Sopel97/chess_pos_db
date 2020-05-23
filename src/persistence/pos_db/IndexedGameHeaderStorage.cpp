#include "IndexedGameHeaderStorage.h"

#include "algorithm/Unsort.h"

namespace persistence
{
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
