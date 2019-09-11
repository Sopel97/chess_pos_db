#pragma once

#include "EnumMap.h"
#include "External.h"
#include "GameClassification.h"
#include "Pgn.h"
#include "PositionSignature.h"
#include "StorageHeader.h"

#include <array>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace persistence
{
    struct LocalStorageFormatEntry
    {
        LocalStorageFormatEntry() = default;

        LocalStorageFormatEntry(const Position& pos, std::uint32_t gameIdx) :
            m_positionSignature(pos),
            m_gameIdx(gameIdx)
        {
        }

        LocalStorageFormatEntry(const LocalStorageFormatEntry&) = default;
        LocalStorageFormatEntry(LocalStorageFormatEntry&&) = default;
        LocalStorageFormatEntry& operator=(const LocalStorageFormatEntry&) = default;
        LocalStorageFormatEntry& operator=(LocalStorageFormatEntry&&) = default;

        [[nodiscard]] friend bool operator<(const LocalStorageFormatEntry& lhs, const LocalStorageFormatEntry& rhs) noexcept
        {
            return lhs.m_positionSignature < rhs.m_positionSignature;
        }

        [[nodiscard]] std::uint32_t hashMod(std::uint32_t d) const
        {
            return m_positionSignature.hash()[0] % d;
        }

    private:
        PositionSignature m_positionSignature;
        std::uint32_t m_gameIdx;
    };

    static_assert(sizeof(LocalStorageFormatEntry) == 20);

    struct LocalStorageFormatFile
    {
        LocalStorageFormatFile(std::filesystem::path path) :
            m_entries(std::move(path)),
            m_id(std::stoi(m_entries.path().filename().string()))
        {
        }

        LocalStorageFormatFile(ext::ImmutableSpan<LocalStorageFormatEntry>&& entries) :
            m_entries(std::move(entries)),
            m_id(std::stoi(m_entries.path().filename().string()))
        {
        }

        [[nodiscard]] friend bool operator<(const LocalStorageFormatFile& lhs, const LocalStorageFormatFile& rhs) noexcept
        {
            return lhs.m_id < rhs.m_id;
        }

        [[nodiscard]] int id() const
        {
            return m_id;
        }

    private:
        ext::ImmutableSpan<LocalStorageFormatEntry> m_entries;
        int m_id;
    };

    struct LocalStorageFormatPartition
    {
        LocalStorageFormatPartition() = default;

        LocalStorageFormatPartition(std::filesystem::path path)
        {
            setPath(std::move(path));
        }

        void setPath(std::filesystem::path path)
        {
            m_path = std::move(path);
            std::filesystem::create_directories(m_path);

            discoverFiles();
        }

        // data has to be sorted in ascending order
        void store(const LocalStorageFormatEntry* data, std::size_t count)
        {
            ASSERT(!m_path.empty());

            ext::BinaryOutputFile outFile(nextPath());
            ext::withBackInserterUnbuffered<LocalStorageFormatEntry>(outFile, [&](ext::BackInserter<LocalStorageFormatEntry>& out) {
                out.append(data, count);
            });

            m_files.emplace_back(outFile.seal());
        }

        // entries have to be sorted in ascending order
        void store(const std::vector<LocalStorageFormatEntry>& entries)
        {
            store(entries.data(), entries.size());
        }

    private:
        std::filesystem::path m_path;
        std::vector<LocalStorageFormatFile> m_files;

        [[nodiscard]] int nextId() const
        {
            if (m_files.empty())
            {
                return 0;
            }
            else
            {
                return m_files.back().id() + 1;
            }
        }

        [[nodiscard]] std::filesystem::path nextPath() const
        {
            return m_path / std::to_string(nextId());
        }

        void discoverFiles()
        {
            m_files.clear();

            for (auto& entry : std::filesystem::directory_iterator(m_path))
            {
                if (!entry.is_regular_file())
                {
                    continue;
                }

                m_files.emplace_back(entry.path());
            }

            std::sort(m_files.begin(), m_files.end());
        }
    };

    struct LocalStorageFormat
    {
    private:
        static inline const std::string m_name = "local";

        static constexpr int m_numPartitionsByHashModulo = 4;

        template <typename T>
        using PerDirectory = EnumMap2<GameLevel, GameResult, std::array<T, m_numPartitionsByHashModulo>>;

        template <typename T>
        using PerGameLevel = EnumMap<GameResult, std::array<T, m_numPartitionsByHashModulo>>;

        using FilesStorageType = PerDirectory<LocalStorageFormatPartition>;

        using DirectoryPathsStorageType = PerDirectory<std::filesystem::path>;

        static inline const EnumMap<GameLevel, std::filesystem::path> m_pathByGameLevel = {
            "human",
            "engine",
            "server"
        };

        static inline const EnumMap<GameResult, std::filesystem::path> m_pathByGameResult = {
            "w",
            "l",
            "d"
        };

        static constexpr int m_totalNumDirectories =
            cardinality<GameLevel>()
            * cardinality<GameResult>()
            * m_numPartitionsByHashModulo;

    public:
        struct ImportStats
        {
            std::size_t numGames = 0;
            std::size_t numSkippedGames = 0; // We skip games with an unknown result.
            std::size_t numPositions = 0;
        };

        LocalStorageFormat(std::filesystem::path path) :
            m_path(path),
            m_header(path, 1'000'000) // TODO: pass memory to here
        {
            initializePartitions();
        }

        [[nodiscard]] const std::string& name() const
        {
            return m_name;
        }

        // returns number of positions added
        ImportStats importPgns(const std::vector<std::filesystem::path>& paths, GameLevel level, std::size_t memory)
        {
            const std::size_t bucketSize = 
                ext::numObjectsPerBufferUnit<LocalStorageFormatEntry>(
                    memory, 
                    cardinality<GameResult>() * m_numPartitionsByHashModulo
                );

            PerGameLevel<std::vector<LocalStorageFormatEntry>> entries;
            forEach(entries, [bucketSize](auto& bucket, GameResult result, int idx) {
                bucket.reserve(bucketSize);
            });

            ImportStats stats{};
            for (auto& path : paths)
            {
                pgn::LazyPgnFileReader fr(path);
                if (!fr.isOpen())
                {
                    std::cout << "Failed to open file " << path << '\n';
                    break;
                }

                for (auto& game : fr)
                {
                    const pgn::GameResult pgnResult = game.result();
                    if (pgnResult == pgn::GameResult::Unknown)
                    {
                        stats.numSkippedGames += 1;
                        continue;
                    }

                    const std::uint32_t gameIdx = m_header.addGame(game);
                    stats.numGames += 1;

                    const GameResult result = convertResult(pgnResult);

                    for (auto& pos : game.positions())
                    {
                        LocalStorageFormatEntry entry(pos, gameIdx);
                        const int partitionIdx = entry.hashMod(m_numPartitionsByHashModulo);
                        auto& bucket = entries[result][partitionIdx];
                        bucket.emplace_back(entry);
                        stats.numPositions += 1;

                        if (bucket.size() == bucketSize)
                        {
                            store(bucket, level, result, partitionIdx);
                            bucket.clear();
                        }
                    }
                }
            }

            forEach(entries, [this, level](auto& bucket, GameResult result, int idx) {
                store(bucket, level, result, idx);
             });

            return stats;
        }

        ImportStats importPgn(const std::filesystem::path& path, GameLevel level, std::size_t memory)
        {
            return importPgns({ path }, level, memory);
        }

    private:
        std::filesystem::path m_path;

        Header m_header;
        FilesStorageType m_files;

        // this is nontrivial to do in the constructor initializer list
        void initializePartitions()
        {
            for (const auto& level : values<GameLevel>())
            {
                const std::filesystem::path levelPath = m_pathByGameLevel[level];
                for (const auto& result : values<GameResult>())
                {
                    const std::filesystem::path resultPath = levelPath / m_pathByGameResult[result];
                    for (int partitionIdx = 0; partitionIdx < m_numPartitionsByHashModulo; ++partitionIdx)
                    {
                        const std::filesystem::path partitionPath = resultPath / std::to_string(partitionIdx);
                        m_files[level][result][partitionIdx].setPath(m_path / partitionPath);
                    }
                }
            }
        }

        void store(std::vector<LocalStorageFormatEntry>& entries, GameLevel level, GameResult result, int partitionIdx)
        {
            std::stable_sort(entries.begin(), entries.end());
            m_files[level][result][partitionIdx].store(entries);
        }

        template <typename T, typename FuncT>
        static void forEach(PerDirectory<T>& data, FuncT&& f)
        {
            for (const auto& level : values<GameLevel>())
            {
                for (const auto& result : values<GameResult>())
                {
                    for (int partitionIdx = 0; partitionIdx < m_numPartitionsByHashModulo; ++partitionIdx)
                    {
                        f(data[level][result][partitionIdx], level, result, partitionIdx);
                    }
                }
            }
        }

        template <typename T, typename FuncT>
        static void forEach(PerGameLevel<T>& data, FuncT&& f)
        {
            for (const auto& result : values<GameResult>())
            {
                for (int partitionIdx = 0; partitionIdx < m_numPartitionsByHashModulo; ++partitionIdx)
                {
                    f(data[result][partitionIdx], result, partitionIdx);
                }
            }
        }

        [[nodiscard]] static inline GameResult convertResult(pgn::GameResult res)
        {
            ASSERT(res != pgn::GameResult::Unknown);

            return static_cast<GameResult>(static_cast<int>(res));
        }
    };
}