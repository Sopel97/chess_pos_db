#pragma once

#include "EnumMap.h"
#include "External.h"
#include "GameClassification.h"
#include "PositionSignature.h"

#include <array>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace persistence
{
    struct LocalStorageFormatEntry
    {
        LocalStorageFormatEntry(const Position& pos, std::uint32_t gameIdx) :
            m_positionSignature(pos),
            m_gameIdx(gameIdx)
        {
        }

    private:
        PositionSignature m_positionSignature;
        std::uint32_t m_gameIdx;
    };

    static_assert(sizeof(LocalStorageFormatEntry) == 20);

    struct LocalStorageFormatFile
    {
        LocalStorageFormatFile(std::filesystem::path path) :
            m_entries(std::move(path))
        {
        }

    private:
        ext::ImmutableSpan<LocalStorageFormatEntry> m_entries;
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

    private:
        std::filesystem::path m_path;
        std::vector<LocalStorageFormatFile> m_files;

        void discoverFiles()
        {
            m_files.clear();

            // TODO: file discovery
        }
    };

    struct LocalStorageFormat
    {
    private:
        static inline const std::string m_name = "local";

        static constexpr int m_numPartitionsByHashModulo = 16;

        template <typename T>
        using PerDirectory = EnumMap2<GameLevel, GameResult, std::array<T, 16>>;

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
        LocalStorageFormat(std::filesystem::path path) :
            m_path(path)
        {
            initializePartitions();
        }

        const std::string& name() const
        {
            return m_name;
        }

    private:
        std::filesystem::path m_path;

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
    };
}