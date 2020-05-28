#pragma once

#include "Query.h"

#include "chess/GameClassification.h"

#include "enum/EnumArray.h"

#include "json/json.hpp"

#include <cstdint>
#include <execution>
#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace persistence
{
    struct SingleGameLevelDatabaseStats
    {
        std::size_t numGames = 0;
        std::size_t numPositions = 0;
        std::size_t totalWhiteElo = 0;
        std::size_t totalBlackElo = 0;
        std::size_t numGamesWithElo = 0;
        std::size_t numGamesWithDate = 0;
        std::uint16_t minElo = 0;
        std::uint16_t maxElo = 0;
        Date minDate{};
        Date maxDate{};

        SingleGameLevelDatabaseStats& operator+=(const SingleGameLevelDatabaseStats& rhs);

        friend void to_json(nlohmann::json& j, const SingleGameLevelDatabaseStats& stats);
        friend void from_json(const nlohmann::json& j, SingleGameLevelDatabaseStats& stats);
    };

    struct DatabaseStats
    {
        [[nodiscard]] SingleGameLevelDatabaseStats total() const;

        const SingleGameLevelDatabaseStats& operator[](GameLevel level) const;
        SingleGameLevelDatabaseStats& operator[](GameLevel level);

        void add(SingleGameLevelDatabaseStats stats, GameLevel level);

        friend void to_json(nlohmann::json& j, const DatabaseStats& stats);
        friend void from_json(const nlohmann::json& j, DatabaseStats& stats);

    private:
        EnumArray<GameLevel, SingleGameLevelDatabaseStats> m_statsByLevel;
    };

    struct SingleGameLevelImportStats : SingleGameLevelDatabaseStats
    {
        std::size_t numSkippedGames = 0; // We skip games with an unknown result.

        SingleGameLevelImportStats& operator+=(const SingleGameLevelImportStats& rhs);

        friend void to_json(nlohmann::json& j, const SingleGameLevelImportStats& stats);
        friend void from_json(const nlohmann::json& j, SingleGameLevelImportStats& stats);
    };

    struct ImportStats
    {
        ImportStats() = default;
        ImportStats(SingleGameLevelImportStats stats, GameLevel level);

        void add(SingleGameLevelImportStats stats, GameLevel level);

        [[nodiscard]] SingleGameLevelImportStats total() const;

        const SingleGameLevelImportStats& operator[](GameLevel level) const;
        SingleGameLevelImportStats& operator[](GameLevel level);

        ImportStats& operator+=(const ImportStats& rhs);

        friend void to_json(nlohmann::json& j, const ImportStats& stats);
        friend void from_json(const nlohmann::json& j, ImportStats& stats);

    private:
        EnumArray<GameLevel, SingleGameLevelImportStats> m_statsByLevel;
    };

    enum struct ImportableFileType
    {
        Pgn,
        Bcgn,
        Unknown
    };

    [[nodiscard]] const std::string& importableFileTypeExtension(ImportableFileType type);
    [[nodiscard]] ImportableFileType importableFileTypeFromPath(const std::filesystem::path path);

    enum struct MergeMode
    {
        None,
        Consecutive,
        Any
    };

    struct MergableFile
    {
        std::string name;
        std::size_t sizeBytes;

        friend void to_json(nlohmann::json& j, const MergableFile& file);
    };

    using ImportableFilePath = std::filesystem::path;
    using ImportableFilePaths = std::vector<std::filesystem::path>;

    struct ImportableFile
    {
        ImportableFile(std::filesystem::path path, GameLevel level);

        [[nodiscard]] const std::filesystem::path& path() const &;

        [[nodiscard]] ImportableFilePath path() &&;

        [[nodiscard]] GameLevel level() const;

        [[nodiscard]] ImportableFileType type() const;

    private:
        ImportableFilePath m_path;
        GameLevel m_level;
        ImportableFileType m_type;
    };

    using ImportableFiles = std::vector<ImportableFile>;

    struct DatabaseSupportManifest
    {
        // This structure is not optimized.
        // It doesn't need to be.
        // It also doesn't need to have future ABI compatibility.

        std::vector<ImportableFileType> importableFileTypes;
        MergeMode mergeMode;

        std::uint64_t maxGames;
        std::uint64_t maxPositions;
        std::uint64_t maxInstancesOfSinglePosition;

        bool hasOneWayKey;
        std::uint64_t estimatedMaxCollisions;
        std::uint64_t estimatedMaxPositionsWithNoCollisions; // the breaking point is 50% chance of having a collision.

        bool hasCount;

        bool hasEloDiff;
        std::uint64_t maxAbsEloDiff;
        std::uint64_t maxAverageAbsEloDiff;

        bool hasWhiteElo;
        bool hasBlackElo;
        std::uint64_t minElo;
        std::uint64_t maxElo;
        bool hasCountWithElo;

        bool hasFirstGame;
        bool hasLastGame;

        bool allowsFilteringTranspositions;
        bool hasReverseMove;

        bool allowsFilteringByEloRange;
        std::uint64_t eloFilterGranularity;

        bool allowsFilteringByMonthRange;
        std::uint64_t monthFilterGranularity;

        std::uint64_t maxBytesPerPosition;
        std::optional<double> estimatedAverageBytesPerPosition;
    };

    struct DatabaseManifest
    {
        std::string key;
        bool requiresMatchingEndianness;

        friend void to_json(nlohmann::json& j, const DatabaseManifest& manifest);
    };

    enum struct ManifestValidationResult
    {
        Ok,
        KeyMismatch,
        EndiannessMismatch,
        InvalidManifest
    };

    struct Database
    {
        struct ImportProgressReport
        {
            std::size_t workDone;
            std::size_t workTotal;
            std::optional<std::filesystem::path> importedPgnPath;

            [[nodiscard]] double ratio() const
            {
                return static_cast<double>(workDone) / static_cast<double>(workTotal);
            }
        };

        struct MergeProgressReport
        {
            std::size_t workDone;
            std::size_t workTotal;

            [[nodiscard]] double ratio() const
            {
                return static_cast<double>(workDone) / static_cast<double>(workTotal);
            }
        };

        using ImportProgressCallback = std::function<void(const ImportProgressReport&)>;
        using MergeProgressCallback = std::function<void(const MergeProgressReport&)>;

        Database(const std::filesystem::path& dirPath, const DatabaseManifest& manifestModel);

        [[nodiscard]] static std::filesystem::path manifestPath(const std::filesystem::path& dirPath);

        [[nodiscard]] static std::optional<std::string> tryReadKey(const std::filesystem::path& dirPath);

        [[nodiscard]] virtual const DatabaseManifest& manifest() const = 0;

        virtual const std::filesystem::path & path() const = 0;

        const DatabaseStats& stats() const;

        [[nodiscard]] virtual query::Response executeQuery(query::Request query) = 0;

        virtual void mergeAll(
            const std::vector<std::filesystem::path>& temporaryDirs,
            std::optional<MemoryAmount> temporarySpace,
            MergeProgressCallback progressCallback = {}
        ) = 0;

        virtual void merge(
            const std::vector<std::filesystem::path>& temporaryDirs,
            std::optional<MemoryAmount> temporarySpace,
            const std::string& partitionName,
            const std::vector<std::string>& filenames,
            MergeProgressCallback progressCallback = {}
        ) = 0;

        virtual ImportStats import(
            const ImportableFiles& pgns, 
            std::size_t memory,
            ImportProgressCallback progressCallback = {}
        ) = 0;

        [[nodiscard]] virtual std::map<std::string, std::vector<MergableFile>> mergableFiles() const = 0;

        virtual void flush() = 0;

        virtual void clear() = 0;

        virtual ~Database();

    protected:
        void addStats(ImportStats stats);

    private:
        static const inline std::filesystem::path m_manifestFilename = "manifest";
        static const inline std::filesystem::path m_statsFilename = "stats";

        [[nodiscard]] static std::filesystem::path statsPath(const std::filesystem::path& dirPath);

        [[nodiscard]] std::filesystem::path statsPath() const;

        std::filesystem::path m_baseDirPath;
        DatabaseStats m_stats;
        DatabaseManifest m_manifestModel;

        void loadStats();
        void saveStats();

        [[nodiscard]] ManifestValidationResult createOrValidateManifest() const;

        void initializeManifest() const;

        void createManifest() const;

        [[nodiscard]] ManifestValidationResult validateManifest() const;

        [[nodiscard]] std::filesystem::path manifestPath() const;

        void writeManifest(const std::vector<std::byte>& data) const;

        std::vector<std::byte> readManifest() const;
    };
}
