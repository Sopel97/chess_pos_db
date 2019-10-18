#pragma once

#include "Query.h"

#include <execution>
#include <filesystem>
#include <string>
#include <vector>

namespace persistence
{
    struct ImportStats
    {
        std::size_t numGames = 0;
        std::size_t numSkippedGames = 0; // We skip games with an unknown result.
        std::size_t numPositions = 0;

        ImportStats& operator+=(const ImportStats& rhs);
    };

    using ImportablePgnFilePath = std::filesystem::path;
    using ImportablePgnFilePaths = std::vector<std::filesystem::path>;

    struct ImportablePgnFile
    {
        ImportablePgnFile(std::filesystem::path path, GameLevel level);

        [[nodiscard]] const std::filesystem::path& path() const &;

        [[nodiscard]] ImportablePgnFilePath path() &&;

        [[nodiscard]] GameLevel level() const;

    private:
        ImportablePgnFilePath m_path;
        GameLevel m_level;
    };

    using ImportablePgnFiles = std::vector<ImportablePgnFile>;

    struct DatabaseManifest
    {
        std::string key;
        bool requiresMatchingEndianness;
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
        [[nodiscard]] virtual const DatabaseManifest& manifest() const = 0;

        virtual const std::filesystem::path & path() const = 0;

        [[nodiscard]] virtual query::Response executeQuery(query::Request query) = 0;

        virtual void mergeAll() = 0;

        virtual void replicateMergeAll(const std::filesystem::path& path) = 0;

        virtual ImportStats import(
            std::execution::parallel_unsequenced_policy,
            const ImportablePgnFiles& pgns,
            std::size_t memory,
            std::size_t numThreads = std::thread::hardware_concurrency()
        ) = 0;

        virtual ImportStats import(
            std::execution::sequenced_policy,
            const ImportablePgnFiles& pgns,
            std::size_t memory
        ) = 0;

        virtual ImportStats import(const ImportablePgnFiles& pgns, std::size_t memory) = 0;

        virtual void flush() = 0;

        virtual void clear() = 0;

        [[nodiscard]] ManifestValidationResult createOrValidateManifest() const;

        void initializeManifest() const;

        virtual ~Database();

    private:
        static const inline std::filesystem::path m_manifestFilename = "manifest";

        void createManifest() const;

        [[nodiscard]] ManifestValidationResult validateManifest() const;

        [[nodiscard]] std::filesystem::path manifestPath() const;

        void writeManifest(const std::vector<std::byte>& data) const;

        std::vector<std::byte> readManifest() const;
    };
}
