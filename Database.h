#pragma once

#include <execution>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "Endian.h"
#include "Query.h"

namespace persistence
{
    struct ImportStats
    {
        std::size_t numGames = 0;
        std::size_t numSkippedGames = 0; // We skip games with an unknown result.
        std::size_t numPositions = 0;

        ImportStats& operator+=(const ImportStats& rhs)
        {
            numGames += rhs.numGames;
            numSkippedGames += rhs.numSkippedGames;
            numPositions += rhs.numPositions;

            return *this;
        }
    };

    using ImportablePgnFilePath = std::filesystem::path;
    using ImportablePgnFilePaths = std::vector<std::filesystem::path>;

    struct ImportablePgnFile
    {
        ImportablePgnFile(std::filesystem::path path, GameLevel level) :
            m_path(std::move(path)),
            m_level(level)
        {
        }

        [[nodiscard]] const auto& path() const &
        {
            return m_path;
        }

        [[nodiscard]] ImportablePgnFilePath path() &&
        {
            return std::move(m_path);
        }

        [[nodiscard]] GameLevel level() const
        {
            return m_level;
        }

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

        void createManifest() const
        {
            const auto& manifestData = this->manifest();

            std::vector<std::byte> data;
            const std::size_t keyLength = manifestData.key.size();
            if (keyLength > 255)
            {
                throw std::runtime_error("Manifest key must be at most 255 chars long.");
            }

            const std::size_t endiannessSignatureLength = manifestData.requiresMatchingEndianness ? sizeof(EndiannessSignature) : 0;
            const std::size_t totalLength = 1 + keyLength + endiannessSignatureLength;

            data.resize(totalLength);

            const std::uint8_t keyLengthLow = static_cast<std::uint8_t>(keyLength);
            std::memcpy(data.data(), &keyLengthLow, 1);
            std::memcpy(data.data() + 1, manifestData.key.data(), keyLength);
            
            if (endiannessSignatureLength != 0)
            {
                const EndiannessSignature sig;
                std::memcpy(data.data() + 1 + keyLength, &sig, endiannessSignatureLength);
            }
        }

        [[nodiscard]] ManifestValidationResult validateManifest() const
        {
            const auto& dbManifestData = this->manifest();

            const auto& manifestData = readManifest();
            if (manifestData.size() == 0) return ManifestValidationResult::InvalidManifest;

            const std::size_t keyLength = std::to_integer<std::size_t>(manifestData[0]);
            if (manifestData.size() < 1 + keyLength) return ManifestValidationResult::InvalidManifest;
            if (keyLength != dbManifestData.key.size()) return ManifestValidationResult::KeyMismatch;

            std::string key;
            key.resize(keyLength);
            std::memcpy(key.data(), manifestData.data() + 1, keyLength);
            if (dbManifestData.key != key) return ManifestValidationResult::KeyMismatch;

            if (dbManifestData.requiresMatchingEndianness)
            {
                if (manifestData.size() != 1 + keyLength + sizeof(EndiannessSignature))
                {
                    return ManifestValidationResult::InvalidManifest;
                }
                else
                {
                    EndiannessSignature sig;
                    std::memcpy(&sig, manifestData.data() + 1 + keyLength, sizeof(EndiannessSignature));

                    return sig == EndiannessSignature{}
                        ? ManifestValidationResult::Ok
                        : ManifestValidationResult::EndiannessMismatch;
                }
            }
            else if(manifestData.size() == 1 + keyLength)
            {
                return ManifestValidationResult::Ok;
            }
            else
            {
                return ManifestValidationResult::InvalidManifest;
            }
        }

        virtual ~Database() {};

    private:
        static const inline std::filesystem::path m_manifestFilename = "manifest";

        [[nodiscard]] std::filesystem::path manifestPath() const
        {
            return this->path() / m_manifestFilename;
        }

        void writeManifest(const std::vector<std::byte>& data) const
        {
            std::ofstream out(manifestPath());
            out.write(reinterpret_cast<const char*>(data.data()), data.size());
        }

        std::vector<std::byte> readManifest() const
        {
            std::ifstream in(manifestPath());
            std::vector<char> data(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>{});
            std::vector<std::byte> bytes(data.size());
            std::memcpy(bytes.data(), data.data(), data.size());
            return bytes;
        }
    };
}
