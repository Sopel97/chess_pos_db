#include "Database.h"

#include "Query.h"

#include "util/Endian.h"

#include <execution>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include <iostream>

namespace persistence
{
    SingleGameLevelImportStats& SingleGameLevelImportStats::operator+=(const SingleGameLevelImportStats& rhs)
    {
        numGames += rhs.numGames;
        numSkippedGames += rhs.numSkippedGames;
        numPositions += rhs.numPositions;

        return *this;
    }

    ImportStats& ImportStats::operator+=(const ImportStats& rhs)
    {
        for (GameLevel level : values<GameLevel>())
        {
            statsByLevel[level] += rhs.statsByLevel[level];
        }

        return *this;
    }

    ImportStats::ImportStats(SingleGameLevelImportStats stats, GameLevel level)
    {
        statsByLevel[level] = stats;
    }

    [[nodiscard]] std::size_t ImportStats::totalNumGames() const
    {
        std::size_t total = 0;
        for (GameLevel level : values<GameLevel>())
        {
            total += statsByLevel[level].numGames;
        }
        return total;
    }

    [[nodiscard]] std::size_t ImportStats::totalNumSkippedGames() const
    {
        std::size_t total = 0;
        for (GameLevel level : values<GameLevel>())
        {
            total += statsByLevel[level].numSkippedGames;
        }
        return total;
    }

    [[nodiscard]] std::size_t ImportStats::totalNumPositions() const
    {
        std::size_t total = 0;
        for (GameLevel level : values<GameLevel>())
        {
            total += statsByLevel[level].numPositions;
        }
        return total;
    }

    void ImportStats::add(SingleGameLevelImportStats stats, GameLevel level)
    {
        statsByLevel[level] += stats;
    }

    SingleGameLevelDatabaseStats& SingleGameLevelDatabaseStats::operator+=(const SingleGameLevelImportStats& rhs)
    {
        numGames += rhs.numGames;
        numPositions += rhs.numPositions;
        
        return *this;
    }

    DatabaseStats& DatabaseStats::operator+=(const ImportStats& rhs)
    {
        for (GameLevel level : values<GameLevel>())
        {
            statsByLevel[level] += rhs.statsByLevel[level];
        }

        return *this;
    }

    [[nodiscard]] const std::string& importableFileTypeExtension(ImportableFileType type)
    {
        static std::string extensions[] = {
            ".pgn",
            ".bcgn",
            ""
        };

        return extensions[static_cast<int>(type)];
    }

    [[nodiscard]] ImportableFileType importableFileTypeFromPath(const std::filesystem::path path)
    {
        const auto extension = path.extension();

        if (extension == importableFileTypeExtension(ImportableFileType::Pgn)) 
            return ImportableFileType::Pgn;

        if (extension == importableFileTypeExtension(ImportableFileType::Bcgn))
            return ImportableFileType::Bcgn;

        return ImportableFileType::Unknown;
    }

    void to_json(nlohmann::json& j, const MergableFile& file)
    {
        j["name"] = file.name;
        j["size"] = file.sizeBytes;
    }

    ImportableFile::ImportableFile(std::filesystem::path path, GameLevel level) :
        m_path(std::move(path)),
        m_level(level),
        m_type(importableFileTypeFromPath(m_path))
    {
    }

    [[nodiscard]] const std::filesystem::path& ImportableFile::path() const &
    {
        return m_path;
    }

    [[nodiscard]] ImportableFilePath ImportableFile::path() &&
    {
        return std::move(m_path);
    }

    [[nodiscard]] GameLevel ImportableFile::level() const
    {
        return m_level;
    }

    [[nodiscard]] ImportableFileType ImportableFile::type() const
    {
        return m_type;
    }

    Database::Database(const std::filesystem::path& dirPath, const DatabaseManifest& manifestModel) :
        m_baseDirPath(dirPath),
        m_manifestModel(manifestModel)
    {
        initializeManifest();
        loadStats();
    }

    [[nodiscard]] std::filesystem::path Database::manifestPath(const std::filesystem::path& dirPath)
    {
        return dirPath / m_manifestFilename;
    }

    [[nodiscard]] std::optional<std::string> Database::tryReadKey(const std::filesystem::path& dirPath)
    {
        std::ifstream in(manifestPath(dirPath));

        if (!in.is_open()) return std::nullopt;

        std::vector<char> data(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>{});

        if (data.size() == 0) return std::nullopt;

        const std::size_t keyLength = static_cast<std::size_t>(data[0]);
        if (data.size() < 1 + keyLength) return std::nullopt;

        std::string key;
        key.resize(keyLength);
        std::memcpy(key.data(), data.data() + 1, keyLength);
        return key;
    }

    const DatabaseStats& Database::stats() const
    {
        return m_stats;
    }

    [[nodiscard]] ManifestValidationResult Database::createOrValidateManifest() const
    {
        if (std::filesystem::exists(manifestPath()))
        {
            return validateManifest();
        }
        else
        {
            createManifest();
            return ManifestValidationResult::Ok;
        }
    }

    void Database::initializeManifest() const
    {
        auto res = createOrValidateManifest();
        switch (res)
        {
        case ManifestValidationResult::EndiannessMismatch:
            throw std::runtime_error("Cannot load database. Endianness mismatch.");
        case ManifestValidationResult::KeyMismatch:
            throw std::runtime_error("Cannot load database. Key mismatch.");
        case ManifestValidationResult::InvalidManifest:
            throw std::runtime_error("Cannot load database. Invalid manifest.");
        }
    }

    Database::~Database() {};

    void Database::addStats(ImportStats stats)
    {
        m_stats += stats;
        saveStats();
    }

    void Database::loadStats()
    {
        std::ifstream file(statsPath());
        if (file.is_open())
        {
            for (GameLevel level : values<GameLevel>())
            {
                file >> m_stats.statsByLevel[level].numGames;
                file >> m_stats.statsByLevel[level].numPositions;
            }
        }
        else
        {
            saveStats();
        }
    }

    void Database::saveStats()
    {
        std::ofstream file(statsPath(), std::ios_base::out | std::ios_base::trunc);
        for (GameLevel level : values<GameLevel>())
        {
            file << m_stats.statsByLevel[level].numGames << ' ';
            file << m_stats.statsByLevel[level].numPositions << ' ';
        }
    }
    
    void Database::createManifest() const
    {
        const auto& manifestData = m_manifestModel;

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

        writeManifest(data);
    }

    [[nodiscard]] ManifestValidationResult Database::validateManifest() const
    {
        const auto& dbManifestData = m_manifestModel;

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
        else if (manifestData.size() == 1 + keyLength)
        {
            return ManifestValidationResult::Ok;
        }
        else
        {
            return ManifestValidationResult::InvalidManifest;
        }
    }

    [[nodiscard]] std::filesystem::path Database::statsPath(const std::filesystem::path& dirPath)
    {
        return dirPath / m_statsFilename;
    }

    [[nodiscard]] std::filesystem::path Database::statsPath() const
    {
        return statsPath(m_baseDirPath);
    }

    [[nodiscard]] std::filesystem::path Database::manifestPath() const
    {
        return m_baseDirPath / m_manifestFilename;
    }

    void Database::writeManifest(const std::vector<std::byte>& data) const
    {
        const auto path = manifestPath();
        std::filesystem::create_directories(path.parent_path());
        std::ofstream out(path, std::ios::binary);
        out.write(reinterpret_cast<const char*>(data.data()), data.size());
    }

    std::vector<std::byte> Database::readManifest() const
    {
        std::ifstream in(manifestPath(), std::ios::binary);
        std::vector<char> data(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>{});
        std::vector<std::byte> bytes(data.size());
        std::memcpy(bytes.data(), data.data(), data.size());
        return bytes;
    }
}
