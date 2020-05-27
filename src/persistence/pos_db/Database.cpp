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
    SingleGameLevelDatabaseStats& SingleGameLevelDatabaseStats::operator+=(const SingleGameLevelDatabaseStats& rhs)
    {
        numGames += rhs.numGames;
        numPositions += rhs.numPositions;
        totalWhiteElo += rhs.totalWhiteElo;
        totalBlackElo += rhs.totalBlackElo;

        if (!numGamesWithElo)
        {
            minElo = rhs.minElo;
            maxElo = rhs.maxElo;
        }
        else if (rhs.numGamesWithElo)
        {
            minElo = std::min(minElo, rhs.minElo);
            maxElo = std::max(maxElo, rhs.maxElo);
        }

        if (!numGamesWithDate)
        {
            minDate = rhs.minDate;
            maxDate = rhs.maxDate;
        }
        else if (rhs.numGamesWithDate)
        {
            Date::min(minDate, rhs.minDate);
            Date::max(maxDate, rhs.maxDate);
        }

        numGamesWithElo += rhs.numGamesWithElo;
        numGamesWithDate += rhs.numGamesWithDate;

        return *this;
    }

    void DatabaseStats::add(SingleGameLevelDatabaseStats stats, GameLevel level)
    {
        m_statsByLevel[level] += stats;
    }

    void to_json(nlohmann::json& j, const DatabaseStats& stats)
    {
        for (GameLevel level : values<GameLevel>())
        {
            j[std::string(toString(level))] = nlohmann::json(stats.m_statsByLevel[level]);
        }
    }

    void from_json(const nlohmann::json& j, DatabaseStats& stats)
    {
        for (GameLevel level : values<GameLevel>())
        {
            stats.m_statsByLevel[level] = j[std::string(toString(level))];
        }
    }

    [[nodiscard]] SingleGameLevelDatabaseStats DatabaseStats::total() const
    {
        SingleGameLevelDatabaseStats sum{};
        for (GameLevel level : values<GameLevel>())
        {
            sum += m_statsByLevel[level];
        }
        return sum;
    }

    const SingleGameLevelDatabaseStats& DatabaseStats::operator[](GameLevel level) const
    {
        return m_statsByLevel[level];
    }

    SingleGameLevelDatabaseStats& DatabaseStats::operator[](GameLevel level)
    {
        return m_statsByLevel[level];
    }

    void to_json(nlohmann::json& j, const SingleGameLevelDatabaseStats& stats)
    {
        j["num_games"] = stats.numGames;
        j["num_positions"] = stats.numPositions;
        j["total_white_elo"] = stats.totalWhiteElo;
        j["total_black_elo"] = stats.totalBlackElo;
        j["num_games_with_elo"] = stats.numGamesWithElo;
        j["num_games_with_date"] = stats.numGamesWithDate;
        if (stats.numGamesWithElo)
        {
            j["min_elo"] = stats.minElo;
            j["max_elo"] = stats.maxElo;
        }
        if (stats.numGamesWithDate)
        {
            j["min_date"] = stats.minDate.toString('-');
            j["max_date"] = stats.maxDate.toString('-');
        }
    }

    void from_json(const nlohmann::json& j, SingleGameLevelDatabaseStats& stats)
    {
        stats.numGames = j["num_games"].get<std::size_t>();
        stats.numPositions = j["num_positions"].get<std::size_t>();
        stats.totalWhiteElo = j["total_white_elo"].get<std::size_t>();
        stats.totalBlackElo = j["total_black_elo"].get<std::size_t>();
        stats.numGamesWithElo = j["num_games_with_elo"].get<std::size_t>();
        stats.numGamesWithDate = j["num_games_with_date"].get<std::size_t>();
        if (stats.numGamesWithElo)
        {
            stats.minElo = j["min_elo"].get<std::uint16_t>();
            stats.maxElo = j["max_elo"].get<std::uint16_t>();
        }
        if (stats.numGamesWithDate)
        {
            stats.minDate = Date::tryParse(j["min_date"].get<std::string>(), '-').value();
            stats.maxDate = Date::tryParse(j["max_date"].get<std::string>(), '-').value();
        }
    }

    SingleGameLevelImportStats& SingleGameLevelImportStats::operator+=(const SingleGameLevelImportStats& rhs)
    {
        SingleGameLevelDatabaseStats::operator+=(rhs);

        numSkippedGames += rhs.numSkippedGames;

        return *this;
    }

    void to_json(nlohmann::json& j, const SingleGameLevelImportStats& stats)
    {
        to_json(j, static_cast<const SingleGameLevelDatabaseStats&>(stats));

        j["num_skipped_games"] = stats.numSkippedGames;
    }

    void from_json(const nlohmann::json& j, SingleGameLevelImportStats& stats)
    {
        from_json(j, static_cast<SingleGameLevelDatabaseStats&>(stats));
        
        stats.numSkippedGames = j["num_skipped_games"].get<std::size_t>();
    }

    ImportStats& ImportStats::operator+=(const ImportStats& rhs)
    {
        for (GameLevel level : values<GameLevel>())
        {
            m_statsByLevel[level] += rhs.m_statsByLevel[level];
        }

        return *this;
    }

    ImportStats::ImportStats(SingleGameLevelImportStats stats, GameLevel level)
    {
        m_statsByLevel[level] = stats;
    }

    void ImportStats::add(SingleGameLevelImportStats stats, GameLevel level)
    {
        m_statsByLevel[level] += stats;
    }

    [[nodiscard]] SingleGameLevelImportStats ImportStats::total() const
    {
        SingleGameLevelImportStats sum{};
        for (GameLevel level : values<GameLevel>())
        {
            sum += m_statsByLevel[level];
        }
        return sum;
    }

    const SingleGameLevelImportStats& ImportStats::operator[](GameLevel level) const
    {
        return m_statsByLevel[level];
    }

    SingleGameLevelImportStats& ImportStats::operator[](GameLevel level)
    {
        return m_statsByLevel[level];
    }

    void to_json(nlohmann::json& j, const ImportStats& stats)
    {
        for (GameLevel level : values<GameLevel>())
        {
            j[std::string(toString(level))] = nlohmann::json(stats.m_statsByLevel[level]);
        }
    }

    void from_json(const nlohmann::json& j, ImportStats& stats)
    {
        for (GameLevel level : values<GameLevel>())
        {
            stats.m_statsByLevel[level] = j[std::string(toString(level))];
        }
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

    void to_json(nlohmann::json& j, const DatabaseManifest& manifest)
    {
        j["name"] = manifest.key;
        j["requires_matching_endianness"] = manifest.requiresMatchingEndianness;
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
        for (GameLevel level : values<GameLevel>())
        {
            m_stats.add(stats[level], level);
        }
        saveStats();
    }

    void Database::loadStats()
    {
        std::ifstream file(statsPath());
        if (file.is_open())
        {
            std::string str(
                (std::istreambuf_iterator<char>(file)),
                std::istreambuf_iterator<char>()
            );
            auto json = nlohmann::json::parse(str);
            m_stats = json.get<DatabaseStats>();
        }
        else
        {
            saveStats();
        }
    }

    void Database::saveStats()
    {
        std::ofstream file(statsPath(), std::ios_base::out | std::ios_base::trunc);
        file << nlohmann::json(m_stats);
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
