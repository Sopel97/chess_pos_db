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
    [[nodiscard]] static std::string mergeModeToString(MergeMode m)
    {
        switch (m)
        {
        case MergeMode::None:
            return "none";
        case MergeMode::Consecutive:
            return "consecutive";
        case MergeMode::Any:
            return "any";
        default:
            return "";
        }
    }

    [[nodiscard]] static std::string manifestValidationResultToString(ManifestValidationResult r)
    {
        switch (r)
        {
            case ManifestValidationResult::Ok:
                return "Ok";
            case ManifestValidationResult::EndiannessMismatch:
                return "Endianness mismatch.";
            case ManifestValidationResult::KeyMismatch:
                return "Key mismatch.";
            case ManifestValidationResult::InvalidManifest:
                return "Invalid Manifest.";
            case ManifestValidationResult::InvalidVersion:
                return "Invalid Version.";
            case ManifestValidationResult::UnsupportedVersion:
                return "Unsupported Version.";
        }
    }

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

    void to_json(nlohmann::json& j, const DatabaseSupportManifest& manifest)
    {
        auto supportedTypesJson = nlohmann::json::array();
        for (auto type : manifest.importableFileTypes)
        {
            supportedTypesJson.emplace_back(importableFileTypeExtension(type));
        }

        j["supported_file_types"] = supportedTypesJson;
        j["merge_mode"] = mergeModeToString(manifest.mergeMode);

        j["max_games"] = manifest.maxGames;
        j["max_positions"] = manifest.maxPositions;
        j["max_instances_of_single_position"] = manifest.maxInstancesOfSinglePosition;

        j["has_one_way_key"] = manifest.hasOneWayKey;
        if (manifest.hasOneWayKey)
        {
            j["estimated_max_collisions"] = manifest.estimatedMaxCollisions;
            j["estimated_max_positions_with_no_collisions"] = manifest.estimatedMaxPositionsWithNoCollisions;
        }

        j["has_count"] = manifest.hasCount;

        j["has_elo_diff"] = manifest.hasEloDiff;
        if (manifest.hasEloDiff)
        {
            j["max_abs_elo_diff"] = manifest.maxAbsEloDiff;
            j["max_average_abs_elo_diff"] = manifest.maxAverageAbsEloDiff;
        }

        j["has_white_elo"] = manifest.hasWhiteElo;
        j["has_black_elo"] = manifest.hasBlackElo;
        if (manifest.hasWhiteElo || manifest.hasBlackElo)
        {
            j["min_elo"] = manifest.minElo;
            j["max_elo"] = manifest.maxElo;
            j["has_count_with_elo"] = manifest.hasCountWithElo;
        }

        j["has_first_game"] = manifest.hasFirstGame;
        j["has_last_game"] = manifest.hasLastGame;

        j["allows_filtering_transpositions"] = manifest.allowsFilteringTranspositions;
        j["has_reverse_move"] = manifest.hasReverseMove;

        j["allows_filtering_by_elo_range"] = manifest.allowsFilteringByEloRange;
        j["elo_filter_granularity"] = manifest.eloFilterGranularity;

        j["allows_filtering_by_month_range"] = manifest.allowsFilteringByMonthRange;
        j["month_filter_granularity"] = manifest.monthFilterGranularity;

        j["max_bytes_per_position"] = manifest.maxBytesPerPosition;

        if (manifest.estimatedAverageBytesPerPosition.has_value())
        {
            j["estimated_average_bytes_per_position"] = *manifest.estimatedAverageBytesPerPosition;
        }

        j["version"] = manifest.version.toString();
        j["minimum_supported_version"] = manifest.minimumSupportedVersion.toString();
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
        j["version"] = manifest.version.toString();

        if (manifest.endiannessSignature.has_value())
        {
            j["endianness_signature"] = *manifest.endiannessSignature;
        }
    }

    void from_json(const nlohmann::json& j, DatabaseManifest& manifest)
    {
        manifest.key = j["name"].get<std::string>();
        manifest.version = util::SemanticVersion::fromString(j["version"].get<std::string>()).value();

        if (j.contains("endianness_signature"))
        {
            manifest.endiannessSignature = j["endianness_signature"];
        }
        else
        {
            manifest.endiannessSignature = std::nullopt;
        }
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

    Database::Database(const std::filesystem::path& dirPath, const DatabaseManifestModel& manifestModel, const DatabaseSupportManifest& support) :
        m_baseDirPath(dirPath)
    {
        initializeManifest(manifestModel, support);
        loadStats();
    }

    [[nodiscard]] std::filesystem::path Database::manifestPath(const std::filesystem::path& dirPath)
    {
        return dirPath / m_manifestFilename;
    }

    [[nodiscard]] std::optional<std::string> Database::tryReadKey(const std::filesystem::path& dirPath)
    {
        std::ifstream file(manifestPath(dirPath));
        if (!file.is_open()) return std::nullopt;

        try
        {
            std::string str(
                (std::istreambuf_iterator<char>(file)),
                std::istreambuf_iterator<char>()
            );
            auto json = nlohmann::json::parse(str);
            DatabaseManifest manifest = json.get<DatabaseManifest>();

            return manifest.key;
        }
        catch (...)
        {
            return std::nullopt;
        }
    }

    [[nodiscard]] const DatabaseStats& Database::stats() const
    {
        return m_stats;
    }

    [[nodiscard]] const DatabaseManifest& Database::manifest() const
    {
        return m_manifest;
    }

    DatabaseManifest Database::updateManifest(const DatabaseManifestModel& manifestModel, const DatabaseSupportManifest& support)
    {
        return createManifest(manifestModel, support);
    }

    void Database::initializeManifest(const DatabaseManifestModel& manifestModel, const DatabaseSupportManifest& support)
    {
        const bool firstOpen = !std::filesystem::exists(manifestPath());

        if (firstOpen)
        {
            m_manifest = createManifest(manifestModel, support);
        }
        else
        {
            const auto ec = validateManifest(manifestModel, support);
            if (ec != ManifestValidationResult::Ok)
            {
                throw std::runtime_error("Cannot open database: " + manifestValidationResultToString(ec));
            }

            m_manifest = updateManifest(manifestModel, support);
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
    
    DatabaseManifest Database::createManifest(const DatabaseManifestModel& manifestModel, const DatabaseSupportManifest& support)
    {
        DatabaseManifest manifest{};
        
        manifest.key = manifestModel.key;
        manifest.version = manifestModel.version;

        if (manifestModel.requiresMachingEndianness)
        {
            manifest.endiannessSignature = EndiannessSignature{};
        }

        writeManifest(manifest);

        return manifest;
    }

    [[nodiscard]] ManifestValidationResult Database::validateManifest(const DatabaseManifestModel& manifestModel, const DatabaseSupportManifest& support) const
    {
        try
        {
            DatabaseManifest manifest = readManifest();

            if (manifest.key != manifestModel.key)
            {
                return ManifestValidationResult::KeyMismatch;
            }

            if (manifest.version < support.minimumSupportedVersion)
            {
                return ManifestValidationResult::UnsupportedVersion;
            }

            if (manifestModel.requiresMachingEndianness)
            {
                if (!manifest.endiannessSignature.has_value() 
                    || manifest.endiannessSignature != EndiannessSignature{})
                {
                    return ManifestValidationResult::EndiannessMismatch;
                }
            }

            return ManifestValidationResult::Ok;
        }
        catch (...)
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

    void Database::writeManifest(const DatabaseManifest& manifest)
    {
        nlohmann::json json = manifest;
        std::string jsonString = json.dump();

        const auto path = manifestPath();
        std::filesystem::create_directories(path.parent_path());
        std::ofstream out(path, std::ios::binary);
        out.write(reinterpret_cast<const char*>(jsonString.data()), jsonString.size());
    }

    DatabaseManifest Database::readManifest() const
    {
        std::ifstream in(manifestPath(), std::ios::binary);
        std::string str(
            (std::istreambuf_iterator<char>(in)),
            std::istreambuf_iterator<char>()
        );
        auto json = nlohmann::json::parse(str);
        return json.get<DatabaseManifest>();
    }
}
