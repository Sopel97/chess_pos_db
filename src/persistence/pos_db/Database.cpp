#include "Database.h"

#include "Query.h"

#include "util/Endian.h"

#include <execution>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace persistence
{
    ImportStats& ImportStats::operator+=(const ImportStats& rhs)
    {
        numGames += rhs.numGames;
        numSkippedGames += rhs.numSkippedGames;
        numPositions += rhs.numPositions;

        return *this;
    }

    ImportablePgnFile::ImportablePgnFile(std::filesystem::path path, GameLevel level) :
        m_path(std::move(path)),
        m_level(level)
    {
    }

    [[nodiscard]] const std::filesystem::path& ImportablePgnFile::path() const &
    {
        return m_path;
    }

    [[nodiscard]] ImportablePgnFilePath ImportablePgnFile::path() &&
    {
        return std::move(m_path);
    }

    [[nodiscard]] GameLevel ImportablePgnFile::level() const
    {
        return m_level;
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

    void Database::replicateMergeAll(const std::filesystem::path& path, Database::MergeProgressCallback)
    {
        std::filesystem::copy_file(manifestPath(this->path()), manifestPath(path));
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
    
    void Database::createManifest() const
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

        writeManifest(data);
    }

    [[nodiscard]] ManifestValidationResult Database::validateManifest() const
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
        else if (manifestData.size() == 1 + keyLength)
        {
            return ManifestValidationResult::Ok;
        }
        else
        {
            return ManifestValidationResult::InvalidManifest;
        }
    }

    [[nodiscard]] std::filesystem::path Database::manifestPath() const
    {
        return this->path() / m_manifestFilename;
    }

    void Database::writeManifest(const std::vector<std::byte>& data) const
    {
        std::ofstream out(manifestPath());
        out.write(reinterpret_cast<const char*>(data.data()), data.size());
    }

    std::vector<std::byte> Database::readManifest() const
    {
        std::ifstream in(manifestPath());
        std::vector<char> data(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>{});
        std::vector<std::byte> bytes(data.size());
        std::memcpy(bytes.data(), data.data(), data.size());
        return bytes;
    }
}
