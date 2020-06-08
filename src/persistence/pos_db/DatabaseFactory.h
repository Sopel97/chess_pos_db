#pragma once

#include "Database.h"

#include <filesystem>
#include <map>
#include <memory>
#include <string>

namespace persistence
{
    struct SpecificDatabaseFactoryBase
    {
        [[nodiscard]] virtual std::unique_ptr<Database> create(const std::filesystem::path&) const = 0;

        [[nodiscard]] virtual DatabaseSupportManifest supportManifest() const = 0;

        virtual ~SpecificDatabaseFactoryBase() {};
    };

    template <typename DatabaseT>
    struct SpecificDatabaseFactory : SpecificDatabaseFactoryBase
    {
        [[nodiscard]] std::unique_ptr<Database> create(const std::filesystem::path& path) const override
        {
            return std::make_unique<DatabaseT>(path);
        }

        [[nodiscard]] DatabaseSupportManifest supportManifest() const override
        {
            return DatabaseT::supportManifest();
        }
    };

    struct DatabaseFactory
    {
        DatabaseFactory() = default;

        template <typename DatabaseT>
        void registerDatabaseSchema()
        {
            m_factories[DatabaseT::schema()] = std::make_unique<SpecificDatabaseFactory<DatabaseT>>();
        }

        [[nodiscard]] std::unique_ptr<Database> tryInstantiateBySchema(const std::string& key, const std::filesystem::path& path) const;

        [[nodiscard]] const SpecificDatabaseFactoryBase& at(const std::string& key) const;

        [[nodiscard]] std::map<std::string, DatabaseSupportManifest> supportManifests() const;

    private:
        std::map<std::string, std::unique_ptr<SpecificDatabaseFactoryBase>> m_factories;
    };
}
