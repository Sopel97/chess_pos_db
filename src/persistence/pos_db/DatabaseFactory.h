#pragma once

#include "Database.h"

#include <filesystem>
#include <map>
#include <memory>
#include <string>

namespace persistence
{
    struct DatabaseFactory
    {
        using SpecificDatabaseFactory = std::unique_ptr<Database>(*)(const std::filesystem::path&);

        DatabaseFactory() = default;

        template <typename DatabaseT>
        void registerDatabaseType()
        {
            m_factories[DatabaseT::key()] = [](const std::filesystem::path& path) {
                return std::make_unique<DatabaseT>(path);
            };
        }

        [[nodiscard]] std::unique_ptr<Database> tryInstantiateByKey(const std::string& key, const std::filesystem::path& path) const;

    private:
        std::map<std::string, SpecificDatabaseFactory> m_factories;
    };
}
