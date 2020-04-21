#include "DatabaseFactory.h"

namespace persistence
{
    [[nodiscard]] std::unique_ptr<Database> DatabaseFactory::tryInstantiateByKey(const std::string& key, const std::filesystem::path& path) const
    {
        auto it = m_factories.find(key);
        if (it == m_factories.end()) return nullptr;

        return it->second->create(path);
    }

    [[nodiscard]] const SpecificDatabaseFactoryBase& DatabaseFactory::at(const std::string& key) const
    {
        return *m_factories.at(key);
    }

    [[nodiscard]] std::map<std::string, DatabaseSupportManifest> DatabaseFactory::supportManifests() const
    {
        std::map<std::string, DatabaseSupportManifest> manifests;

        for (auto&& [name, fac] : m_factories)
        {
            manifests[name] = fac->supportManifest();
        }
    }
}
