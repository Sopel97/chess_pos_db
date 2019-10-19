#include "DatabaseFactory.h"

namespace persistence
{
    [[nodiscard]] std::unique_ptr<Database> DatabaseFactory::tryInstantiateByKey(const std::string& key, const std::filesystem::path& path) const
    {
        auto it = m_factories.find(key);
        if (it == m_factories.end()) return nullptr;

        return it->second(path);
    }
}
