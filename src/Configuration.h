#pragma once

#include <fstream>
#include <cstdint>
#include <string>
#include <iostream>

#include "json/json.hpp"

#include "util/MemoryAmount.h"
#include "Logger.h"

namespace cfg
{
    struct Configuration
    {
        static const Configuration& instance();

        template <typename T>
        decltype(auto) operator[](T&& arg) const
        {
            return m_json[std::forward<T>(arg)];
        }

        void print(std::ostream& out) const;

    private:
        nlohmann::json m_json;

        static const nlohmann::json& defaultJson();

        Configuration();
    };

    static inline const Configuration& g_config = Configuration::instance();
}
