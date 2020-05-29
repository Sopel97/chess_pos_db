#pragma once

#include <cstdint>
#include <vector>
#include <string_view>

namespace util
{
    [[nodiscard]] std::vector<std::string_view> splitExcludeEmpty(std::string_view sv, char delimiter);
}
