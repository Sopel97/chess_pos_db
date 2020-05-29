#include "StringUtil.h"

#include <cstdint>
#include <vector>
#include <string_view>

namespace util
{
    [[nodiscard]] std::vector<std::string_view> splitExcludeEmpty(std::string_view sv, char delimiter)
    {
        std::vector<std::string_view> parts;
        std::size_t start = 0;

        while (start < sv.size())
        {
            const auto end = sv.find(delimiter, start);

            if (start != end)
            {
                parts.emplace_back(sv.substr(start, end - start));
            }

            if (end == std::string_view::npos)
            {
                break;
            }

            start = end + 1;
        }

        return parts;
    }
}
