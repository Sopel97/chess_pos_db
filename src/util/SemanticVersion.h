#pragma once

#include "StringUtil.h"

#include <charconv>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace util
{
    struct SemanticVersion
    {
        std::uint32_t major;
        std::uint32_t minor;
        std::uint32_t patch;

        [[nodiscard]] constexpr friend bool operator==(const SemanticVersion& lhs, const SemanticVersion& rhs) noexcept
        {
            return
                lhs.major == rhs.major
                && lhs.minor == rhs.minor
                && lhs.patch == rhs.patch;
        }

        [[nodiscard]] constexpr friend bool operator!=(const SemanticVersion& lhs, const SemanticVersion& rhs) noexcept
        {
            return !(lhs == rhs);
        }

        [[nodiscard]] constexpr friend bool operator<(const SemanticVersion& lhs, const SemanticVersion& rhs) noexcept
        {
            if (lhs.major < rhs.major) return true;
            else if (lhs.major > rhs.major) return false;

            if (lhs.minor < rhs.minor) return true;
            else if (lhs.minor > rhs.minor) return false;

            return lhs.patch < rhs.patch;
        }

        [[nodiscard]] constexpr friend bool operator>(const SemanticVersion& lhs, const SemanticVersion& rhs) noexcept
        {
            return rhs < lhs;
        }

        [[nodiscard]] constexpr friend bool operator<=(const SemanticVersion& lhs, const SemanticVersion& rhs) noexcept
        {
            return !(rhs < lhs);
        }

        [[nodiscard]] constexpr friend bool operator>=(const SemanticVersion& lhs, const SemanticVersion& rhs) noexcept
        {
            return !(lhs < rhs);
        }

        [[nodiscard]] std::string toString() const
        {
            std::string s;

            s += std::to_string(major);
            s += '.';
            s += std::to_string(minor);
            s += '.';
            s += std::to_string(patch);

            return s;
        }

        static std::optional<SemanticVersion> fromString(std::string_view sv)
        {
            SemanticVersion v;

            const auto parts = util::splitExcludeEmpty(sv, '.');
            if (parts.size() != 3)
            {
                return std::nullopt;
            }

            if (
                std::from_chars(parts[0].data(), parts[0].data() + parts[0].size(), v.major).ec != std::errc()
                || std::from_chars(parts[1].data(), parts[1].data() + parts[1].size(), v.minor).ec != std::errc()
                || std::from_chars(parts[2].data(), parts[2].data() + parts[2].size(), v.patch).ec != std::errc()
                )
            {
                return std::nullopt;
            }

            return v;
        }
    };

    static_assert(sizeof(SemanticVersion) == 12);
}
