#pragma once

#include <cstdint>
#include <string_view>
#include <string>
#include <map>

#include "json/json.hpp"

struct MemoryAmount
{
    constexpr MemoryAmount() :
        m_bytes{}
    {
    }

    MemoryAmount(const std::string& s);

    constexpr static MemoryAmount bytes(std::size_t amount)
    {
        return MemoryAmount(amount);
    }

    constexpr static MemoryAmount kilobytes(std::size_t amount)
    {
        return MemoryAmount(amount * 1000);
    }

    constexpr static MemoryAmount megabytes(std::size_t amount)
    {
        return MemoryAmount(amount * 1000 * 1000);
    }

    constexpr static MemoryAmount gigabytes(std::size_t amount)
    {
        return MemoryAmount(amount * 1000 * 1000 * 1000);
    }

    constexpr static MemoryAmount terabytes(std::size_t amount)
    {
        return MemoryAmount(amount * 1000 * 1000 * 1000 * 1000);
    }

    constexpr static MemoryAmount kibibytes(std::size_t amount)
    {
        return MemoryAmount(amount * 1024);
    }

    constexpr static MemoryAmount mebibytes(std::size_t amount)
    {
        return MemoryAmount(amount * 1024 * 1024);
    }

    constexpr static MemoryAmount gibibytes(std::size_t amount)
    {
        return MemoryAmount(amount * 1024 * 1024 * 1024);
    }

    constexpr static MemoryAmount tebibytes(std::size_t amount)
    {
        return MemoryAmount(amount * 1024 * 1024 * 1024 * 1024);
    }

    [[nodiscard]] constexpr operator std::size_t() const
    {
        return m_bytes;
    }

    friend void to_json(nlohmann::json& j, const MemoryAmount& v);

    friend void from_json(const nlohmann::json& j, MemoryAmount& v);

private:
    static const std::map<std::string_view, std::size_t>& units();

    constexpr MemoryAmount(std::size_t volume) :
        m_bytes(volume)
    {
    }

private:
    std::size_t m_bytes;
};
