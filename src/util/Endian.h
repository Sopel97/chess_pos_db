#pragma once

#include <cstdint>
#include <type_traits>

#include "json/json.hpp"

struct EndiannessSignature
{
    [[nodiscard]] friend bool operator==(const EndiannessSignature& lhs, const EndiannessSignature& rhs) noexcept
    {
        return lhs.m_uint64 == rhs.m_uint64
            && lhs.m_uint32 == rhs.m_uint32
            && lhs.m_uint16 == rhs.m_uint16
            && lhs.m_uint8 == rhs.m_uint8;
    }

    [[nodiscard]] friend bool operator!=(const EndiannessSignature& lhs, const EndiannessSignature& rhs) noexcept
    {
        return !(lhs == rhs);
    }

    friend void to_json(nlohmann::json& json, const EndiannessSignature& e)
    {
        unsigned char bytes[sizeof(EndiannessSignature)];
        std::memcpy(bytes, &e, sizeof(EndiannessSignature));

        std::vector<unsigned> bytesAsValues(bytes, bytes + sizeof(EndiannessSignature));
        json["endianness_signature"] = bytesAsValues;
    }

    friend void from_json(const nlohmann::json& json, EndiannessSignature& e)
    {
        std::vector<unsigned> bytesAsValues;
        json["endianness_signature"].get_to(bytesAsValues);

        if (bytesAsValues.size() != sizeof(EndiannessSignature))
        {
            throw std::runtime_error("Invalid endiannss signature.");
        }

        unsigned char bytes[sizeof(EndiannessSignature)];
        for (int i = 0; i < sizeof(EndiannessSignature); ++i)
        {
            if (bytesAsValues[i] > 255)
            {
                throw std::runtime_error("Invalid endiannss signature.");
            }

            bytes[i] = static_cast<unsigned char>(bytesAsValues[i]);
        }
        std::memcpy(&e, bytes, sizeof(EndiannessSignature));
    }

private:
    std::uint64_t m_uint64 = 0x11223344556677u;
    std::uint32_t m_uint32 = 0x8899AABBu;
    std::uint16_t m_uint16 = 0xCCDD;
    std::uint8_t m_uint8 = 0xEE;
};

static_assert(std::is_trivially_copy_assignable_v<EndiannessSignature>);
static_assert(sizeof(EndiannessSignature) == 16);
