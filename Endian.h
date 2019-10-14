#pragma once

#include <cstdint>
#include <type_traits>

struct EndiannessSignature
{
    [[nodiscard]] friend bool operator==(const EndiannessSignature& lhs, const EndiannessSignature& rhs) noexcept
    {
        return lhs.m_uint64 == rhs.m_uint64
            && lhs.m_uint32 == rhs.m_uint32
            && lhs.m_uint16 == rhs.m_uint16
            && lhs.m_uint8 == rhs.m_uint8;
    }

private:
    std::uint64_t m_uint64 = 0x11223344556677u;
    std::uint32_t m_uint32 = 0x8899AABBu;
    std::uint16_t m_uint16 = 0xCCDD;
    std::uint8_t m_uint8 = 0xEE;
};

static_assert(std::is_trivially_copy_assignable_v<EndiannessSignature>);
static_assert(sizeof(EndiannessSignature) == 16);
