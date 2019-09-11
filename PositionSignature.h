#pragma once

#include "Position.h"

#include "lib/xxhash/xxhash_cpp.h"

#include <cstring>
#include <functional>

// currently only uses hash
// currently doesn't differentiate positions by available legal moves
// TODO: loseless compression later

struct PositionSignature
{
    using StorageType = std::array<std::uint32_t, 4>;

    PositionSignature(const Position& pos)
    {
        auto h = xxhash::XXH3_128bits(pos.piecesRaw(), 64);
        std::memcpy(m_hash.data(), &h, sizeof(StorageType));
        m_hash[0] ^= ordinal(pos.sideToMove());
    }

    [[nodiscard]] friend bool operator==(const PositionSignature& lhs, const PositionSignature& rhs) noexcept
    {
        return
            lhs.m_hash[0] == rhs.m_hash[0]
            && lhs.m_hash[1] == rhs.m_hash[1]
            && lhs.m_hash[2] == rhs.m_hash[2]
            && lhs.m_hash[3] == rhs.m_hash[3];
    }

    [[nodiscard]] friend bool operator<(const PositionSignature& lhs, const PositionSignature& rhs) noexcept
    {
        if (lhs.m_hash[0] < rhs.m_hash[0]) return true;
        else if (lhs.m_hash[0] > rhs.m_hash[0]) return false;

        if (lhs.m_hash[1] < rhs.m_hash[1]) return true;
        else if (lhs.m_hash[1] > rhs.m_hash[1]) return false;

        if (lhs.m_hash[2] < rhs.m_hash[2]) return true;
        else if (lhs.m_hash[2] > rhs.m_hash[2]) return false;

        if (lhs.m_hash[3] < rhs.m_hash[3]) return true;
        return false;
    }

    [[nodiscard]] const StorageType& hash() const
    {
        return m_hash;
    }

private:
    StorageType m_hash;
};
static_assert(sizeof(PositionSignature) == 16);

namespace std
{
    template<> struct hash<PositionSignature>
    {
        using argument_type = PositionSignature;
        using result_type = std::size_t;
        result_type operator()(const argument_type& s) const noexcept
        {
            return (static_cast<std::size_t>(s.hash()[0]) << 32) | s.hash()[1];
        }
    };
}