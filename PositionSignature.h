#pragma once

#include "Position.h"

#include "lib/xxhash/xxhash_cpp.h"

#include <functional>

// currently only uses hash
// currently doesn't differentiate positions by available legal moves
// TODO: loseless compression later

struct PositionSignature
{
    PositionSignature(const Position& pos) :
        m_hash(xxhash::XXH3_128bits(pos.piecesRaw(), 64))
    {
        m_hash.low64 ^= ordinal(pos.sideToMove());
    }

    [[nodiscard]] friend bool operator==(const PositionSignature& lhs, const PositionSignature& rhs) noexcept
    {
        return (lhs.m_hash.high64 == rhs.m_hash.high64) && (lhs.m_hash.low64 == rhs.m_hash.low64);
    }

    [[nodiscard]] friend bool operator<(const PositionSignature& lhs, const PositionSignature& rhs) noexcept
    {
        return (lhs.m_hash.high64 < rhs.m_hash.high64) | ((lhs.m_hash.high64 == rhs.m_hash.high64) & (lhs.m_hash.low64 < rhs.m_hash.low64));
    }

    [[nodiscard]] const xxhash::XXH128_hash_t& hash() const
    {
        return m_hash;
    }

private:
    xxhash::XXH128_hash_t m_hash;
};

namespace std
{
    template<> struct hash<PositionSignature>
    {
        using argument_type = PositionSignature;
        using result_type = std::size_t;
        result_type operator()(const argument_type& s) const noexcept
        {
            return static_cast<std::size_t>(s.hash().low64);
        }
    };
}