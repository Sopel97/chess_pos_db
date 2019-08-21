#pragma once

#include <cassert>
#include <cstdint>
#include <immintrin.h>
#include <nmmintrin.h>
#include <intrin.h>

// the following enables constexpr intrinsics, but they are slower
// it's useful for running compile time tests
#define USE_CONSTEXPR_INTRINSICS

#if defined (USE_CONSTEXPR_INTRINSICS)
namespace intrin
{
    constexpr int popcount(std::uint64_t value)
    {
        int r = 0;
        while (value)
        {
            value &= value - 1;
            ++r;
        }
        return r;
    }

    constexpr int lsb(std::uint64_t value)
    {
        // assumes value != 0

        int r = 0;
        while (value <<= 1) ++r;
        return 63 - r;
    }

    constexpr int msb(std::uint64_t value)
    {
        // assumes value != 0

        int r = 0;
        while (value >>= 1) ++r;
        return r;
    }
}
#else
namespace intrin
{
    inline int popcount(std::uint64_t b)
    {
#if defined(_MSC_VER) || defined(__INTEL_COMPILER)

        return static_cast<int>(_mm_popcnt_u64(b));

#else // Assumed gcc or compatible compiler

        return static_cast<int>(__builtin_popcountll(b));

#endif
    }

#if defined(__GNUC__)  // GCC, Clang, ICC

    inline int lsb(std::uint64_t value) 
    {
        assert(value != 0);
        return __builtin_ctzll(value);
    }

    inline Square msb(std::uint64_t value) 
    {
        assert(value != 0);
        return 63 ^ __builtin_clzll(value);
    }

#elif defined(_MSC_VER)  // MSVC

    inline int lsb(std::uint64_t value) 
    {
        assert(value != 0);
        unsigned long idx;
        _BitScanForward64(&idx, value);
        return static_cast<int>(idx);
    }

    inline int msb(std::uint64_t value) 
    {
        assert(value != 0);
        unsigned long idx;
        _BitScanReverse64(&idx, value);
        return static_cast<int>(idx);
    }

#endif
}
#endif