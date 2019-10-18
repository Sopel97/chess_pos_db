#pragma once

#include <cassert>
#include <cstdint>
#include <immintrin.h>
#include <nmmintrin.h>
#include <intrin.h>

#include "util/Assert.h"

#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)

#define FORCEINLINE __attribute__((always_inline))

#elif defined(_MSC_VER)

// NOTE: for some reason it breaks the profiler a little
//       keep it on only when not profiling.
//#define FORCEINLINE __forceinline
#define FORCEINLINE

#else

#define FORCEINLINE inline

#endif

#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)

#define NOINLINE __attribute__((noinline))

#elif defined(_MSC_VER)

#define NOINLINE __declspec(noinline)

#else

#define NOINLINE

#endif


// the following enables constexpr intrinsics, but they are slower
// it's useful for running compile time tests
// TODO: in C++20 replace with std::is_constant_evaluated()
#define USE_CONSTEXPR_INTRINSICS

#if defined (USE_CONSTEXPR_INTRINSICS)

#define INTRIN_CONSTEXPR constexpr

#else

#define INTRIN_CONSTEXPR inline

#endif


#if defined (USE_CONSTEXPR_INTRINSICS)
namespace intrin
{
    [[nodiscard]] constexpr int popcount(std::uint64_t value)
    {
        int r = 0;
        while (value)
        {
            value &= value - 1;
            ++r;
        }
        return r;
    }

    [[nodiscard]] constexpr int lsb(std::uint64_t value)
    {
        ASSERT(value != 0);

        int c = 0;
        value &= ~value + 1; // leave only the lsb
        if ((value & 0x00000000FFFFFFFFull) == 0) c += 32;
        if ((value & 0x0000FFFF0000FFFFull) == 0) c += 16;
        if ((value & 0x00FF00FF00FF00FFull) == 0) c += 8;
        if ((value & 0x0F0F0F0F0F0F0F0Full) == 0) c += 4;
        if ((value & 0x3333333333333333ull) == 0) c += 2;
        if ((value & 0x5555555555555555ull) == 0) c += 1;
        return c;
    }

    [[nodiscard]] constexpr int msb(std::uint64_t value)
    {
        ASSERT(value != 0);

        int c = 63;
        if ((value & 0xFFFFFFFF00000000ull) == 0) { c -= 32; value <<= 32; }
        if ((value & 0xFFFF000000000000ull) == 0) { c -= 16; value <<= 16; }
        if ((value & 0xFF00000000000000ull) == 0) { c -= 8; value <<= 8; }
        if ((value & 0xF000000000000000ull) == 0) { c -= 4; value <<= 4; }
        if ((value & 0xC000000000000000ull) == 0) { c -= 2; value <<= 2; }
        if ((value & 0x8000000000000000ull) == 0) { c -= 1; }
        return c;
    }
}

#else

namespace intrin
{
    [[nodiscard]] inline int popcount(std::uint64_t b)
    {
#if defined(_MSC_VER) || defined(__INTEL_COMPILER)

        return static_cast<int>(_mm_popcnt_u64(b));

#else

        return static_cast<int>(__builtin_popcountll(b));

#endif
    }

#if defined(_MSC_VER)

    [[nodiscard]] inline int lsb(std::uint64_t value)
    {
        ASSERT(value != 0);

        unsigned long idx;
        _BitScanForward64(&idx, value);
        return static_cast<int>(idx);
    }

    [[nodiscard]] inline int msb(std::uint64_t value)
    {
        ASSERT(value != 0);

        unsigned long idx;
        _BitScanReverse64(&idx, value);
        return static_cast<int>(idx);
    }

#else

    [[nodiscard]] inline int lsb(std::uint64_t value)
    {
        ASSERT(value != 0);

        return __builtin_ctzll(value);
    }

    [[nodiscard]] inline Square msb(std::uint64_t value)
    {
        ASSERT(value != 0);

        return 63 ^ __builtin_clzll(value);
    }

#endif
}
#endif