#pragma once

#include "intrin/Intrinsics.h"

#include <array>
#include <cstdint>
#include <limits>
#include <type_traits>

template <typename IntT>
[[nodiscard]] constexpr IntT mulSaturate(IntT lhs, IntT rhs)
{
    static_assert(std::is_unsigned_v<IntT>); // currently no support for signed

#if defined (_MSC_VER)

    if (lhs == 0) return 0;

    const IntT result = lhs * rhs;
    return result / lhs == rhs ? result : std::numeric_limits<IntT>::max();

#elif defined (__GNUC__)

    IntT result{};
    return __builtin_mul_overflow(lhs, rhs, &result) ? std::numeric_limits<IntT>::max() : result;

#endif
}

template <typename IntT>
[[nodiscard]] constexpr IntT addSaturate(IntT lhs, IntT rhs)
{
    static_assert(std::is_unsigned_v<IntT>); // currently no support for signed

#if defined (_MSC_VER)

    const IntT result = lhs + rhs;
    return result >= lhs ? result : std::numeric_limits<IntT>::max();

#elif defined (__GNUC__)

    IntT result{};
    return __builtin_add_overflow(lhs, rhs, &result) ? std::numeric_limits<IntT>::max() : result;

#endif
}

template <typename IntT>
[[nodiscard]] constexpr bool addOverflows(IntT lhs, IntT rhs)
{
#if defined (_MSC_VER)

    return static_cast<IntT>(lhs + rhs) < lhs;

#elif defined (__GNUC__)

    return __builtin_add_overflow(lhs, rhs, &result);

#endif
}

template <typename IntT>
[[nodiscard]] INTRIN_CONSTEXPR IntT floorLog2(IntT value)
{
    return intrin::msb(value);
}

template <typename IntT>
constexpr std::size_t maxFibonacciNumberIndexForType()
{
    static_assert(std::is_unsigned_v<IntT>);

    switch (sizeof(IntT))
    {
    case 8:
        return 93;
    case 4:
        return 47;
    case 2:
        return 24;
    case 1:
        return 13;
    }

    return 0;
}

template <typename IntT>
constexpr auto computeMasks()
{
    static_assert(std::is_unsigned_v<IntT>);

    constexpr std::size_t numBits = sizeof(IntT) * CHAR_BIT;
    std::array<IntT, numBits + 1u> nbitmasks{};

    for (std::size_t i = 0; i < numBits; ++i)
    {
        nbitmasks[i] = (static_cast<IntT>(1u) << i) - 1u;
    }
    nbitmasks[numBits] = ~static_cast<IntT>(0u);

    return nbitmasks;
}

template <typename IntT>
constexpr auto nbitmask = computeMasks<IntT>();

template <typename IntT>
constexpr auto computeFibonacciNumbers()
{
    constexpr std::size_t size = maxFibonacciNumberIndexForType<IntT>() + 1;
    std::array<IntT, size> numbers{};
    numbers[0] = 0;
    numbers[1] = 1;

    for (std::size_t i = 2; i < size; ++i)
    {
        numbers[i] = numbers[i - 1] + numbers[i - 2];
    }

    return numbers;
}

// F(0) = 0, F(1) = 1
template <typename IntT>
constexpr auto fibonacciNumbers = computeFibonacciNumbers<IntT>();