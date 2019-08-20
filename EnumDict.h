#pragma once

#include <Enum.h>

// assumes the indices are from 0 to n

template <typename EnumT, typename ValueT, int SizeV = cardinality<EnumT>()>
struct EnumArray : public std::array<EnumT, SizeV>
{
    static_assert(isNaturalIndex<EnumT>(), "Enum must start with 0 and end with cardinality-1.");

    using BaseType = std::array<EnumT, SizeV>;
    using KeyType = EnumT;
    using ValueType = ValueT;

    typename BaseType::reference operator[](typename BaseType::size_type) = delete;
    typename BaseType::const_reference operator[](typename BaseType::size_type) const = delete;
    typename BaseType::reference at(typename BaseType::size_type) = delete;
    typename BaseType::const_reference at(typename BaseType::size_type) const = delete;

    [[nodiscard]] constexpr T& operator[](const KeyType& dir)
    {
        return BaseType::operator[](ordinal(dir));
    }

    [[nodiscard]] constexpr const T& operator[](const KeyType& dir) const
    {
        return BaseType::operator[](ordinal(dir));
    }
}
