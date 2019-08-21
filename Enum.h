#pragma once

#include <array>

template <typename EnumT>
struct EnumTraits;

template <typename EnumT>
[[nodiscard]] constexpr bool isNaturalIndex() noexcept
{
    return EnumTraits<EnumT>::isNaturalIndex;
}

template <typename EnumT>
[[nodiscard]] constexpr int cardinality() noexcept
{
    return EnumTraits<EnumT>::cardinality;
}

template <typename EnumT>
[[nodiscard]] constexpr const std::array<EnumT, cardinality<EnumT>()>& values() noexcept
{
    return EnumTraits<EnumT>::values;
}

template <typename EnumT>
[[nodiscard]] constexpr EnumT fromOrdinal(int id) noexcept
{
    return EnumTraits<EnumT>::fromOrdinal(id);
}

template <typename EnumT>
[[nodiscard]] constexpr typename EnumTraits<EnumT>::IdType ordinal(EnumT v) noexcept
{
    return EnumTraits<EnumT>::ordinal(v);
}

template <>
struct EnumTraits<bool>
{
    using IdType = int;
    using EnumType = bool;

    static constexpr int cardinality = 2;
    static constexpr bool isNaturalIndex = true;

    static constexpr std::array<EnumType, cardinality> values{
        false,
        true
    };

    [[nodiscard]] static constexpr int ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        return static_cast<EnumType>(id);
    }
};