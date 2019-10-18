#pragma once

#include <array>

#include "util/Assert.h"

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
    ASSERT(!EnumTraits<EnumT>::isNaturalIndex || (id >= 0 && id < EnumTraits<EnumT>::cardinality));

    return EnumTraits<EnumT>::fromOrdinal(id);
}

template <typename EnumT>
[[nodiscard]] constexpr typename EnumTraits<EnumT>::IdType ordinal(EnumT v) noexcept
{
    return EnumTraits<EnumT>::ordinal(v);
}

template <typename EnumT>
[[nodiscard]] decltype(auto) toString(EnumT v)
{
    return EnumTraits<EnumT>::toString(v);
}

template <typename EnumT, typename FormatT>
[[nodiscard]] decltype(auto) toString(FormatT&& f, EnumT v)
{
    return EnumTraits<EnumT>::toString(std::forward<FormatT>(f), v);
}

template <typename EnumT>
[[nodiscard]] decltype(auto) fromString(std::string_view str)
{
    return EnumTraits<EnumT>::fromString(str);
}

template <typename EnumT, typename FormatT>
[[nodiscard]] decltype(auto) fromString(FormatT&& f, std::string_view str)
{
    return EnumTraits<EnumT>::fromString(std::forward<FormatT>(f), str);
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