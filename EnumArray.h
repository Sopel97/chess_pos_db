#pragma once

#include <cstdint>
#include <iterator>

#include "Enum.h"

// assumes the indices are from 0 to n

template <typename ValueT, typename EnumT, std::size_t SizeV = cardinality<EnumT>()>
struct EnumArray
{
    static_assert(isNaturalIndex<EnumT>(), "Enum must start with 0 and end with cardinality-1.");

    using value_type      = ValueT;
    using size_type       = std::size_t;
    using difference_type = std::ptrdiff_t;
    using pointer         = ValueT *;
    using const_pointer   = const ValueT*;
    using reference       = ValueT &;
    using const_reference = const ValueT &;

    using iterator       = pointer;
    using const_iterator = const_pointer;

    using reverse_iterator       = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    using KeyType = EnumT;
    using ValueType = ValueT;

    constexpr void fill(const ValueType& init)
    {
        for (auto& v : elements)
        {
            v = init;
        }
    }

    [[nodiscard]] constexpr ValueType& operator[](const KeyType& dir)
    {
        return elements[ordinal(dir)];
    }

    [[nodiscard]] constexpr const ValueType& operator[](const KeyType& dir) const
    {
        return elements[ordinal(dir)];
    }

    [[nodiscard]] constexpr ValueType& front()
    {
        return elements[0];
    }

    [[nodiscard]] constexpr const ValueType& front() const
    {
        return elements[0];
    }

    [[nodiscard]] constexpr ValueType& back()
    {
        return elements[SizeV - 1];
    }

    [[nodiscard]] constexpr const ValueType& back() const
    {
        return elements[SizeV - 1];
    }

    [[nodiscard]] constexpr pointer data()
    {
        return elements;
    }

    [[nodiscard]] constexpr const_pointer data() const
    {
        return elements;
    }

    [[nodiscard]] constexpr iterator begin() noexcept 
    {
        return elements;
    }

    [[nodiscard]] constexpr const_iterator begin() const noexcept 
    {
        return elements;
    }

    [[nodiscard]] constexpr iterator end() noexcept 
    {
        return elements + SizeV;
    }

    [[nodiscard]] constexpr const_iterator end() const noexcept 
    {
        return elements + SizeV;
    }

    [[nodiscard]] constexpr reverse_iterator rbegin() noexcept 
    {
        return reverse_iterator(end());
    }

    [[nodiscard]] constexpr const_reverse_iterator rbegin() const noexcept 
    {
        return const_reverse_iterator(end());
    }

    [[nodiscard]] constexpr reverse_iterator rend() noexcept
    {
        return reverse_iterator(begin());
    }

    [[nodiscard]] constexpr const_reverse_iterator rend() const noexcept 
    {
        return const_reverse_iterator(begin());
    }

    [[nodiscard]] constexpr const_iterator cbegin() const noexcept 
    {
        return begin();
    }

    [[nodiscard]] constexpr const_iterator cend() const noexcept 
    {
        return end();
    }

    [[nodiscard]] constexpr const_reverse_iterator crbegin() const noexcept 
    {
        return rbegin();
    }

    [[nodiscard]] constexpr const_reverse_iterator crend() const noexcept 
    {
        return rend();
    }

    [[nodiscard]] constexpr size_type size() const noexcept
    {
        return SizeV;
    }

    ValueT elements[SizeV];
};

template <typename ValueT, typename Enum1T, typename Enum2T, std::size_t Size1V = cardinality<Enum1T>(), std::size_t Size2V = cardinality<Enum2T>()>
using EnumArray2 = EnumArray<EnumArray<ValueT, Enum2T, Size2V>, Enum1T, Size1V>;
