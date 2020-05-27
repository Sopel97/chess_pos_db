#pragma once

#include "ArithmeticUtility.h"
#include "Meta.h"

#include "intrin/Intrinsics.h"

#include <cstdint>

namespace util
{
    namespace detail
    {
        template <typename T>
        [[nodiscard]] constexpr bool hasContiguousSetBits(T v)
        {
            static_assert(std::is_unsigned_v<T>, "Only unsigned types support bit operations.");

            if (v == 0) return true;
            while ((v & T(1)) == 0) v >>= 1;
            return (v & (v + T(1))) == 0;
        }
    }

    // ID may be used to further improve safety.
    // For example to make BitSpans with the same mask
    // but being contained by different PackedInts
    // not being interchangable.
    template <typename T, typename IntT, IntT MaskV, std::size_t IdV = 0>
    struct BitField
    {
        static_assert(std::is_unsigned_v<IntT>, "Bit span may only operate on unsigned types.");
        static_assert(MaskV, "Bit span must consist of at least one bit.");
        static_assert(detail::hasContiguousSetBits(MaskV), "All set bits in the bit span must be contiguous.");
        
        static_assert(std::is_integral_v<T>);

        using UnderlyingType = IntT;
        using ValueType = T;

        static constexpr IntT mask = MaskV;
        static constexpr IntT shift = intrin::lsb_constexpr(MaskV);
        static constexpr IntT size = intrin::popcount_constexpr(MaskV);

        static_assert(sizeof(T) * 8 >= size, "The result type must be large enough to contain the loaded value.");

        static constexpr bool isValueTypeUnsigned = std::is_unsigned_v<T>;

        [[nodiscard]] static constexpr IntT storeSafe(T v)
        {
            // precondition: msb(v) < size
            if constexpr (isValueTypeUnsigned)
            {
                return storeUnsigned(v) & mask;
            }
            else
            {
                return storeUnsigned(static_cast<IntT>(v)) & mask;
            }
        }

        [[nodiscard]] static constexpr IntT store(T v)
        {
            // precondition: msb(v) < size
            if constexpr (isValueTypeUnsigned)
            {
                return storeUnsigned(v);
            }
            else
            {
                // we & with mask to ensure negative values don't spill
                return storeUnsigned(static_cast<IntT>(v)) & mask;
            }
        }

        [[nodiscard]] static constexpr T load(IntT v)
        {
            if constexpr (isValueTypeUnsigned)
            {
                return loadUnsigned(v);
            }
            else
            {
                const auto uns = loadUnsigned(v);
                return signExtend<size, IntT, T>(uns);
            }
        }

        [[nodiscard]] static constexpr IntT loadRaw(IntT v)
        {
            return v & mask;
        }

    private:
        [[nodiscard]] static constexpr IntT storeUnsigned(IntT v)
        {
            // The compiler probably could do it by itself but may as well explicitely
            if constexpr (shift == 0)
            {
                return v;
            }
            else
            {
                return v << shift;
            }
        }

        [[nodiscard]] static constexpr IntT loadUnsigned(IntT v)
        {
            // The compiler probably could do it by itself but may as well explicitely
            if constexpr (shift == 0)
            {
                if constexpr (size == sizeof(IntT) * 8)
                {
                    return v;
                }
                else
                {
                    return v & mask;
                }
            }
            else
            {
                if constexpr (shift + size == sizeof(IntT) * 8)
                {
                    return v >> shift;
                }
                else
                {
                    return (v & mask) >> shift;
                }
            }
        }
    };

    template <typename... BitSpanTs>
    struct PackedInts
    {
        static_assert(
            (intrin::popcount_constexpr((BitSpanTs::mask | ...)))
            == (intrin::popcount_constexpr(BitSpanTs::mask) + ...),
            "Bit spans have overlapping masks.");

        static_assert(meta::AreAllTheSame<typename BitSpanTs::UnderlyingType...>::value);

        using FirstBitSpanT = typename meta::FirstInPack<BitSpanTs...>::type;

        using UnderlyingType = typename FirstBitSpanT::UnderlyingType;

        static constexpr UnderlyingType mask = (BitSpanTs::mask | ...);
        static constexpr UnderlyingType size = (BitSpanTs::size + ...);

        constexpr PackedInts() :
            m_value(0)
        {
        }

        template <typename... LocalBitSpanTs, typename... ValueTs>
        constexpr PackedInts(meta::TypeList<LocalBitSpanTs...>, ValueTs... values) :
            m_value((LocalBitSpanTs::store(values) | ...))
        {
            static_assert(sizeof...(LocalBitSpanTs) == sizeof...(ValueTs), "Incorrect number of parameters.");
            static_assert((meta::IsContained<LocalBitSpanTs, BitSpanTs...>::value && ...), "One or more bit spans don't belong to this type.");
        }

        template <typename... ValueTs>
        constexpr PackedInts(ValueTs... values) :
            m_value((BitSpanTs::store(values) | ...))
        {
            static_assert(sizeof...(BitSpanTs) == sizeof...(ValueTs), "Incorrect number of parameters.");
        }

        constexpr PackedInts(const PackedInts&) = default;
        constexpr PackedInts(PackedInts&&) = default;

        constexpr PackedInts& operator=(const PackedInts&) = default;
        constexpr PackedInts& operator=(PackedInts&&) = default;

        template <typename BitSpanT>
        [[nodiscard]] constexpr auto get()
        {
            static_assert(meta::IsContained<BitSpanT, BitSpanTs...>::value, "The bit span doesn't belong to this type.");
            return BitSpanT::template load(m_value);
        }

        template <typename... LocalBitSpanTs>
        [[nodiscard]] constexpr auto getRaw()
        {
            static_assert((meta::IsContained<LocalBitSpanTs, BitSpanTs...>::value && ...), "The bit span doesn't belong to this type.");
            return m_value & (LocalBitSpanTs::mask | ...);
        }

        template <typename BitSpanT>
        constexpr void init(typename BitSpanT::ValueType v)
        {
            // precondition: value of this type was not yet set
            static_assert(meta::IsContained<BitSpanT, BitSpanTs...>::value, "The bit span doesn't belong to this type.");
            m_value |= BitSpanT::store(v);
        }

        template <typename BitSpanT>
        constexpr void set(typename BitSpanT::ValueType v)
        {
            static_assert(meta::IsContained<BitSpanT, BitSpanTs...>::value, "The bit span doesn't belong to this type.");
            m_value &= ~BitSpanT::mask;
            m_value |= BitSpanT::store(v);
        }

        template <typename BitSpanT>
        constexpr void setSafe(typename BitSpanT::ValueType v)
        {
            static_assert(meta::IsContained<BitSpanT, BitSpanTs...>::value, "The bit span doesn't belong to this type.");
            m_value &= ~BitSpanT::mask;
            m_value |= BitSpanT::storeSafe(v);
        }

        [[nodiscard]] constexpr UnderlyingType value() const
        {
            return m_value;
        }

    private:
        UnderlyingType m_value;
    };
}
