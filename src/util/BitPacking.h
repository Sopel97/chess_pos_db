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
    template <typename IntT, IntT MaskV, std::size_t IdV = 0>
    struct BitSpan
    {
        static_assert(std::is_unsigned_v<IntT>, "Bit span may only operate on unsigned types.");
        static_assert(MaskV, "Bit span must consist of at least one bit.");
        static_assert(detail::hasContiguousSetBits(MaskV), "All set bits in the bit span must be contiguous.");

        using UnsignedInt = IntT;
        using SignedInt = std::make_signed_t<IntT>;

        static constexpr IntT mask = MaskV;
        static constexpr IntT shift = intrin::lsb_constexpr(MaskV);
        static constexpr IntT size = intrin::popcount_constexpr(MaskV);

        template <typename T>
        [[nodiscard]] static constexpr IntT storeSafe(T v)
        {
            // precondition: msb(v) < size
            if constexpr (std::is_signed_v<T>)
            {
                using Unsigned = std::make_unsigned_t<T>;
                return storeUnsigned(static_cast<Unsigned>(v)) & mask;
            }
            else
            {
                return storeUnsigned(v) & mask;
            }
        }

        template <typename T>
        [[nodiscard]] static constexpr IntT store(T v)
        {
            // precondition: msb(v) < size
            if constexpr (std::is_signed_v<T>)
            {
                using Unsigned = std::make_unsigned_t<T>;
                // we & with mask to ensure negative values don't spill
                return storeUnsigned(static_cast<Unsigned>(v)) & mask;
            }
            else
            {
                return storeUnsigned(v);
            }
        }

        template <typename T>
        [[nodiscard]] static constexpr T load(IntT v)
        {
            static_assert(sizeof(T) * 8 >= size, "The result type must be large enough to contain the loaded value.");

            if constexpr (std::is_signed_v<T>)
            {
                using Unsigned = std::make_unsigned_t<T>;
                const auto uns = loadUnsigned<Unsigned>(v);
                return signExtend<size, Unsigned, T>(uns);
            }
            else
            {
                return loadUnsigned<T>(v);
            }
        }

    private:
        template <typename T>
        [[nodiscard]] static constexpr IntT storeUnsigned(T v)
        {
            static_assert(std::is_unsigned_v<T>);

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

        template <typename T>
        [[nodiscard]] static constexpr T loadUnsigned(IntT v)
        {
            static_assert(std::is_unsigned_v<T>);
            static_assert(sizeof(T) * 8 >= size);

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

        static_assert(meta::AreAllTheSame<typename BitSpanTs::UnsignedInt...>::value);

        using FirstBitSpanT = typename meta::FirstInPack<BitSpanTs...>::type;

        using UnsignedInt = typename FirstBitSpanT::UnsignedInt;
        using SignedInt = typename FirstBitSpanT::SignedInt;

        static constexpr UnsignedInt mask = (BitSpanTs::mask | ...);
        static constexpr UnsignedInt size = (BitSpanTs::size + ...);

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

        template <typename BitSpanT, typename T>
        [[nodiscard]] constexpr T get()
        {
            static_assert(meta::IsContained<BitSpanT, BitSpanTs...>::value, "The bit span doesn't belong to this type.");
            return BitSpanT::template load<T>(m_value);
        }

        template <typename BitSpanT, typename T>
        constexpr void init(T v)
        {
            // precondition: value of this type was not yet set
            static_assert(meta::IsContained<BitSpanT, BitSpanTs...>::value, "The bit span doesn't belong to this type.");
            m_value |= BitSpanT::store(v);
        }

        template <typename BitSpanT, typename T>
        constexpr void set(T v)
        {
            static_assert(meta::IsContained<BitSpanT, BitSpanTs...>::value, "The bit span doesn't belong to this type.");
            m_value &= ~BitSpanT::mask;
            m_value |= BitSpanT::store(v);
        }

        template <typename BitSpanT, typename T>
        constexpr void setSafe(T v)
        {
            static_assert(meta::IsContained<BitSpanT, BitSpanTs...>::value, "The bit span doesn't belong to this type.");
            m_value &= ~BitSpanT::mask;
            m_value |= BitSpanT::storeSafe(v);
        }

        [[nodiscard]] constexpr UnsignedInt value() const
        {
            return m_value;
        }

    private:
        UnsignedInt m_value;
    };
}
