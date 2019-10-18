#pragma once

#include <array>
#include <cstdint>
#include <type_traits>
#include <vector>

#include "util/ArithmeticUtility.h"
#include "util/Assert.h"
#include "Compression.h"

namespace bit
{
    template <typename CodingT, typename ValueT, typename EnableT = void>
    struct CompressedSizeUpperBound
    {
        // in bits
        static constexpr std::size_t value = std::numeric_limits<std::size_t>::max();
    };

    template <typename CodingT, typename ValueT, std::size_t SizeV>
    struct CompressedSizeUpperBound<CodingT, std::array<ValueT, SizeV>, void>
    {
        // in bits
        static constexpr std::size_t value = mulSaturate(CompressedSizeUpperBound<CodingT, ValueT>::value, SizeV);
    };

    template <typename CodingT>
    struct Coding
    {
        // if in the worst case compressor would require more bits
        // then we use a dynamic bit stream
        static constexpr std::size_t maxStaticBitStreamSize = CHAR_BIT * 1024u;

        template <typename ValueT>
        using BitStreamType = BitStream<
            CompressedSizeUpperBound<CodingT, ValueT>::value <= maxStaticBitStreamSize
            ? CompressedSizeUpperBound<CodingT, ValueT>::value
            : bitStreamDynamicExtent
        >; 
        
        template <typename ValueT, typename BitStreamT, std::size_t SizeV>
        void compress(BitStreamT& bs, const std::array<ValueT, SizeV>& values) const
        {
            for (const auto& v : values)
            {
                static_cast<const CodingT&>(*this).compress(bs, v);
            }
        }

        template <typename ValueT, typename BitStreamT, typename AllocatorT>
        void compress(BitStreamT& bs, const std::vector<ValueT, AllocatorT>& values) const
        {
            static_cast<const CodingT&>(*this).compress(bs, values.size());
            for (const auto& v : values)
            {
                static_cast<const CodingT&>(*this).compress(bs, v);
            }
        }

        template <typename ValueT, typename ReaderT, std::size_t SizeV>
        [[nodiscard]] std::array<ValueT, SizeV> decompress(ReaderT&& reader, Type<std::array<ValueT, SizeV>>) const
        {
            std::array<ValueT, SizeV> values;

            for (auto& v : values)
            {
                v = static_cast<const CodingT&>(*this).decompress(std::forward<ReaderT>(reader), Type<ValueT>{});
            }

            return values;
        }

        template <typename ValueT, typename ReaderT, typename AllocatorT>
        [[nodiscard]] std::vector<ValueT, AllocatorT> decompress(ReaderT&& reader, Type<std::vector<ValueT, AllocatorT>>) const
        {
            std::vector<ValueT, AllocatorT> values;

            const std::size_t size = static_cast<const CodingT&>(*this).decompress(std::forward<ReaderT>(reader), Type<std::size_t>{});
            values.reserve(size);

            for (std::size_t i = 0; i < size; ++i)
            {
                values.emplace_back(static_cast<const CodingT&>(*this).decompress(std::forward<ReaderT>(reader), Type<ValueT>{}));
            }

            return values;
        }
    };

    struct EliasDeltaCoding : Coding<EliasDeltaCoding>
    {
        // compressed representation of x requires
        // s = floor(log2(x)) + 2*floor(log2(floor(log2(x)) + 1)) + 1
        // bits
        // so:
        // bits    s
        // 1       1   
        // 2       4
        // 4       8
        // 8       14
        // 16      24
        // 32      42
        // 64      76

        using BaseType = Coding<EliasDeltaCoding>;

        using BaseType::compress;
        using BaseType::decompress;

        template <typename IntT, typename BitStreamT>
        std::enable_if_t<std::is_unsigned_v<IntT>, void> compress(BitStreamT& bs, IntT value) const
        {
            ASSERT(value != std::numeric_limits<IntT>::max());
            value += 1u;

            const std::size_t N = floorLog2(value);
            const std::size_t L = floorLog2(N + 1u);

            // we have to write 3 values
            //
            // bs.writeBit(false, L);
            // bs.writeBits(N + 1u, L + L + 1u);
            // bs.writeBits(value, N);
            //
            // but since L is small (at most 6 for 64 bit values)
            // and the bits of N + 1 above L + 1 are already zero
            // we can write the header in one go

            bs.writeBits(N + 1u, L + L + 1u);
            bs.writeBits(value, N);
        }

        template <typename IntT, typename ReaderT>
        [[nodiscard]] std::enable_if_t<std::is_unsigned_v<IntT>, IntT> decompress(ReaderT&& reader, Type<IntT>) const
        {
            const std::size_t L = reader.skipBitsWhileEqualTo(false);
            const std::size_t N = reader.readBits(L + 1u) - 1u;
            const std::size_t value = ((static_cast<std::size_t>(1u) << N) | reader.readBits(N));

            return static_cast<IntT>(value - 1u);
        }

        // maximum size after compression of an integer with numBits bits
        [[nodiscard]] static constexpr std::size_t maxCompressedSize(std::size_t numBits)
        {
            ASSERT(numBits != 0u);

            return (numBits - 1u) + 2u * (floorLog2(numBits)) + 1u;
        }
    };

    template <typename IntT> struct CompressedSizeUpperBound<EliasDeltaCoding, IntT, std::enable_if_t<std::is_integral_v<IntT>>>
    { 
        static constexpr std::size_t value = EliasDeltaCoding::maxCompressedSize(sizeof(IntT) * CHAR_BIT);
    };

    struct FibonacciCoding : Coding<FibonacciCoding>
    {
        using BaseType = Coding<FibonacciCoding>;

        using BaseType::compress;
        using BaseType::decompress;

        template <typename IntT, typename BitStreamT>
        std::enable_if_t<std::is_unsigned_v<IntT>, void> compress(BitStreamT& bs, IntT value) const
        {
            ASSERT(value != std::numeric_limits<IntT>::max());
            value += 1u;

            constexpr auto& fibonacci = fibonacciNumbers<IntT>;
            const auto lastLessOrEqualIter = std::prev(std::upper_bound(std::begin(fibonacci), std::end(fibonacci), value));
            const std::size_t lastLessOrEqualFibIdx = std::distance(std::begin(fibonacci), lastLessOrEqualIter);

            ASSERT(lastLessOrEqualFibIdx >= 2u);
            ASSERT(lastLessOrEqualFibIdx <= 128u); // this should be true unless bigger types are added
            ASSERT(addSaturate(fibonacci[lastLessOrEqualFibIdx], fibonacci[lastLessOrEqualFibIdx - 1u]) >= value); //verify that it is indeed highest we can get below value
            // we need to write `firstLessOrEqualIdx` bits meaning contributions from
            // fibonacci numbers from F(2) to F(lastLessOrEqualFibIdx); and a 1 bit at the end

            // function to process one bit and increment counters
            auto processBit = [&fibonacci](std::size_t& fibIdx, std::size_t& bitIdx, std::uint64_t& compressed, IntT& value)
            {
                /*
                if (fibonacci[fibIdx] <= value)
                {
                    value -= fibonacci[fibIdx];
                    compressed |= static_cast<std::uint64_t>(1u) << bitIdx;

                    // Zeckendorf's theorem tells us that fibonacci[fibIdx - 1u] won't fit in the new value
                    fibIdx -= 2u;
                    bitIdx += 2u;
                }
                else
                {
                    fibIdx -= 1u;
                    bitIdx += 1u;
                }
                */

                // a branchless alternative for the above
                // TODO: benchmark
                const auto b = static_cast<std::uint64_t>(fibonacci[fibIdx] <= value);
                value -= fibonacci[fibIdx] * static_cast<IntT>(b);
                compressed |= b << bitIdx;
                fibIdx -= 1u + b;
                bitIdx += 1u + b;
            };

            if (lastLessOrEqualFibIdx <= 64u)
            {
                // we can pack it in a single first uint64 instead of pushing bit by bit
                std::uint64_t compressed = 1u;
                std::size_t fibIdx = lastLessOrEqualFibIdx;
                std::size_t bitIdx = 1u;
                while (fibIdx >= 2u)
                {
                    processBit(fibIdx, bitIdx, compressed, value);
                }

                bs.writeBits(compressed, lastLessOrEqualFibIdx);
            }
            else
            {
                // we need 2 64bit integers to hold the result
                std::uint64_t compressedLow = 1u;
                std::uint64_t compressedHigh = 0u;
                std::size_t fibIdx = lastLessOrEqualFibIdx;
                std::size_t bitIdx = 1u;
                while (bitIdx < 64u)
                {
                    processBit(fibIdx, bitIdx, compressedLow, value);
                }

                bitIdx &= 1u; // bitIdx -= 64; // bitIdx is either 64 or 65 at this point
                while (fibIdx >= 2u)
                {
                    processBit(fibIdx, bitIdx, compressedHigh, value);
                }

                bs.writeBits(compressedHigh, lastLessOrEqualFibIdx - 64u);
                bs.writeBits(compressedLow, 64u);
            }
        }

        template <typename IntT, typename ReaderT>
        [[nodiscard]] std::enable_if_t<std::is_unsigned_v<IntT>, IntT> decompress(ReaderT&& reader, Type<IntT>) const
        {
            IntT value = 0u;

            constexpr auto& fibonacci = fibonacciNumbers<IntT>;
            bool prevBit = false;
            for (std::size_t fibIdx = 2u; ; ++fibIdx)
            {
                const bool bit = reader.readBit();
                // if two consecutive bits are 1 then we hit the end of the compressed value
                if (prevBit && bit) break;

                ASSERT(fibIdx < fibonacci.size());

                if (bit)
                {
                    value += fibonacci[fibIdx];
                }

                prevBit = bit;
            }

            return value - 1;
        }

        // maximum size after compression of an integer with numBits bits
        [[nodiscard]] static constexpr std::size_t maxCompressedSize(std::size_t numBits)
        {
            ASSERT(numBits != 0u);
            ASSERT(numBits <= 64u);

            constexpr auto& fibonacci = fibonacciNumbers<uint64_t>;
            for (std::size_t i = fibonacci.size() - 1; i > 0; --i)
            {
                if (fibonacci[i] <= nbitmask<uint64_t>[numBits])
                {
                    return i;
                }
            }
        }
    };

    template <typename IntT> struct CompressedSizeUpperBound<FibonacciCoding, IntT, std::enable_if_t<std::is_integral_v<IntT>>>
    {
        static constexpr std::size_t value = FibonacciCoding::maxCompressedSize(sizeof(IntT) * CHAR_BIT);
    };

    struct EliasGammaCoding : Coding<EliasGammaCoding>
    {
        // requires 2*floor(log2(x))+1 bits

        using BaseType = Coding<EliasGammaCoding>;

        using BaseType::compress;
        using BaseType::decompress;

        template <typename IntT, typename BitStreamT>
        std::enable_if_t<std::is_unsigned_v<IntT>, void> compress(BitStreamT& bs, IntT value) const
        {
            ASSERT(value != std::numeric_limits<IntT>::max());
            value += 1u;

            const std::size_t N = floorLog2(value);

            bs.writeBit(false, N);
            bs.writeBits(value, N + 1u);
        }

        template <typename IntT, typename ReaderT>
        [[nodiscard]] std::enable_if_t<std::is_unsigned_v<IntT>, IntT> decompress(ReaderT&& reader, Type<IntT>) const
        {
            const std::size_t N = reader.skipBitsWhileEqualTo(false);
            const std::uint64_t value = reader.readBits(N + 1u);

            return static_cast<IntT>(value - 1u);
        }

        // maximum size after compression of an integer with numBits bits
        [[nodiscard]] static constexpr std::size_t maxCompressedSize(std::size_t numBits)
        {
            ASSERT(numBits != 0u);

            return 2u * (numBits - 1u) + 1u;
        }
    };

    template <typename IntT> struct CompressedSizeUpperBound<EliasGammaCoding, IntT, std::enable_if_t<std::is_integral_v<IntT>>>
    {
        static constexpr std::size_t value = EliasGammaCoding::maxCompressedSize(sizeof(IntT) * CHAR_BIT);
    };

    struct EliasOmegaCoding : Coding<EliasOmegaCoding>
    {
        using BaseType = Coding<EliasOmegaCoding>;

        using BaseType::compress;
        using BaseType::decompress;

        template <typename IntT, typename BitStreamT>
        std::enable_if_t<std::is_unsigned_v<IntT>, void> compress(BitStreamT& bs, IntT value0) const
        {
            ASSERT(value0 != std::numeric_limits<IntT>::max());
            value0 += 1u;

            // value       bits written
            // 2^2^16-1    2^16
            // 2^16-1	   16
            // 15          4
            // 3           2 end
            //
            // 1           1 always at the end
            //
            // the max bits stack depth is small so we can 'unroll' the recursion
            // we have specialized handling for each range

            // value[N] == len(value[N-1]) - 1
            // values are encoded in the order:
            // value3 value2 value1 value0 0

            if (sizeof(IntT) > 2u && value0 > 0xFFFFu)
            {
                const std::uint64_t value1 = floorLog2(value0);
                const std::uint64_t value2 = floorLog2(value1);
                const std::uint64_t value3 = floorLog2(value2);

                // manually pack the header since it will fit in 64 bits
                std::uint64_t header = value3;
                header = (header << (value3 + 1u)) | value2;
                header = (header << (value2 + 1u)) | value1;

                bs.writeBits(header, (value2 + 1u) + (value3 + 1u) + 2u);
                bs.writeBits(value0, value1 + 1u);
            }
            else if (value0 > 15u)
            {
                const std::uint64_t value1 = floorLog2(value0);
                const std::uint64_t value2 = floorLog2(value1);

                // manually pack eveything since it will fit in 64 bits
                std::uint64_t header = value2;
                header = (header << (value2 + 1u)) | value1;
                header = (header << (value1 + 1u)) | value0;

                bs.writeBits(header, (value1 + 1u) + (value2 + 1u) + 2u);
            }
            else if (value0 > 3u)
            {
                const std::uint64_t value1 = floorLog2(value0);

                // manually pack eveything since it will fit in 64 bits
                std::uint64_t header = value1;
                header = (header << (value1 + 1u)) | value0;

                bs.writeBits(header, (value1 + 1u) + 2u);
            }
            else if (value0 > 1u)
            {
                bs.writeBits(value0, 2u);
            }

            bs.writeBit(false);
        }

        template <typename IntT, typename ReaderT>
        [[nodiscard]] std::enable_if_t<std::is_unsigned_v<IntT>, IntT> decompress(ReaderT&& reader, Type<IntT>) const
        {
            // use goto here because we need to clean up the peeked bit
            // and doing it 5 times inline is ugly

            std::uint64_t value = 1;
            if (!reader.peekBit()) goto ret;
            value = reader.readBits(value + 1u);
            if (!reader.peekBit()) goto ret;
            value = reader.readBits(value + 1u);
            if (!reader.peekBit()) goto ret;
            value = reader.readBits(value + 1u);
            if constexpr (sizeof(IntT) > 2u)
            {
                if (!reader.peekBit()) goto ret;
                value = reader.readBits(value + 1u);
            }

        ret:
            ASSERT(!reader.peekBit()); // at most 4 levels of recursion
            reader.skipBits(1u);
            return static_cast<IntT>(value - 1u);
        }

        // maximum size after compression of an integer with numBits bits
        [[nodiscard]] static constexpr std::size_t maxCompressedSize(std::size_t numBits)
        {
            switch (numBits)
            {
            case 8u:
                return 14u;
            case 16u:
                return 23u;
            case 32u:
                return 43u;
            case 64u:
                return 76u;
            }

            // we don't care about other sizes, and there is no easy formula
            return std::numeric_limits<std::size_t>::max();
        }
    };

    template <typename IntT> struct CompressedSizeUpperBound<EliasOmegaCoding, IntT, std::enable_if_t<std::is_integral_v<IntT>>>
    {
        static constexpr std::size_t value = EliasOmegaCoding::maxCompressedSize(sizeof(IntT) * CHAR_BIT);
    };

    template <std::size_t OrderV = 0u>
    struct ExpGolombCoding : Coding<ExpGolombCoding<OrderV>>
    {
        static_assert(OrderV < 64u);

        using BaseType = Coding<ExpGolombCoding<OrderV>>;

        using BaseType::compress;
        using BaseType::decompress;

        template <typename IntT, typename BitStreamT>
        std::enable_if_t<std::is_unsigned_v<IntT>, void> compress(BitStreamT& bs, IntT value) const
        {
            static_assert(sizeof(IntT) * CHAR_BIT > OrderV);

            const IntT quotient = value >> OrderV;
            EliasGammaCoding{}.compress(bs, quotient);

            if constexpr (OrderV > 0)
            {
                const IntT remainder = value & nbitmask<IntT>[OrderV];
                bs.writeBits(remainder, OrderV);
            }
        }

        template <typename IntT, typename ReaderT>
        [[nodiscard]] std::enable_if_t<std::is_unsigned_v<IntT>, IntT> decompress(ReaderT&& reader, Type<IntT>) const
        {
            std::uint64_t value = EliasGammaCoding{}.decompress(reader, Type<IntT>{});

            if constexpr (OrderV > 0u)
            {
                value = (value << OrderV) | reader.readBits(OrderV);
            }

            return static_cast<IntT>(value);
        }

        // maximum size after compression of an integer with numBits bits
        [[nodiscard]] static constexpr std::size_t maxCompressedSize(std::size_t numBits)
        {
            ASSERT(numBits > 0u);
            ASSERT(numBits <= 64u);

            // we can't code intmax
            const std::uint64_t value = nbitmask<std::uint64_t>[numBits] - 1u;
            return 2u * (floorLog2((value >> OrderV) + 1u)) + 1u + OrderV;
        }
    };

    template <std::size_t OrderV, typename IntT> struct CompressedSizeUpperBound<ExpGolombCoding<OrderV>, IntT, std::enable_if_t<std::is_integral_v<IntT>>>
    {
        static constexpr std::size_t value = ExpGolombCoding<OrderV>::maxCompressedSize(sizeof(IntT) * CHAR_BIT);
    };

    // GroupSizeV denotes the number of data bits in one part
    template <std::size_t GroupSizeV = 7u>
    struct VariableLengthCoding : Coding<VariableLengthCoding<GroupSizeV>>
    {
        // sizes greater than 33 don't make much sense
        static_assert(GroupSizeV > 0u && GroupSizeV < 33u);

        using BaseType = Coding<VariableLengthCoding<GroupSizeV>>;

        using BaseType::compress;
        using BaseType::decompress;

        template <typename IntT, typename BitStreamT>
        std::enable_if_t<std::is_unsigned_v<IntT>, void> compress(BitStreamT& bs, IntT value) const
        {
            static_assert(sizeof(IntT) * CHAR_BIT / 2u + 1u >= GroupSizeV);

            // we store the continuation bit as the LSB of a part

            constexpr IntT groupMask = nbitmask<IntT>[GroupSizeV];

            for (;;)
            {
                const IntT group = (value & groupMask) << 1u;
                value >>= GroupSizeV;
                if (value)
                {
                    bs.writeBits(group | 1u, GroupSizeV + 1u);
                }
                else
                {
                    bs.writeBits(group, GroupSizeV + 1u);
                    break;
                }
            }
        }

        template <typename IntT, typename ReaderT>
        [[nodiscard]] std::enable_if_t<std::is_unsigned_v<IntT>, IntT> decompress(ReaderT&& reader, Type<IntT>) const
        {
            std::uint64_t value = 0;

            for (std::size_t nextBitIdx = 0;; nextBitIdx += GroupSizeV)
            {
                const std::uint64_t part = reader.readBits(GroupSizeV + 1u);

                value |= (part >> 1u) << nextBitIdx;

                if (!(part & 1u))
                {
                    break;
                }
            }

            return static_cast<IntT>(value);
        }

        // maximum size after compression of an integer with numBits bits
        [[nodiscard]] static constexpr std::size_t maxCompressedSize(std::size_t numBits)
        {
            ASSERT(numBits > 0u);

            const std::size_t numParts = (numBits + GroupSizeV - 1u) / GroupSizeV;
            const std::size_t partSize = GroupSizeV + 1u;
            return numParts * partSize;
        }
    };

    template <std::size_t GroupSizeV, typename IntT> struct CompressedSizeUpperBound<VariableLengthCoding<GroupSizeV>, IntT, std::enable_if_t<std::is_integral_v<IntT>>>
    {
        static constexpr std::size_t value = VariableLengthCoding<GroupSizeV>::maxCompressedSize(sizeof(IntT) * CHAR_BIT);
    };
}
