#pragma once

#include <array>
#include <vector>
#include <iostream>

#include "Assert.h"
#include "Compression.h"
#include "Coding.h"

namespace test
{
    inline void testEliasDeltaCoding()
    {
        for (std::size_t n = 0u; n < 128u; ++n)
        {
            ASSERT(bit::decompress(bit::compress(bit::EliasDeltaCoding{}, n)) == n);
        }

        for (std::size_t n = 1u; n < 128u; ++n)
        {
            const std::size_t value = static_cast<std::size_t>(std::pow(1.414, n));
            ASSERT(bit::decompress(bit::compress(bit::EliasDeltaCoding{}, value)) == value);
        }

        ASSERT((bit::compress(bit::EliasDeltaCoding{}, 0xFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasDeltaCoding, std::uint8_t>::value));
        ASSERT((bit::compress(bit::EliasDeltaCoding{}, 0xFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasDeltaCoding, std::uint16_t>::value));
        ASSERT((bit::compress(bit::EliasDeltaCoding{}, 0xFFFFFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasDeltaCoding, std::uint32_t>::value));
        ASSERT((bit::compress(bit::EliasDeltaCoding{}, 0xFFFFFFFFFFFFFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasDeltaCoding, std::uint64_t>::value));

        {
            std::array<std::uint32_t, 3u> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::EliasDeltaCoding{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
            ASSERT(compressed.numBits() == 72u);
        }

        {
            std::vector<std::uint32_t> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::EliasDeltaCoding{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
            ASSERT(compressed.numBits() == 77u);
        }
    }

    inline void testFibonacciCoding()
    {
        for (std::size_t n = 0u; n < 128u; ++n)
        {
            ASSERT(bit::decompress(bit::compress(bit::FibonacciCoding{}, n)) == n);
        }

        for (std::size_t n = 1u; n < 128u; ++n)
        {
            const std::size_t value = static_cast<std::size_t>(std::pow(1.414, n));
            ASSERT(bit::decompress(bit::compress(bit::FibonacciCoding{}, value)) == value);
        }

        ASSERT((bit::compress(bit::FibonacciCoding{}, 0xFEu).numBits() == bit::CompressedSizeUpperBound<bit::FibonacciCoding, std::uint8_t>::value));
        ASSERT((bit::compress(bit::FibonacciCoding{}, 0xFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::FibonacciCoding, std::uint16_t>::value));
        ASSERT((bit::compress(bit::FibonacciCoding{}, 0xFFFFFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::FibonacciCoding, std::uint32_t>::value));
        ASSERT((bit::compress(bit::FibonacciCoding{}, 0xFFFFFFFFFFFFFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::FibonacciCoding, std::uint64_t>::value));

        {
            std::array<std::uint32_t, 3u> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::FibonacciCoding{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
            ASSERT(compressed.numBits() == 73);
        }

        {
            std::vector<std::uint32_t> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::FibonacciCoding{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
            ASSERT(compressed.numBits() == 77u);
        }
    }

    inline void testEliasGammaCoding()
    {
        for (std::size_t n = 0u; n < 128u; ++n)
        {
            ASSERT(bit::decompress(bit::compress(bit::EliasGammaCoding{}, n)) == n);
        }

        for (std::size_t n = 1u; n < 128u; ++n)
        {
            const std::size_t value = static_cast<std::size_t>(std::pow(1.414, n));
            ASSERT(bit::decompress(bit::compress(bit::EliasGammaCoding{}, value)) == value);
        }

        ASSERT((bit::compress(bit::EliasGammaCoding{}, 0xFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasGammaCoding, std::uint8_t>::value));
        ASSERT((bit::compress(bit::EliasGammaCoding{}, 0xFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasGammaCoding, std::uint16_t>::value));
        ASSERT((bit::compress(bit::EliasGammaCoding{}, 0xFFFFFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasGammaCoding, std::uint32_t>::value));
        ASSERT((bit::compress(bit::EliasGammaCoding{}, 0xFFFFFFFFFFFFFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasGammaCoding, std::uint64_t>::value));

        {
            std::array<std::uint32_t, 3u> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::EliasGammaCoding{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
            ASSERT(compressed.numBits() == 97u);
        }

        {
            std::vector<std::uint32_t> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::EliasGammaCoding{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
            ASSERT(compressed.numBits() == 102u);
        }
    }

    inline void testEliasOmegaCoding()
    {
        for (std::size_t n = 0u; n < 128u; ++n)
        {
            ASSERT(bit::decompress(bit::compress(bit::EliasOmegaCoding{}, n)) == n);
        }

        for (std::size_t n = 1u; n < 128u; ++n)
        {
            const std::size_t value = static_cast<std::size_t>(std::pow(1.414, n));
            ASSERT(bit::decompress(bit::compress(bit::EliasOmegaCoding{}, value)) == value);
        }

        ASSERT((bit::compress(bit::EliasOmegaCoding{}, 0xFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasOmegaCoding, std::uint8_t>::value));
        ASSERT((bit::compress(bit::EliasOmegaCoding{}, 0xFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasOmegaCoding, std::uint16_t>::value));
        ASSERT((bit::compress(bit::EliasOmegaCoding{}, 0xFFFFFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasOmegaCoding, std::uint32_t>::value));
        ASSERT((bit::compress(bit::EliasOmegaCoding{}, 0xFFFFFFFFFFFFFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::EliasOmegaCoding, std::uint64_t>::value));

        {
            std::array<std::uint32_t, 3u> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::EliasOmegaCoding{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
            ASSERT(compressed.numBits() == 79u);
        }

        {
            std::vector<std::uint32_t> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::EliasOmegaCoding{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
            ASSERT(compressed.numBits() == 85u);
        }
    }

    template <std::size_t OrderV>
    void testExpGolombCoding()
    {
        for (std::size_t n = 0u; n < 128u; ++n)
        {
            ASSERT(bit::decompress(bit::compress(bit::ExpGolombCoding<OrderV>{}, n)) == n);
        }

        for (std::size_t n = 1u; n < 128u; ++n)
        {
            const std::size_t value = static_cast<std::size_t>(std::pow(1.414, n));
            ASSERT(bit::decompress(bit::compress(bit::ExpGolombCoding<OrderV>{}, value)) == value);
        }

        ASSERT((bit::compress(bit::ExpGolombCoding<OrderV>{}, 0xFEu).numBits() == bit::CompressedSizeUpperBound<bit::ExpGolombCoding<OrderV>, std::uint8_t>::value));
        ASSERT((bit::compress(bit::ExpGolombCoding<OrderV>{}, 0xFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::ExpGolombCoding<OrderV>, std::uint16_t>::value));
        ASSERT((bit::compress(bit::ExpGolombCoding<OrderV>{}, 0xFFFFFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::ExpGolombCoding<OrderV>, std::uint32_t>::value));
        ASSERT((bit::compress(bit::ExpGolombCoding<OrderV>{}, 0xFFFFFFFFFFFFFFFEu).numBits() == bit::CompressedSizeUpperBound<bit::ExpGolombCoding<OrderV>, std::uint64_t>::value));

        {
            std::array<std::uint32_t, 3u> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::ExpGolombCoding<OrderV>{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
        }

        {
            std::vector<std::uint32_t> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::ExpGolombCoding<OrderV>{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
        }
    }

    template <std::size_t GroupSizeV>
    void testVariableLengthCoding()
    {
        for (std::size_t n = 0u; n < 128u; ++n)
        {
            ASSERT(bit::decompress(bit::compress(bit::VariableLengthCoding<GroupSizeV>{}, n)) == n);
        }

        for (std::size_t n = 1u; n < 128u; ++n)
        {
            const std::size_t value = static_cast<std::size_t>(std::pow(1.414, n));
            ASSERT(bit::decompress(bit::compress(bit::VariableLengthCoding<GroupSizeV>{}, value)) == value);
        }

        ASSERT((bit::compress(bit::VariableLengthCoding<GroupSizeV>{}, 0xFFu).numBits() == bit::CompressedSizeUpperBound<bit::VariableLengthCoding<GroupSizeV>, std::uint8_t>::value));
        ASSERT((bit::compress(bit::VariableLengthCoding<GroupSizeV>{}, 0xFFFFu).numBits() == bit::CompressedSizeUpperBound<bit::VariableLengthCoding<GroupSizeV>, std::uint16_t>::value));
        ASSERT((bit::compress(bit::VariableLengthCoding<GroupSizeV>{}, 0xFFFFFFFFu).numBits() == bit::CompressedSizeUpperBound<bit::VariableLengthCoding<GroupSizeV>, std::uint32_t>::value));
        ASSERT((bit::compress(bit::VariableLengthCoding<GroupSizeV>{}, 0xFFFFFFFFFFFFFFFFu).numBits() == bit::CompressedSizeUpperBound<bit::VariableLengthCoding<GroupSizeV>, std::uint64_t>::value));

        {
            std::array<std::uint32_t, 3u> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::VariableLengthCoding<GroupSizeV>{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
        }

        {
            std::vector<std::uint32_t> uncompressed{ 412312u, 652342u, 1421u };
            auto compressed = bit::compress(bit::VariableLengthCoding<GroupSizeV>{}, uncompressed);
            auto decompressed = bit::decompress(compressed);
            ASSERT(uncompressed == decompressed);
        }
    }

    void runCodingTests()
    {
        testEliasDeltaCoding();
        testFibonacciCoding();
        testEliasGammaCoding();
        testEliasOmegaCoding();
        testExpGolombCoding<0u>();
        testExpGolombCoding<1u>();
        testExpGolombCoding<2u>();
        testExpGolombCoding<4u>();
        testExpGolombCoding<8u>();
        testVariableLengthCoding<1u>();
        testVariableLengthCoding<2u>();
        testVariableLengthCoding<3u>();
        testVariableLengthCoding<7u>();
        testVariableLengthCoding<15u>();
        std::cerr << "CodingTests Passed\n";
    }
}
