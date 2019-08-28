#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "Assert.h"
#include "FixedVector.h"

namespace bit
{
    // NOTE: assumes little endian

    static_assert(CHAR_BIT == 8);

    static constexpr std::size_t bitStreamDynamicExtent = 0u;

    // The layout of the serialized bytes.
    // In standard layout bytes are stored in insertion order
    // and bit order within bytes is from most significant
    // bit to the least significant bit.
    //
    // For example say we inserted 11 bits: 0 1 0 0 1 0 0   1 1 0
    // The output consists of 2 bytes.
    //  First byte: 0 1 0 0 1 0 0
    // Second byte: 1 1 0 0 0 0 0
    //
    // NOTE: When inserting a sequence of bits the order of
    //       the bits being inserted MUST be preserved, 
    //       regardless of the layout.
    //       So for example the output after having inserted
    //       a bit sequence 0100100110 is exactly the same as
    //       in the example of single bit packing above.
    struct BitStreamStandardLayoutTag;

    // packing starts from most significant bits.
    // so pushing individual bits 1,0,1,1 gives
    // 1011... == 13 << (numBitsInEntry - 4)
    template <std::size_t MaxNumBitsV = bitStreamDynamicExtent>
    struct BitStream
    {
        using EntryType = std::uint64_t;
        using LayoutType = BitStreamStandardLayoutTag;
        using size_type = std::size_t;

        static constexpr bool isDynamic = (MaxNumBitsV == bitStreamDynamicExtent);

        static constexpr size_type numBitsInEntry = sizeof(EntryType) * CHAR_BIT;

        using EntryStorage = std::conditional_t<
            isDynamic,
            std::vector<EntryType>,
            FixedVector<EntryType, (MaxNumBitsV / numBitsInEntry + (MaxNumBitsV % numBitsInEntry != 0u))>
        >;

        static constexpr auto nbitmasks = []() {
            std::array<EntryType, numBitsInEntry + 1> nbitmasks{};

            for (EntryType i = 0; i < numBitsInEntry; ++i)
            {
                nbitmasks[i] = (static_cast<EntryType>(1u) << i) - 1u;
            }
            nbitmasks[numBitsInEntry] = ~static_cast<EntryType>(0u);

            return nbitmasks;
        }();

        static constexpr auto ones = nbitmasks.back();
        static constexpr auto zeros = nbitmasks.front();

        static constexpr std::size_t initialAllocationSize = 16u;

        BitStream() :
            m_numBits(0)
        {
            if constexpr (isDynamic)
            {
                m_entries.reserve(initialAllocationSize);
            }
        }

        [[nodiscard]] size_type numBits() const
        {
            return m_numBits;
        }

        [[nodiscard]] size_type numBytes() const
        {
            return numBytesToStoreBits(m_numBits);
        }

        [[nodiscard]] size_type numEntries() const
        {
            return numEntriesToStoreBits(m_numBits);
        }

        void clear()
        {
            m_entries.clear();
            m_numBits = 0;
        }

        // On little-endian machines there is a problem caused by the last entry.
        // The least significant bits come first but they are filled last.
        void getBytes(std::byte* out)
        {
            const size_type size = numBytes();
            const size_type numWholeEntries = size / sizeof(EntryType);
            std::memcpy(out, m_entries.data(), numWholeEntries * sizeof(EntryType));

            if (size % sizeof(EntryType))
            {
                // last entry shifted to have least significant bits filled

                const size_type lastEntrySize = size % sizeof(EntryType);
                const size_type lastEntryBits = lastEntrySize * CHAR_BIT;
                const size_type lastEntry = m_entries.back() >> (numBitsInEntry - lastEntryBits);
                std::memcpy(out + numWholeEntries * sizeof(EntryType), reinterpret_cast<const void*>(&lastEntry), lastEntrySize);
            }
        }

        void setBytes(const std::byte* data, size_type size)
        {
            // we don't have to clear the entry vector
            m_numBits = size * CHAR_BIT;
            m_entries.resize(numEntriesToStoreBytes(size));
            m_entries.back() = 0u;
            std::memcpy(m_entries.data(), data, size);

            if (size % sizeof(EntryType))
            {
                // we have to adjust the last entry to start at the msb

                const size_type lastEntryBits = (size % sizeof(EntryType) * CHAR_BIT);
                m_entries.back() <<= numBitsInEntry - lastEntryBits;
            }
        }

        void setBitsFromBytes(const std::byte* data, size_type bits)
        {
            const size_type size = numBytesToStoreBits(bits);
            setBytes(data, size);
            m_numBits = bits;
        }

        // writes length least significant bits of value
        void writeBits(EntryType value, size_type length)
        {
            ASSERT(length <= numBitsInEntry);

            if (length == 0u) return;

            const size_type numFreeBits = numFreeBitsInLastEntry();

            if (numFreeBits == numBitsInEntry)
            {
                // this means that the current entry was not yet 'initialized'
                m_entries.emplace_back(0u);
            }

            // mask so we don't write trash bits
            value &= nbitmasks[length];

            m_numBits += length;

            if (numFreeBits >= length)
            {
                const size_type numFreeBitsAfterInsertion = numFreeBits - length;
                m_entries.back() |= value << numFreeBitsAfterInsertion;
            }
            else
            {
                const size_type numOverflowBits = length - numFreeBits;
                m_entries.back() |= value >> numOverflowBits;

                const size_type numFreeBitsAfterInsertion = numBitsInEntry - numOverflowBits;
                m_entries.emplace_back(value << numFreeBitsAfterInsertion);
            }
        }

        // write as many least significant bits of value as needed to encode it
        void writeBits(EntryType value)
        {
            if (value)
            {
                writeBits(value, intrin::msb(value) + 1u);
            }
        }

        void writeBit(bool b)
        {
            const size_type numFreeBits = numFreeBitsInLastEntry();

            if (numFreeBits == numBitsInEntry)
            {
                // this means that the current entry was not yet 'initialized'
                m_entries.emplace_back(0u);
            }

            m_numBits += 1u;

            const size_type numFreeBitsAfterInsertion = numFreeBits - 1u;
            m_entries.back() |= static_cast<EntryType>(b) << numFreeBitsAfterInsertion;
        }

        void writeBit(bool b, size_type times)
        {
            ASSERT(times <= numBitsInEntry);

            if (times == 0u) return;

            const EntryType value = b ? ones : zeros;
            writeBits(value, times);
        }

        // zero based index
        [[nodiscard]] bool readBit(size_type idx) const
        {
            ASSERT(idx < m_numBits);

            const auto [entryIdx, bitInEntryIdx] = unpackedBitIndex(idx);

            const EntryType entry = m_entries[entryIdx];
            return (entry >> bitInEntryIdx) & 1;
        }

        // zero based index
        [[nodiscard]] EntryType readBits(size_type idx, size_type length) const
        {
            ASSERT(length <= numBitsInEntry);
            ASSERT(idx + length - 1 < m_numBits);

            if (length == 0u) return {};

            const auto [entryIdx, bitInEntryIdx] = unpackedBitIndex(idx);

            const size_type numReadableBitsInThisEntry = bitInEntryIdx + 1u;

            if (numReadableBitsInThisEntry >= length)
            {
                // everything is contained in this single entry

                const size_type numSuperfluousBits = (numReadableBitsInThisEntry - length);
                return (m_entries[entryIdx] >> numSuperfluousBits) & nbitmasks[length];
            }
            else
            {
                // we need to read from 2 entries and assemble the value

                const size_type numLowerBits = (length - numReadableBitsInThisEntry);

                const EntryType upper = m_entries[entryIdx] << numLowerBits;
                const EntryType lower = m_entries[entryIdx + 1u] >> (numBitsInEntry - numLowerBits);
                return (lower | upper) & nbitmasks[length];
            }
        }

        [[nodiscard]] size_type countConsecutive(size_type idx, bool b) const
        {
            const std::uint64_t neg = b ? ones : zeros;

            size_type n = 0u;
            for (; idx < m_numBits;)
            {
                const size_type numBitsLeft = m_numBits - idx;
                const size_type numBitsToRead = std::min(numBitsInEntry, numBitsLeft);
                const EntryType entry = (readBits(idx, numBitsToRead) ^ neg) << (numBitsInEntry - numBitsToRead);

                if (entry)
                {
                    return n + (numBitsInEntry - 1u - intrin::msb(entry));
                }

                idx += numBitsToRead;
                n += numBitsToRead;
            }
            return n;
        }

    private:
        EntryStorage m_entries;
        size_type m_numBits;

        [[nodiscard]] size_type numUsedBitsInLastEntry() const
        {
            return m_numBits % numBitsInEntry;
        }

        [[nodiscard]] size_type numFreeBitsInLastEntry() const
        {
            return numBitsInEntry - numUsedBitsInLastEntry();
        }

        [[nodiscard]] size_type numBytesToStoreBits(size_type numBits) const
        {
            return (numBits + CHAR_BIT - 1u) / CHAR_BIT;
        }

        [[nodiscard]] size_type numEntriesToStoreBits(size_type numBits) const
        {
            return (numBits + numBitsInEntry - 1u) / numBitsInEntry;
        }

        [[nodiscard]] size_type numEntriesToStoreBytes(size_type numBytes) const
        {
            return (numBytes + sizeof(EntryType) - 1u) / sizeof(EntryType);
        }

        // (entryIdx, bitInEntryIdx)
        // least significant bit has index 0
        [[nodiscard]] std::pair<size_type, size_type> unpackedBitIndex(size_type idx) const
        {
            return {
                idx / numBitsInEntry,
                (numBitsInEntry - 1) - idx % numBitsInEntry
            };
        }
    };

    template <typename BS1T, typename BS2T>
    struct AreBitStreamsLayoutCompatibile
    {
        static constexpr bool value = std::is_same_v<typename BS1T::LayoutType, typename BS2T::LayoutType>;
    };

    template <typename BitStreamT>
    struct BitStreamSequentialReader
    {
        using BitStreamType = BitStreamT;
        using size_type = typename BitStreamType::size_type;

        using EntryType = typename BitStreamType::EntryType;

        BitStreamSequentialReader(const BitStreamType& bs) :
            m_bitStream(&bs),
            m_numBitsRead(0u)
        {
        }

        [[nodiscard]] bool readBit()
        {
            const bool b = m_bitStream->readBit(m_numBitsRead);
            m_numBitsRead += 1u;
            return b;
        }

        [[nodiscard]] EntryType readBits(size_type length)
        {
            const EntryType value = m_bitStream->readBits(m_numBitsRead, length);
            m_numBitsRead += length;
            return value;
        }

        [[nodiscard]] bool hasNext(size_type count = 1u) const
        {
            return m_numBitsRead + count <= m_bitStream->numBits();
        }

        [[nodiscard]] bool peekBit()
        {
            return m_bitStream->readBit(m_numBitsRead);
        }

        [[nodiscard]] EntryType peekBits(size_type length)
        {
            return m_bitStream->readBits(m_numBitsRead, length);
        }

        // skips all consecutive bits equal to b
        [[nodiscard]] size_type skipBitsWhileEqualTo(bool b)
        {
            const size_type numSkipped = m_bitStream->countConsecutive(m_numBitsRead, b);
            m_numBitsRead += numSkipped;
            return numSkipped;
        }

        void skipBits(size_type n)
        {
            m_numBitsRead += n;
        }

    private:
        const BitStreamType* m_bitStream;
        size_type m_numBitsRead;
    };
}