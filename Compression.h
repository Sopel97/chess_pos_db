#pragma once

#include <cstdint>

#include "ArithmeticUtility.h"
#include "BitStream.h"
#include "TypeUtility.h"

namespace bit
{
    // stores ValueT compressed with CodingT as bytes
    // also stores the actual number of bits of the compressed value
    // and restores it to the bit stream when one is created
    // NOTE: This class is NOT an efficient storage for compressed values.
    //       Not only does it store additional data, it also overallocates memory.
    template <
        typename CodingT,
        typename ValueT,
        typename BitStreamT = typename CodingT::template BitStreamType<ValueT>
    >
        struct Compressed : public CodingT
    {
        using BitStreamType = BitStreamT;
        using CodingType = CodingT;

        Compressed() = delete;

        template <typename OtherBitStreamT>
        Compressed(CodingType&& coding, OtherBitStreamT&& bs) :
            CodingType(std::move(coding)),
            m_numBits(bs.numBits())
        {
            static_assert(AreBitStreamsLayoutCompatibile<remove_cvref_t<OtherBitStreamT>, BitStreamType>::value);

            m_bytes.resize(bs.numBytes());
            bs.getBytes(m_bytes.data());
        }

        Compressed(const Compressed& other) = default;
        Compressed(Compressed&&) = default;
        Compressed& operator=(const Compressed& other) = default;
        Compressed& operator=(Compressed&&) = default;

        [[nodiscard]] const BitStreamType bitStream() const
        {
            BitStreamType bs{};
            copyTo(bs);
            return bs;
        }

        template <typename BitStreamT>
        void copyTo(BitStreamT& bs) const
        {
            bs.setBitsFromBytes(m_bytes.data(), m_numBits);
        }

        [[nodiscard]] const CodingType& coding() const
        {
            return *this;
        }

        template <typename OtherBitStreamT>
        void setFromBitStream(CodingT coding, OtherBitStreamT&& bs)
        {
            static_assert(AreBitStreamsLayoutCompatibile<remove_cvref_t<OtherBitStreamT>, BitStreamType>::value);

            CodingType::operator=(std::move(coding));
            m_numBits = bs.numBits();

            // we don't have to clear the bytes vector
            const std::size_t numBytes = bs.numBytes();
            if (m_bytes.capacity() < numBytes)
            {
                m_bytes.reserve(numBytes * 2u);
            }
            m_bytes.resize(numBytes);
            bs.getBytes(m_bytes.data());
        }

        [[nodiscard]] std::size_t numBits() const
        {
            return m_numBits;
        }

        [[nodiscard]] const auto& bytes() const
        {
            return m_bytes;
        }

    private:
        std::size_t m_numBits;
        std::vector<std::byte> m_bytes;
    };

    template <typename CodingT, typename ValueT>
    [[nodiscard]] Compressed<CodingT, ValueT> compress(CodingT coding, const ValueT& value)
    {
        using BitStreamType = typename CodingT::template BitStreamType<ValueT>;
        BitStreamType bs;

        coding.compress(bs, value);

        return { std::move(coding), std::move(bs) };
    }

    // this overload reuses storage allocated by `bs` and `compressed`
    template <typename CodingT, typename ValueT, typename BitStreamT, typename OtherBitStreamT>
    void compress(CodingT coding, const ValueT& value, BitStreamT& bs, Compressed<CodingT, ValueT, OtherBitStreamT>& compressed)
    {
        bs.clear();

        coding.compress(bs, value);

        compressed.setFromBitStream(coding, bs);
    }

    template <typename CodingT, typename ValueT, typename BitStreamT>
    [[nodiscard]] ValueT decompress(const Compressed<CodingT, ValueT, BitStreamT>& compressed)
    {
        const auto bs = compressed.bitStream();
        return compressed.coding().decompress(BitStreamSequentialReader(bs), Type<ValueT>{});
    }

    // this overload reuses storage allocated `bs`
    template <typename CodingT, typename ValueT, typename BitStreamT, typename OtherBitStreamT>
    [[nodiscard]] ValueT decompress(const Compressed<CodingT, ValueT, BitStreamT>& compressed, OtherBitStreamT& bs)
    {
        compressed.copyTo(bs);
        return compressed.coding().decompress(BitStreamSequentialReader(bs), Type<ValueT>{});
    }
}
