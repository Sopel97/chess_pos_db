#pragma once

#include "chess/Bcgn.h"
#include "chess/Date.h"
#include "chess/Eco.h"
#include "chess/GameClassification.h"
#include "chess/Pgn.h"

#include "external_storage/External.h"

#include "util/MemoryAmount.h"

#include <cstdint>
#include <limits>
#include <string_view>
#include <type_traits>

namespace persistence
{
    template <typename GameIndexT>
    struct PackedGameHeader
    {
        static_assert(std::is_unsigned_v<GameIndexT>);

        using GameIndexType = GameIndexT;

        static constexpr std::uint16_t unknownPlyCount = std::numeric_limits<std::uint16_t>::max();

        PackedGameHeader() = default;

        PackedGameHeader(ext::Vector<char>& headers, std::size_t offset);

        PackedGameHeader(const pgn::UnparsedGame& game, GameIndexType gameIdx, std::uint16_t plyCount);

        PackedGameHeader(const pgn::UnparsedGame& game, GameIndexType gameIdx);

        PackedGameHeader(const bcgn::UnparsedBcgnGame& game, GameIndexType gameIdx, std::uint16_t plyCount);

        PackedGameHeader(const bcgn::UnparsedBcgnGame& game, GameIndexType gameIdx);

        [[nodiscard]] const char* data() const;

        [[nodiscard]] std::size_t size() const;

        [[nodiscard]] GameIndexType gameIdx() const;

        [[nodiscard]] GameResult result() const;

        [[nodiscard]] Date date() const;

        [[nodiscard]] Eco eco() const;

        [[nodiscard]] std::uint16_t plyCount() const;

        [[nodiscard]] std::string_view event() const;

        [[nodiscard]] std::string_view white() const;

        [[nodiscard]] std::string_view black() const;

    private:
        static constexpr std::size_t maxStringLength = 255;
        static constexpr std::size_t numPackedStrings = 3;

        static_assert(maxStringLength < 256); // it's nice to require only one byte for length

        GameIndexType m_gameIdx;

        // We just read sizeof(PackedGameHeader), we don't touch anything
        // in packed strings that would be considered 'garbage'
        std::uint16_t m_size;

        GameResult m_result;
        Date m_date;
        Eco m_eco;
        std::uint16_t m_plyCount;

        // strings for event, white, black
        // strings are preceeded with length
        std::uint8_t m_packedStrings[(maxStringLength + 1) * numPackedStrings];

        void fillPackedStrings(std::string_view event, std::string_view white, std::string_view black);
    };

    using PackedGameHeader32 = PackedGameHeader<std::uint32_t>;
    using PackedGameHeader64 = PackedGameHeader<std::uint64_t>;

    static_assert(sizeof(PackedGameHeader32) == 4 + 2 + 2 + 4 + 2 + 2 + 768);
    static_assert(sizeof(PackedGameHeader64) == 8 + 2 + 2 + 4 + 2 + 2 + 768 + 4 /* padding */);

    static_assert(std::is_trivially_copyable_v<PackedGameHeader32>);
    static_assert(std::is_trivially_copyable_v<PackedGameHeader64>);

    extern template struct PackedGameHeader<std::uint32_t>;
    extern template struct PackedGameHeader<std::uint64_t>;
}
