#include "PackedGameHeader.h"

namespace persistence
{
    template <typename GameIndexT>
    PackedGameHeader<GameIndexT>::PackedGameHeader(ext::Vector<char>& headers, std::size_t offset) :
        m_gameIdx{},
        m_size{},
        m_result{},
        m_date{},
        m_eco{},
        m_plyCount{},
        m_packedStrings{}
    {
        // there may be garbage at the end
        // we don't care because we have sizes serialized
        const std::size_t read = headers.read(reinterpret_cast<char*>(this), offset, sizeof(PackedGameHeader));
        ASSERT(m_size <= read);
        (void)read;
    }

    template <typename GameIndexT>
    PackedGameHeader<GameIndexT>::PackedGameHeader(const pgn::UnparsedGame& game, GameIndexT gameIdx, std::uint16_t plyCount) :
        m_gameIdx(gameIdx),
        m_plyCount(plyCount)
    {
        std::string_view event;
        std::string_view white;
        std::string_view black;
        std::optional<GameResult> result;
        game.getResultDateEcoEventWhiteBlack(result, m_date, m_eco, event, white, black);
        m_result = *result;
        fillPackedStrings(event, white, black);
    }

    template <typename GameIndexT>
    PackedGameHeader<GameIndexT>::PackedGameHeader(const pgn::UnparsedGame& game, GameIndexT gameIdx) :
        m_gameIdx(gameIdx)
    {
        std::string_view event;
        std::string_view white;
        std::string_view black;
        std::optional<GameResult> result;
        game.getResultDateEcoEventWhiteBlackPlyCount(result, m_date, m_eco, event, white, black, m_plyCount);
        m_result = *result;
        fillPackedStrings(event, white, black);
    }

    template <typename GameIndexT>
    PackedGameHeader<GameIndexT>::PackedGameHeader(const bcgn::UnparsedBcgnGame& game, GameIndexT gameIdx, std::uint16_t plyCount) :
        m_gameIdx(gameIdx),
        m_plyCount(plyCount)
    {
        auto header = game.gameHeader();
        m_date = header.date();
        m_eco = header.eco();
        std::string_view event = header.event();
        std::string_view white = header.whitePlayer();
        std::string_view black = header.blackPlayer();
        m_result = *header.result();
        fillPackedStrings(event, white, black);
    }

    template <typename GameIndexT>
    PackedGameHeader<GameIndexT>::PackedGameHeader(const bcgn::UnparsedBcgnGame& game, GameIndexT gameIdx) :
        m_gameIdx(gameIdx)
    {
        auto header = game.gameHeader();
        m_date = header.date();
        m_eco = header.eco();
        std::string_view event = header.event();
        std::string_view white = header.whitePlayer();
        std::string_view black = header.blackPlayer();
        m_result = *header.result();
        fillPackedStrings(event, white, black);
    }

    template <typename GameIndexT>
    [[nodiscard]] const char* PackedGameHeader<GameIndexT>::data() const
    {
        return reinterpret_cast<const char*>(this);
    }

    template <typename GameIndexT>
    [[nodiscard]] std::size_t PackedGameHeader<GameIndexT>::size() const
    {
        return m_size;
    }

    template <typename GameIndexT>
    [[nodiscard]] GameIndexT PackedGameHeader<GameIndexT>::gameIdx() const
    {
        return m_gameIdx;
    }

    template <typename GameIndexT>
    [[nodiscard]] GameResult PackedGameHeader<GameIndexT>::result() const
    {
        return m_result;
    }

    template <typename GameIndexT>
    [[nodiscard]] Date PackedGameHeader<GameIndexT>::date() const
    {
        return m_date;
    }

    template <typename GameIndexT>
    [[nodiscard]] Eco PackedGameHeader<GameIndexT>::eco() const
    {
        return m_eco;
    }

    template <typename GameIndexT>
    [[nodiscard]] std::uint16_t PackedGameHeader<GameIndexT>::plyCount() const
    {
        return m_plyCount;
    }

    template <typename GameIndexT>
    [[nodiscard]] std::string_view PackedGameHeader<GameIndexT>::event() const
    {
        const std::uint8_t length = m_packedStrings[0];
        return std::string_view(reinterpret_cast<const char*>(&m_packedStrings[1]), length);
    }

    template <typename GameIndexT>
    [[nodiscard]] std::string_view PackedGameHeader<GameIndexT>::white() const
    {
        const std::uint8_t length0 = m_packedStrings[0];
        const std::uint8_t length = m_packedStrings[length0 + 1];
        return std::string_view(reinterpret_cast<const char*>(&m_packedStrings[length0 + 2]), length);
    }

    template <typename GameIndexT>
    [[nodiscard]] std::string_view PackedGameHeader<GameIndexT>::black() const
    {
        const std::uint8_t length0 = m_packedStrings[0];
        const std::uint8_t length1 = m_packedStrings[length0 + 1];
        const std::uint8_t length = m_packedStrings[length0 + length1 + 2];
        return std::string_view(reinterpret_cast<const char*>(&m_packedStrings[length0 + length1 + 3]), length);
    }

    template <typename GameIndexT>
    void PackedGameHeader<GameIndexT>::fillPackedStrings(std::string_view event, std::string_view white, std::string_view black)
    {
        using namespace std::literals;

        const std::uint8_t eventSize = static_cast<std::uint8_t>(std::min(event.size(), maxStringLength));
        const std::uint8_t whiteSize = static_cast<std::uint8_t>(std::min(white.size(), maxStringLength));
        const std::uint8_t blackSize = static_cast<std::uint8_t>(std::min(black.size(), maxStringLength));

        std::uint8_t i = 0;
        m_packedStrings[i++] = eventSize;
        event.copy(reinterpret_cast<char*>(&m_packedStrings[i]), eventSize);
        i += eventSize;

        m_packedStrings[i++] = whiteSize;
        white.copy(reinterpret_cast<char*>(&m_packedStrings[i]), whiteSize);
        i += whiteSize;

        m_packedStrings[i++] = blackSize;
        black.copy(reinterpret_cast<char*>(&m_packedStrings[i]), blackSize);
        i += blackSize;

        m_size = sizeof(PackedGameHeader) - sizeof(m_packedStrings) + i;
    }

    template struct PackedGameHeader<std::uint32_t>;
    template struct PackedGameHeader<std::uint64_t>;
}