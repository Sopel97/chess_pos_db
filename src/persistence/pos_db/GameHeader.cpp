#include "GameHeader.h"

#include "chess/Date.h"
#include "chess/Eco.h"

#include <cstdint>
#include <limits>
#include <string_view>

#include "json/json.hpp"

namespace persistence
{
    GameHeader::GameHeader(
        std::uint64_t gameIdx,
        GameResult result,
        Date date,
        Eco eco,
        std::uint16_t plyCount,
        std::string event,
        std::string white,
        std::string black
    ) :
        m_gameIdx(gameIdx),
        m_result(result),
        m_date(date),
        m_eco(eco),
        m_plyCount(plyCount),
        m_event(std::move(event)),
        m_white(std::move(white)),
        m_black(std::move(black))
    {
    }

    [[nodiscard]] std::uint64_t GameHeader::gameIdx() const
    {
        return m_gameIdx;
    }

    [[nodiscard]] GameResult GameHeader::result() const
    {
        return m_result;
    }

    [[nodiscard]] Date GameHeader::date() const
    {
        return m_date;
    }

    [[nodiscard]] Eco GameHeader::eco() const
    {
        return m_eco;
    }

    [[nodiscard]] std::optional<std::uint16_t> GameHeader::plyCount() const
    {
        return m_plyCount;
    }

    [[nodiscard]] const std::string& GameHeader::event() const
    {
        return m_event;
    }

    [[nodiscard]] const std::string& GameHeader::white() const
    {
        return m_white;
    }

    [[nodiscard]] const std::string& GameHeader::black() const
    {
        return m_black;
    }

    void to_json(nlohmann::json& j, const GameHeader& data)
    {
        j = nlohmann::json{
            { "game_id", data.m_gameIdx },
            { "result", toString(GameResultPgnFormat{}, data.m_result) },
            { "date", data.m_date.toString() },
            { "eco", data.m_eco.toString() },
            { "event", data.m_event },
            { "white", data.m_white },
            { "black", data.m_black }
        };

        if (data.m_plyCount.has_value())
        {
            j["ply_count"] = *data.m_plyCount;
        }
    }

    void from_json(const nlohmann::json& j, GameHeader& data)
    {
        j["game_id"].get_to(data.m_gameIdx);

        auto resultOpt = fromString<GameResult>(GameResultPgnFormat{}, j["result"]);
        if (resultOpt.has_value())
        {
            // TODO: throw otherwise?
            data.m_result = *resultOpt;
        }

        auto dateOpt = Date::tryParse(j["date"]);
        if (dateOpt.has_value())
        {
            data.m_date = *dateOpt;
        }

        auto ecoOpt = Eco::tryParse(j["eco"]);
        if (ecoOpt.has_value())
        {
            data.m_eco = *ecoOpt;
        }

        if (j.contains("ply_count"))
        {
            auto plyCountOpt = parser_bits::tryParseUInt16(j["ply_count"]);
            if (plyCountOpt.has_value())
            {
                data.m_plyCount = *plyCountOpt;
            }
        }

        j["event"].get_to(data.m_event);
        j["white"].get_to(data.m_white);
        j["black"].get_to(data.m_black);
    }
}
