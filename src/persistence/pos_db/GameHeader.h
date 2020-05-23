#pragma once

#include "chess/Bcgn.h"
#include "chess/Date.h"
#include "chess/Eco.h"
#include "chess/GameClassification.h"
#include "chess/Pgn.h"

#include "external_storage/External.h"

#include "util/MemoryAmount.h"

#include <cstdint>
#include <filesystem>
#include <limits>
#include <string_view>

#include "json/json.hpp"

namespace persistence
{
    struct GameHeader
    {
        GameHeader() = default;

        GameHeader(
            std::uint64_t gameIdx,
            GameResult result,
            Date date,
            Eco eco,
            std::uint16_t plyCount,
            std::string event,
            std::string white,
            std::string black
        );

        GameHeader(const GameHeader& other) = default;
        GameHeader(GameHeader&& other) = default;

        GameHeader& operator=(const GameHeader& other) = default;
        GameHeader& operator=(GameHeader&& other) = default;

        template <typename PackedGameHeaderT>
        explicit GameHeader(const PackedGameHeaderT& header) :
            m_gameIdx(header.gameIdx()),
            m_result(header.result()),
            m_date(header.date()),
            m_eco(header.eco()),
            m_plyCount(std::nullopt),
            m_event(header.event()),
            m_white(header.white()),
            m_black(header.black())
        {
            if (header.plyCount() != PackedGameHeaderT::unknownPlyCount)
            {
                m_plyCount = header.plyCount();
            }
        }

        template <typename PackedGameHeaderT>
        GameHeader& operator=(const PackedGameHeaderT& header)
        {
            m_gameIdx = header.gameIdx();
            m_result = header.result();
            m_date = header.date();
            m_eco = header.eco();
            m_plyCount = header.plyCount();
            if (m_plyCount == PackedGameHeaderT::unknownPlyCount)
            {
                m_plyCount.reset();
            }
            m_event = header.event();
            m_white = header.white();
            m_black = header.black();

            return *this;
        }

        [[nodiscard]] std::uint64_t gameIdx() const;

        [[nodiscard]] GameResult result() const;

        [[nodiscard]] Date date() const;

        [[nodiscard]] Eco eco() const;

        [[nodiscard]] std::optional<std::uint16_t> plyCount() const;

        [[nodiscard]] const std::string& event() const;

        [[nodiscard]] const std::string& white() const;

        [[nodiscard]] const std::string& black() const;

        friend void to_json(nlohmann::json& j, const GameHeader& data);

        friend void from_json(const nlohmann::json& j, GameHeader& data);

    private:
        std::uint64_t m_gameIdx;
        GameResult m_result;
        Date m_date;
        Eco m_eco;
        std::optional<std::uint16_t> m_plyCount;
        std::string m_event;
        std::string m_white;
        std::string m_black;
    };
}
