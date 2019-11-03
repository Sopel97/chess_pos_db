#include "Query.h"

#include "StorageHeader.h"

#include "chess/GameClassification.h"
#include "chess/MoveGenerator.h"
#include "chess/Position.h"
#include "chess/San.h"

#include "enum/Enum.h"

#include "util/Assert.h"

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "json/json.hpp"

namespace query
{
    void to_json(nlohmann::json& j, const RootPosition& query)
    {
        j["fen"] = query.fen;
        if (query.move.has_value())
        {
            j["move"] = *query.move;
        }
    }

    void from_json(const nlohmann::json& j, RootPosition& query)
    {
        j["fen"].get_to(query.fen);
        if (j.contains("move"))
        {
            query.move = j["move"].get<std::string>();
        }
        else
        {
            query.move.reset();
        }
    }

    [[nodiscard]] std::optional<Position> RootPosition::tryGet() const
    {
        std::optional<Position> positionOpt = Position::tryFromFen(fen);

        if (positionOpt.has_value() && move.has_value())
        {
            const std::optional<Move> moveOpt = san::trySanToMove(*positionOpt, *move);
            if (moveOpt.has_value() && *moveOpt != Move::null())
            {
                positionOpt->doMove(*moveOpt);
            }
            else
            {
                return {};
            }
        }

        return positionOpt;
    }

    [[nodiscard]] std::optional<std::pair<Position, ReverseMove>> RootPosition::tryGetWithHistory() const
    {
        std::optional<Position> positionOpt = Position::tryFromFen(fen);
        ReverseMove reverseMove{};

        if (!positionOpt.has_value())
        {
            return {};
        }

        if (move.has_value())
        {
            const std::optional<Move> moveOpt = san::trySanToMove(*positionOpt, *move);
            if (moveOpt.has_value() && *moveOpt != Move::null())
            {
                reverseMove = positionOpt->doMove(*moveOpt);
            }
            else
            {
                return {};
            }
        }

        return std::make_pair(*positionOpt, reverseMove);
    }

    void to_json(nlohmann::json& j, const AdditionalFetchingOptions& opt)
    {
        j = nlohmann::json{
            { "fetch_children", opt.fetchChildren},
            { "fetch_first_game", opt.fetchFirstGame },
            { "fetch_last_game", opt.fetchLastGame },
            { "fetch_first_game_for_each_child", opt.fetchFirstGameForEachChild },
            { "fetch_last_game_for_each_child", opt.fetchLastGameForEachChild }
        };
    }

    void from_json(const nlohmann::json& j, AdditionalFetchingOptions& opt)
    {
        j["fetch_children"].get_to(opt.fetchChildren);
        j["fetch_first_game"].get_to(opt.fetchFirstGame);
        j["fetch_last_game"].get_to(opt.fetchLastGame);

        if (opt.fetchChildren)
        {
            j["fetch_first_game_for_each_child"].get_to(opt.fetchFirstGameForEachChild);
            j["fetch_last_game_for_each_child"].get_to(opt.fetchLastGameForEachChild);
        }
        else
        {
            opt.fetchFirstGameForEachChild = false;
            opt.fetchLastGameForEachChild = false;
        }
    }

    void to_json(nlohmann::json& j, const Request& query)
    {
        j = nlohmann::json{
            { "token", query.token },
            { "positions", query.positions }
        };

        auto& levels = j["levels"] = nlohmann::json::array();
        auto& results = j["results"] = nlohmann::json::array();

        for (auto&& level : query.levels)
        {
            levels.emplace_back(toString(level));
        }

        for (auto&& result : query.results)
        {
            results.emplace_back(toString(GameResultWordFormat{}, result));
        }

        for (auto&& [select, opt] : query.fetchingOptions)
        {
            j[std::string(toString(select))] = opt;
        }
    }

    void from_json(const nlohmann::json& j, Request& query)
    {
        query.positions.clear();
        query.levels.clear();
        query.results.clear();
        query.fetchingOptions.clear();

        j["token"].get_to(query.token);
        j["positions"].get_to(query.positions);

        for (auto&& levelStr : j["levels"])
        {
            auto levelOpt = fromString<GameLevel>(levelStr);
            if (levelOpt.has_value())
            {
                // TODO: throw otherwise?
                query.levels.emplace_back(*levelOpt);
            }
        }

        for (auto&& resultStr : j["results"])
        {
            auto resultOpt = fromString<GameResult>(GameResultWordFormat{}, resultStr);
            if (resultOpt.has_value())
            {
                query.results.emplace_back(*resultOpt);
            }
        }

        for (const Select select : values<Select>())
        {
            const auto selectStr = std::string(toString(select));
            if (j.contains(selectStr))
            {
                query.fetchingOptions.emplace(select, j[selectStr].get<AdditionalFetchingOptions>());
            }
        }
    }

    [[nodiscard]] bool Request::isValid() const
    {
        if (fetchingOptions.empty()) return false;
        if (fetchingOptions.size() > 2) return false;
        if (fetchingOptions.size() == 2 && fetchingOptions.count(Select::All) != 0) return false;
        if (levels.empty()) return false;
        if (results.empty()) return false;

        for (auto&& root : positions)
        {
            if (!root.tryGet().has_value())
            {
                return false;
            }
        }

        return true;
    }

    Entry::Entry(std::size_t count) :
        count(count)
    {
    }

    void to_json(nlohmann::json& j, const Entry& entry)
    {
        j = nlohmann::json::object({
            { "count", entry.count }
            });

        if (entry.firstGame.has_value())
        {
            j["first_game"] = *entry.firstGame;
        }
        if (entry.lastGame.has_value())
        {
            j["last_game"] = *entry.lastGame;
        }
    }

    void from_json(const nlohmann::json& j, Entry& entry)
    {
        j["count"].get_to(entry.count);

        if (j.contains("first_game"))
        {
            entry.firstGame = j["first_game"].get<persistence::GameHeader>();
        }

        if (j.contains("last_game"))
        {
            entry.lastGame = j["last_game"].get<persistence::GameHeader>();
        }
    }

    void to_json(nlohmann::json& j, const SegregatedEntries& entries)
    {
        j = nlohmann::json::object();

        for (auto&& [origin, entry] : entries)
        {
            const auto levelStr = std::string(toString(origin.level));
            const auto resultStr = std::string(toString(GameResultWordFormat{}, origin.result));
            j[levelStr][resultStr] = entry;
        }
    }

    Entry& SegregatedEntries::at(GameLevel level, GameResult result)
    {
        for (auto&& [origin, entry] : m_entries)
        {
            if (origin.level == level && origin.result == result)
            {
                return entry;
            }
        }

        throw std::out_of_range("");
    }

    const Entry& SegregatedEntries::at(GameLevel level, GameResult result) const
    {
        for (auto&& [origin, entry] : m_entries)
        {
            if (origin.level == level && origin.result == result)
            {
                return entry;
            }
        }

        throw std::out_of_range("");
    }

    [[nodiscard]] bool ResultForRoot::MoveCompareLess::operator()(const Move& lhs, const Move& rhs) const noexcept
    {
        if (ordinal(lhs.from) < ordinal(rhs.from)) return true;
        if (ordinal(lhs.from) > ordinal(rhs.from)) return false;

        if (ordinal(lhs.to) < ordinal(rhs.to)) return true;
        if (ordinal(lhs.to) > ordinal(rhs.to)) return false;

        if (ordinal(lhs.type) < ordinal(rhs.type)) return true;
        if (ordinal(lhs.type) > ordinal(rhs.type)) return false;

        if (ordinal(lhs.promotedPiece) < ordinal(rhs.promotedPiece)) return true;

        return false;
    }

    ResultForRoot::ResultForRoot(const RootPosition& pos) :
        position(pos)
    {
    }

    void to_json(nlohmann::json& j, const ResultForRoot& result)
    {
        const std::optional<Position> positionOpt = result.position.tryGet();
        if (!positionOpt.has_value())
        {
            return;
        }

        const auto& position = *positionOpt;

        // We have a valid object, fill it.
        j = nlohmann::json::object({
            { "position", result.position },
            });

        for (auto&& [select, subresult] : result.resultsBySelect)
        {
            auto& jsonSubresult = j[std::string(toString(select))];

            jsonSubresult["--"] = subresult.root;

            if (!subresult.children.empty())
            {
                for (auto&& [move, entries] : subresult.children)
                {
                    // Move as a key
                    const auto sanStr = san::moveToSan<san::SanSpec::Capture | san::SanSpec::Check | san::SanSpec::Compact>(position, move);
                    jsonSubresult[sanStr] = entries;
                }
            }
        }
    }

    void to_json(nlohmann::json& j, const Response& response)
    {
        j = nlohmann::json{
            { "query", response.query },
            { "results", response.results }
        };
    }

    [[nodiscard]] SelectMask selectMask(const Request& query)
    {
        SelectMask mask = SelectMask::None;
        for (auto&& [select, fetch] : query.fetchingOptions)
        {
            mask |= select;
        }
        return mask;
    }

    [[nodiscard]] SelectMask fetchChildrenSelectMask(const Request& query)
    {
        SelectMask mask = SelectMask::None;
        for (auto&& [select, fetch] : query.fetchingOptions)
        {
            if (!fetch.fetchChildren) continue;

            mask |= select;
        }
        return mask;
    }

    PositionQuery::PositionQuery(const Position& pos, const ReverseMove& rev, std::size_t rootId, PositionQueryOrigin origin) :
        position(pos),
        reverseMove(rev),
        rootId(rootId),
        origin(origin)
    {
    }

    [[nodiscard]] PositionQueries gatherPositionQueries(const std::vector<RootPosition>& rootPositions, bool fetchChildren)
    {
        PositionQueries queries;
        for (std::size_t i = 0; i < rootPositions.size(); ++i)
        {
            const auto& rootPos = rootPositions[i];

            const auto posOpt = rootPos.tryGetWithHistory();
            if (!posOpt.has_value()) throw std::runtime_error("Invalid position in query");

            const auto& pos = posOpt->first;
            const auto& rev = posOpt->second;

            queries.emplace_back(pos, rev, i, PositionQueryOrigin::Root);

            if (fetchChildren)
            {
                movegen::forEachLegalMove(pos, [&](Move move) {
                    auto posCpy = pos;
                    auto rev = posCpy.doMove(move);
                    queries.emplace_back(posCpy, rev, i, PositionQueryOrigin::Child);
                    });
            }
        }

        return queries;
    }

    [[nodiscard]] PositionQueries gatherPositionQueries(const Request& query)
    {
        const bool fetchChildren = std::any_of(
            query.fetchingOptions.begin(),
            query.fetchingOptions.end(),
            [](auto&& v) {return v.second.fetchChildren; }
        );
        return gatherPositionQueries(query.positions, fetchChildren);
    }

    [[nodiscard]] std::vector<ResultForRoot> unflatten(PositionQueryResults&& raw, const Request& query, const PositionQueries& individialQueries)
    {
        std::vector<ResultForRoot> results;
        for (auto&& rootPosition : query.positions)
        {
            results.emplace_back(rootPosition);
        }

        const std::size_t size = raw.size();
        for (std::size_t i = 0; i < size; ++i)
        {
            auto&& entriesBySelect = raw[i];
            auto&& [position, reverseMove, rootId, origin] = individialQueries[i];

            for (auto&& [select, fetch] : query.fetchingOptions)
            {
                if (origin == PositionQueryOrigin::Child && !fetch.fetchChildren)
                {
                    // We have to check for this because we may only want children
                    // for one select. In this case we would just reassign empty Entries.
                    // We don't want that because we would create nodes in the map.
                    continue;
                }

                auto&& entries = entriesBySelect[select];

                auto& entriesDestination =
                    origin == PositionQueryOrigin::Child
                    ? results[rootId].resultsBySelect[select].children[reverseMove.move]
                    : results[rootId].resultsBySelect[select].root;

                entriesDestination = std::move(entries);
            }
        }

        return results;
    }

    GameHeaderDestination::GameHeaderDestination(std::size_t queryId, Select select, GameLevel level, GameResult result, GameHeaderDestination::HeaderMemberPtr headerPtr) :
        queryId(queryId),
        select(select),
        level(level),
        result(result),
        headerPtr(headerPtr)
    {
    }

    void assignGameHeaders(PositionQueryResults& raw, const std::vector<GameHeaderDestination>& destinations, std::vector<persistence::GameHeader>&& headers)
    {
        ASSERT(destinations.size() == headers.size());

        const std::size_t size = destinations.size();
        for (std::size_t i = 0; i < size; ++i)
        {
            auto&& [queryId, select, level, result, headerPtr] = destinations[i];

            auto& entry = raw[queryId][select].at(level, result);
            (entry.*headerPtr).emplace(std::move(headers[i]));
        }
    }

    void assignGameHeaders(PositionQueryResults& raw, const std::vector<GameHeaderDestination>& destinations, const std::vector<persistence::PackedGameHeader>& headers)
    {
        ASSERT(destinations.size() == headers.size());

        const std::size_t size = destinations.size();
        for (std::size_t i = 0; i < size; ++i)
        {
            auto&& [queryId, select, level, result, headerPtr] = destinations[i];

            auto& entry = raw[queryId][select].at(level, result);
            (entry.*headerPtr).emplace(headers[i]);
        }
    }

    [[nodiscard]] FetchLookups buildGameHeaderFetchLookup(const Request& query)
    {
        FetchLookups lookup{};

        for (auto&& [select, fetch] : query.fetchingOptions)
        {
            lookup[PositionQueryOrigin::Root][select] = { fetch.fetchFirstGame, fetch.fetchLastGame };
            lookup[PositionQueryOrigin::Child][select] = { fetch.fetchFirstGameForEachChild, fetch.fetchLastGameForEachChild };
        }

        return lookup;
    }
}
