#pragma once

#include "Assert.h"
#include "Enum.h"
#include "GameClassification.h"
#include "MoveGenerator.h"
#include "Position.h"
#include "StorageHeader.h"

#include "lib/json/json.hpp"

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace query
{
    // TODO: a lot of temporary strings are created because json lib doesn't
    //       support string_view. Find a remedy.

    // Specification of the position which is the basis for a query.
    // It can be either:
    // A FEN, in which case the position has no history
    // and for the query's purpose is interpreted as if the the game started
    // at this exact position.
    // A FEN with a move, in which case the position used as a root
    // is the positions that arises after the move is performed on the
    // position specified by the FEN. In this case root positions
    // is considered to have a history.
    struct RootPosition
    {
        std::string fen;

        // NOTE: If move is specified then the query is made on a 
        // position that arises from fen after the move is made.
        std::optional<std::string> move;

        friend void to_json(nlohmann::json& j, const RootPosition& query)
        {
            j["fen"] = query.fen;
            if (query.move.has_value())
            {
                j["move"] = *query.move;
            }
        }

        friend void from_json(const nlohmann::json& j, RootPosition& query)
        {
            j["fen"].get_to(query.fen);
            if (j["move"].is_null())
            {
                query.move.reset();
            }
            else
            {
                query.move = j["move"].get<std::string>();
            }
        }

        [[nodiscard]] std::optional<Position> tryGet() const
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

        [[nodiscard]] std::optional<std::pair<Position, ReverseMove>> tryGetWithHistory() const
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
    };

    enum struct Select
    {
        Continuations,
        Transpositions,
        All
    };

    template <>
    struct EnumTraits<Select>
    {
        using IdType = int;
        using EnumType = Select;

        static constexpr int cardinality = 3;
        static constexpr bool isNaturalIndex = true;

        static constexpr std::array<EnumType, cardinality> values{
            Select::Continuations,
            Select::Transpositions,
            Select::All
        };

        [[nodiscard]] static constexpr IdType ordinal(EnumType c) noexcept
        {
            return static_cast<IdType>(c);
        }

        [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
        {
            return static_cast<EnumType>(id);
        }

        [[nodiscard]] static std::string_view toString(Select select)
        {
            using namespace std::literals;

            switch (select)
            {
            case Select::Continuations:
                return "continuations"sv;
            case Select::Transpositions:
                return "transpositions"sv;
            case Select::All:
                return "all"sv;
            }

            return ""sv;
        }

        [[nodiscard]] static std::optional<Select> fromString(std::string_view sv)
        {
            using namespace std::literals;

            if (sv == "continuations"sv) return Select::Continuations;
            if (sv == "transpositions"sv) return Select::Transpositions;
            if (sv == "all"sv) return Select::All;

            return {};
        }
    };

    struct AdditionalFetchingOptions
    {
        bool fetchChildren;

        bool fetchFirstGame;
        bool fetchLastGame;

        bool fetchFirstGameForEachChild;
        bool fetchLastGameForEachChild;

        friend void to_json(nlohmann::json& j, const AdditionalFetchingOptions& opt)
        {
            j = nlohmann::json{
                { "fetch_children", opt.fetchChildren},
                { "fetch_first_game", opt.fetchFirstGame },
                { "fetch_last_game", opt.fetchLastGame },
                { "fetch_first_game_for_each_child", opt.fetchFirstGameForEachChild },
                { "fetch_last_game_for_each_child", opt.fetchLastGameForEachChild }
            };
        }

        friend void from_json(const nlohmann::json& j, AdditionalFetchingOptions& opt)
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
    };

    struct Request
    {   
        // token can be used to match queries to results by the client
        std::string token;

        std::vector<RootPosition> positions;

        std::vector<GameLevel> levels;
        std::vector<GameResult> results;
        std::map<Select, AdditionalFetchingOptions> fetchingOptions;

        friend void to_json(nlohmann::json& j, const Request& query)
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

        friend void from_json(const nlohmann::json& j, Request& query)
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

        [[nodiscard]] bool isValid() const
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
    };

    struct Entry
    {
        Entry(std::size_t count) :
            count(count)
        {
        }

        std::size_t count;
        std::optional<persistence::GameHeader> firstGame;
        std::optional<persistence::GameHeader> lastGame;

        friend void to_json(nlohmann::json& j, const Entry& entry)
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

        friend void from_json(const nlohmann::json& j, Entry& entry)
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
    };

    struct SegregatedEntries
    {
    private:
        struct Origin
        {
            GameLevel level;
            GameResult result;
        };

    public:
        SegregatedEntries() = default;

        template <typename... Args>
        decltype(auto) emplace(GameLevel level, GameResult result, Args&&... args)
        {
            m_entries.emplace_back(
                std::piecewise_construct,
                std::forward_as_tuple(Origin{level, result}),
                std::forward_as_tuple(std::forward<Args>(args)...)
            );
        }

        [[nodiscard]] decltype(auto) begin()
        {
            return m_entries.begin();
        }

        [[nodiscard]] decltype(auto) end()
        {
            return m_entries.end();
        }

        [[nodiscard]] decltype(auto) begin() const
        {
            return m_entries.begin();
        }

        [[nodiscard]] decltype(auto) end() const
        {
            return m_entries.end();
        }

        friend void to_json(nlohmann::json& j, const SegregatedEntries& entries)
        {
            j = nlohmann::json::object();

            for (auto&& [origin, entry] : entries)
            {
                const auto levelStr = std::string(toString(origin.level));
                const auto resultStr = std::string(toString(GameResultWordFormat{}, origin.result));
                j[levelStr][resultStr] = entry;
            }
        }

        Entry& at(GameLevel level, GameResult result)
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

        const Entry& at(GameLevel level, GameResult result) const
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

    private:
        std::vector<std::pair<Origin, Entry>> m_entries;
    };

    struct ResultForRoot
    {
        struct MoveCompareLess
        {
            [[nodiscard]] bool operator()(const Move& lhs, const Move& rhs) const noexcept
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
        };

        struct SelectResult
        {
            SegregatedEntries root;
            std::map<Move, SegregatedEntries, MoveCompareLess> children;
        };

        ResultForRoot(const RootPosition& pos) :
            position(pos)
        {
        }

        RootPosition position;
        std::map<Select, SelectResult> resultsBySelect;

        friend void to_json(nlohmann::json& j, const ResultForRoot& result)
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
    };

    struct Response
    {
        Request query;
        std::vector<ResultForRoot> results;

        friend void to_json(nlohmann::json& j, const Response& response)
        {
            j = nlohmann::json{
                { "query", response.query },
                { "results", response.results }
            };
        }
    };

    enum struct PositionQueryOrigin
    {
        Root,
        Child
    };

    template <>
    struct EnumTraits<PositionQueryOrigin>
    {
        using IdType = int;
        using EnumType = PositionQueryOrigin;

        static constexpr int cardinality = 2;
        static constexpr bool isNaturalIndex = true;

        static constexpr std::array<EnumType, cardinality> values{
            PositionQueryOrigin::Root,
            PositionQueryOrigin::Child
        };

        [[nodiscard]] static constexpr IdType ordinal(EnumType c) noexcept
        {
            return static_cast<IdType>(c);
        }

        [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
        {
            return static_cast<EnumType>(id);
        }
    };

    enum struct SelectMask : unsigned
    {
        None = 0,
        OnlyContinuations = 1 << ordinal(Select::Continuations),
        OnlyTranspositions = 1 << ordinal(Select::Transpositions),
        AllSeparate = OnlyContinuations | OnlyTranspositions,
        AllCombined = 1 << ordinal(Select::All)
    };

    [[nodiscard]] constexpr SelectMask operator|(SelectMask lhs, SelectMask rhs)
    {
        return static_cast<SelectMask>(static_cast<unsigned>(lhs) | static_cast<unsigned>(rhs));
    }

    [[nodiscard]] constexpr SelectMask operator|(SelectMask lhs, Select rhs)
    {
        return static_cast<SelectMask>(static_cast<unsigned>(lhs) | (1 << static_cast<unsigned>(rhs)));
    }

    [[nodiscard]] constexpr SelectMask operator&(SelectMask lhs, SelectMask rhs)
    {
        return static_cast<SelectMask>(static_cast<unsigned>(lhs) & static_cast<unsigned>(rhs));
    }

    [[nodiscard]] constexpr SelectMask operator&(SelectMask lhs, Select rhs)
    {
        return static_cast<SelectMask>(static_cast<unsigned>(lhs) & (1 << static_cast<unsigned>(rhs)));
    }

    constexpr SelectMask& operator|=(SelectMask& lhs, SelectMask rhs)
    {
        lhs = static_cast<SelectMask>(static_cast<unsigned>(lhs) | static_cast<unsigned>(rhs));
        return lhs;
    }

    constexpr SelectMask& operator|=(SelectMask& lhs, Select rhs)
    {
        lhs = static_cast<SelectMask>(static_cast<unsigned>(lhs) | (1 << static_cast<unsigned>(rhs)));
        return lhs;
    }

    constexpr SelectMask& operator&=(SelectMask& lhs, SelectMask rhs)
    {
        lhs = static_cast<SelectMask>(static_cast<unsigned>(lhs) & static_cast<unsigned>(rhs));
        return lhs;
    }

    constexpr SelectMask& operator&=(SelectMask& lhs, Select rhs)
    {
        lhs = static_cast<SelectMask>(static_cast<unsigned>(lhs) & (1 << static_cast<unsigned>(rhs)));
        return lhs;
    }

    [[nodiscard]] constexpr SelectMask asMask(Select select)
    {
        return static_cast<SelectMask>(1 << static_cast<unsigned>(select));
    }

    // checks whether lhs contains rhs
    [[nodiscard]] constexpr bool contains(SelectMask lhs, SelectMask rhs)
    {
        return (lhs & rhs) == rhs;
    }

    [[nodiscard]] constexpr bool contains(SelectMask lhs, Select rhs)
    {
        return (lhs & rhs) == asMask(rhs);
    }

    [[nodiscard]] constexpr bool isValid(SelectMask mask)
    {
        return 
            mask == SelectMask::OnlyContinuations
            || mask == SelectMask::OnlyTranspositions
            || mask == SelectMask::AllSeparate
            || mask == SelectMask::AllCombined;
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

    struct PositionQuery
    {
        Position position;
        ReverseMove reverseMove;
        std::size_t rootId;
        PositionQueryOrigin origin;

        PositionQuery(const Position& pos, const ReverseMove& rev, std::size_t rootId, PositionQueryOrigin origin) :
            position(pos),
            reverseMove(rev),
            rootId(rootId),
            origin(origin)
        {
        }
    };

    using PositionQueries = std::vector<PositionQuery>;

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

    // This is the result type to be used by databases' query functions
    // It is flatter, allows easier in memory manipulation.
    using PositionQueryResults = std::vector<EnumMap<Select, SegregatedEntries>>;

    [[nodiscard]] std::vector<ResultForRoot> unflatten(PositionQueryResults&& raw, const Request& query, const PositionQueries& individialQueries)
    {
        std::vector<ResultForRoot> results;
        for (auto&& rootPosition : query.positions)
        {
            results.emplace_back(rootPosition);
        }

        const std::size_t size = raw.size();
        for(std::size_t i = 0; i < size; ++i)
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

    struct GameHeaderDestination
    {
        using HeaderMemberPtr = std::optional<persistence::GameHeader> Entry::*;

        GameHeaderDestination(std::size_t queryId, Select select, GameLevel level, GameResult result, HeaderMemberPtr headerPtr) :
            queryId(queryId),
            select(select),
            level(level),
            result(result),
            headerPtr(headerPtr)
        {
        }

        std::size_t queryId;
        Select select;
        GameLevel level;
        GameResult result;
        HeaderMemberPtr headerPtr;
    };

    void assignGameHeaders(PositionQueryResults& raw, const std::vector<GameHeaderDestination>& destinations, std::vector<persistence::GameHeader>&& headers)
    {
        ASSERT(destination.size() == headers.size());

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
        ASSERT(destination.size() == headers.size());

        const std::size_t size = destinations.size();
        for (std::size_t i = 0; i < size; ++i)
        {
            auto&& [queryId, select, level, result, headerPtr] = destinations[i];

            auto& entry = raw[queryId][select].at(level, result);
            (entry.*headerPtr).emplace(headers[i]);
        }
    }

    struct GameFetchSettings
    {
        bool fetchFirst;
        bool fetchLast;
    };

    using FetchLookups = EnumMap2<PositionQueryOrigin, Select, GameFetchSettings>;

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
