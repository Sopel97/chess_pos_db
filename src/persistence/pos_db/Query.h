#pragma once

#include "GameHeader.h"

#include "chess/GameClassification.h"
#include "chess/Position.h"

#include "enum/Enum.h"

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "json/json.hpp"

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

        friend void to_json(nlohmann::json& j, const RootPosition& query);

        friend void from_json(const nlohmann::json& j, RootPosition& query);

        [[nodiscard]] std::optional<Position> tryGet() const;

        [[nodiscard]] std::optional<std::pair<Position, ReverseMove>> tryGetWithHistory() const;
    };

    enum struct Select
    {
        Continuations,
        Transpositions,
        All
    };
}

template <>
struct EnumTraits<query::Select>
{
    using IdType = int;
    using EnumType = query::Select;

    static constexpr int cardinality = 3;
    static constexpr bool isNaturalIndex = true;

    static constexpr std::array<EnumType, cardinality> values{
        query::Select::Continuations,
        query::Select::Transpositions,
        query::Select::All
    };

    [[nodiscard]] static constexpr IdType ordinal(EnumType c) noexcept
    {
        return static_cast<IdType>(c);
    }

    [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
    {
        return static_cast<EnumType>(id);
    }

    [[nodiscard]] static constexpr std::string_view toString(query::Select select)
    {
        using namespace std::literals;

        switch (select)
        {
        case query::Select::Continuations:
            return "continuations"sv;
        case query::Select::Transpositions:
            return "transpositions"sv;
        case query::Select::All:
            return "all"sv;
        }

        return ""sv;
    }

    [[nodiscard]] static constexpr std::optional<query::Select> fromString(std::string_view sv)
    {
        using namespace std::literals;

        if (sv == "continuations"sv) return query::Select::Continuations;
        if (sv == "transpositions"sv) return query::Select::Transpositions;
        if (sv == "all"sv) return query::Select::All;

        return {};
    }
};

namespace query
{
    struct AdditionalFetchingOptions
    {
        bool fetchChildren;

        bool fetchFirstGame;
        bool fetchLastGame;

        bool fetchFirstGameForEachChild;
        bool fetchLastGameForEachChild;

        friend void to_json(nlohmann::json& j, const AdditionalFetchingOptions& opt);

        friend void from_json(const nlohmann::json& j, AdditionalFetchingOptions& opt);
    };

    struct AdditionalRetractionsFetchingOptions
    {
        bool fetchFirstGameForEach;
        bool fetchLastGameForEach;

        friend void to_json(nlohmann::json& j, const AdditionalRetractionsFetchingOptions& opt);

        friend void from_json(const nlohmann::json& j, AdditionalRetractionsFetchingOptions& opt);
    };

    struct Request
    {
        // token can be used to match queries to results by the client
        std::string token;

        std::vector<RootPosition> positions;

        std::vector<GameLevel> levels;
        std::vector<GameResult> results;
        std::map<Select, AdditionalFetchingOptions> fetchingOptions;
        std::optional<AdditionalRetractionsFetchingOptions> retractionsFetchingOptions;

        friend void to_json(nlohmann::json& j, const Request& query);

        friend void from_json(const nlohmann::json& j, Request& query);

        [[nodiscard]] bool isValid() const;
    };

    struct Entry
    {
        std::size_t count;
        std::optional<persistence::GameHeader> firstGame;
        std::optional<persistence::GameHeader> lastGame;
        std::optional<std::int64_t> eloDiff;

        Entry(std::size_t count);

        friend void to_json(nlohmann::json& j, const Entry& entry);

        friend void from_json(const nlohmann::json& j, Entry& entry);
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
        decltype(auto) emplace(GameLevel level, GameResult result, Args&& ... args)
        {
            return m_entries.emplace_back(
                std::piecewise_construct,
                std::forward_as_tuple(Origin{ level, result }),
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

        friend void to_json(nlohmann::json& j, const SegregatedEntries& entries);

        Entry& at(GameLevel level, GameResult result);

        const Entry& at(GameLevel level, GameResult result) const;

    private:
        std::vector<std::pair<Origin, Entry>> m_entries;
    };

    struct ResultForRoot
    {
        struct SelectResult
        {
            SegregatedEntries root;
            std::map<Move, SegregatedEntries, MoveCompareLess> children;
        };

        struct RetractionsResult
        {
            std::map<ReverseMove, SegregatedEntries, ReverseMoveCompareLess> retractions;
        };

        RootPosition position;
        std::map<Select, SelectResult> resultsBySelect;
        RetractionsResult retractionsResults;

        ResultForRoot(const RootPosition& pos);

        friend void to_json(nlohmann::json& j, const ResultForRoot& result);
    };

    struct Response
    {
        Request query;
        std::vector<ResultForRoot> results;

        friend void to_json(nlohmann::json& j, const Response& response);
    };

    enum struct PositionQueryOrigin
    {
        Root,
        Child
    };
}

template <>
struct EnumTraits<query::PositionQueryOrigin>
{
    using IdType = int;
    using EnumType = query::PositionQueryOrigin;

    static constexpr int cardinality = 2;
    static constexpr bool isNaturalIndex = true;

    static constexpr std::array<EnumType, cardinality> values{
        query::PositionQueryOrigin::Root,
        query::PositionQueryOrigin::Child
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

namespace query
{
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

    [[nodiscard]] SelectMask selectMask(const Request& query);

    [[nodiscard]] SelectMask fetchChildrenSelectMask(const Request& query);

    struct PositionQuery
    {
        Position position;
        ReverseMove reverseMove;
        std::size_t rootId;
        PositionQueryOrigin origin;

        PositionQuery(const Position& pos, const ReverseMove& rev, std::size_t rootId, PositionQueryOrigin origin);
    };

    using PositionQueries = std::vector<PositionQuery>;

    [[nodiscard]] PositionQueries gatherPositionQueries(const std::vector<RootPosition>& rootPositions, bool fetchChildren);

    [[nodiscard]] PositionQueries gatherPositionQueries(const Request& query);

    // This is the result type to be used by databases' query functions
    // It is flatter, allows easier in memory manipulation.
    using PositionQueryResults = std::vector<EnumArray<Select, SegregatedEntries>>;
    using RetractionsQueryResults = std::map<ReverseMove, query::SegregatedEntries, ReverseMoveCompareLess>;

    [[nodiscard]] std::vector<ResultForRoot> unflatten(PositionQueryResults&& raw, const Request& query, const PositionQueries& individialQueries);

    struct GameHeaderDestination
    {
        using HeaderMemberPtr = std::optional<persistence::GameHeader> Entry::*;

        std::size_t queryId;
        Select select;
        GameLevel level;
        GameResult result;
        HeaderMemberPtr headerPtr;

        GameHeaderDestination(std::size_t queryId, Select select, GameLevel level, GameResult result, HeaderMemberPtr headerPtr);
    };

    struct GameHeaderDestinationForRetraction
    {
        using HeaderMemberPtr = std::optional<persistence::GameHeader> Entry::*;

        ReverseMove rmove;
        GameLevel level;
        GameResult result;
        HeaderMemberPtr headerPtr;

        GameHeaderDestinationForRetraction(const ReverseMove& rmove, GameLevel level, GameResult result, HeaderMemberPtr headerPtr);
    };

    template <typename GameHeaderT>
    void assignGameHeaders(
        PositionQueryResults& raw, 
        const std::vector<GameHeaderDestination>& destinations, 
        std::vector<GameHeaderT>&& headers
    )
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

    template <typename GameHeaderT>
    void assignGameHeaders(
        RetractionsQueryResults& raw, 
        const std::vector<GameHeaderDestinationForRetraction>& destinations, 
        std::vector<GameHeaderT>&& headers
    )
    {
        ASSERT(destinations.size() == headers.size());

        const std::size_t size = destinations.size();
        for (std::size_t i = 0; i < size; ++i)
        {
            auto&& [rmove, level, result, headerPtr] = destinations[i];

            auto& entry = raw[rmove].at(level, result);
            (entry.*headerPtr).emplace(std::move(headers[i]));
        }
    }

    struct GameFetchSettings
    {
        bool fetchFirst;
        bool fetchLast;
    };

    using FetchLookups = EnumArray2<PositionQueryOrigin, Select, GameFetchSettings>;

    [[nodiscard]] FetchLookups buildGameHeaderFetchLookup(const Request& query);
}
