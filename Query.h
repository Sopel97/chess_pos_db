#pragma once

#include "Enum.h"
#include "GameClassification.h"
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

    enum struct Category
    {
        Continuations,
        Transpositions,
        All
    };

    template <>
    struct EnumTraits<Category>
    {
        using IdType = int;
        using EnumType = Category;

        static constexpr int cardinality = 3;
        static constexpr bool isNaturalIndex = true;

        static constexpr std::array<EnumType, cardinality> values{
            Category::Continuations,
            Category::Transpositions,
            Category::All
        };

        [[nodiscard]] static constexpr IdType ordinal(EnumType c) noexcept
        {
            return static_cast<IdType>(c);
        }

        [[nodiscard]] static constexpr EnumType fromOrdinal(IdType id) noexcept
        {
            return static_cast<EnumType>(id);
        }

        [[nodiscard]] static std::string_view toString(Category cat)
        {
            using namespace std::literals;

            switch (cat)
            {
            case Category::Continuations:
                return "continuations"sv;
            case Category::Transpositions:
                return "transpositions"sv;
            case Category::All:
                return "all"sv;
            }

            return ""sv;
        }

        [[nodiscard]] static std::optional<Category> fromString(std::string_view sv)
        {
            using namespace std::literals;

            if (sv == "continuations"sv) return Category::Continuations;
            if (sv == "transpositions"sv) return Category::Transpositions;
            if (sv == "all"sv) return Category::All;

            return {};
        }
    };

    struct FetchingOptions
    {
        bool fetchChildren;

        bool fetchFirstGame;
        bool fetchLastGame;

        bool fetchFirstGameForEachChild;
        bool fetchLastGameForEachChild;

        friend void to_json(nlohmann::json& j, const FetchingOptions& opt)
        {
            j = nlohmann::json{
                { "fetch_children", opt.fetchChildren},
                { "fetch_first_game", opt.fetchFirstGame },
                { "fetch_last_game", opt.fetchLastGame },
                { "fetch_first_game_for_each_child", opt.fetchFirstGameForEachChild },
                { "fetch_last_game_for_each_child", opt.fetchLastGameForEachChild }
            };
        }

        friend void from_json(const nlohmann::json& j, FetchingOptions& opt)
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
        std::map<Category, FetchingOptions> fetchingOptions;

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

            for (auto&& [cat, opt] : query.fetchingOptions)
            {
                j[std::string(toString(cat))] = opt;
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
                query.levels.emplace_back(fromString<GameLevel>(levelStr));
            }

            for (auto&& resultStr : j["results"])
            {
                query.results.emplace_back(fromString<GameResult>(GameResultWordFormat{}, resultStr));
            }

            for (const Category cat : values<Category>())
            {
                const auto catStr = std::string(toString(cat));
                if (j.contains(catStr))
                {
                    query.fetchingOptions.emplace(cat, j[catStr].get<FetchingOptions>());
                }
            }
        }

        [[nodiscard]] bool isValid() const
        {
            if (fetchingOptions.empty()) return false;
            if (fetchingOptions.size() > 2) return false;
            if (fetchingOptions.size() == 2 && fetchingOptions.count(Category::All) != 0) return false;
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

    struct Entries
    {
    private:
        struct Origin
        {
            GameLevel level;
            GameResult result;
        };

    public:
        Entries() = default;

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

        friend void to_json(nlohmann::json& j, const Entries& entries)
        {
            j = nlohmann::json::object();

            for (auto&& [origin, entry] : entries)
            {
                const auto levelStr = std::string(toString(origin.level));
                const auto resultStr = std::string(toString(GameResultWordFormat{}, origin.result));
                j[levelStr][resultStr] = entry;
            }
        }

    private:
        std::vector<std::pair<Origin, Entry>> m_entries;
    };

    struct Result
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

        struct SubResult
        {
            Entries root;
            std::map<Move, Entries, MoveCompareLess> children;
        };

        Result(const RootPosition& pos) :
            position(pos)
        {
        }

        RootPosition position;
        std::map<Category, SubResult> resultsByCategory;

        friend void to_json(nlohmann::json& j, const Result& result)
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

            for (auto&& [cat, subresult] : result.resultsByCategory)
            {
                auto& jsonSubresult = j[std::string(toString(cat))];

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
        std::vector<Result> results;

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

    enum struct CategoryMask : unsigned
    {
        None = 0,
        OnlyContinuations = 1 << ordinal(Category::Continuations),
        OnlyTranspositions = 1 << ordinal(Category::Transpositions),
        AllSeparate = OnlyContinuations | OnlyTranspositions,
        AllCombined = 1 << ordinal(Category::All)
    };

    [[nodiscard]] constexpr CategoryMask operator|(CategoryMask lhs, CategoryMask rhs)
    {
        return static_cast<CategoryMask>(static_cast<unsigned>(lhs) | static_cast<unsigned>(rhs));
    }

    [[nodiscard]] constexpr CategoryMask operator|(CategoryMask lhs, Category rhs)
    {
        return static_cast<CategoryMask>(static_cast<unsigned>(lhs) | (1 << static_cast<unsigned>(rhs)));
    }

    [[nodiscard]] constexpr CategoryMask operator&(CategoryMask lhs, CategoryMask rhs)
    {
        return static_cast<CategoryMask>(static_cast<unsigned>(lhs) & static_cast<unsigned>(rhs));
    }

    [[nodiscard]] constexpr CategoryMask operator&(CategoryMask lhs, Category rhs)
    {
        return static_cast<CategoryMask>(static_cast<unsigned>(lhs) & (1 << static_cast<unsigned>(rhs)));
    }

    constexpr CategoryMask& operator|=(CategoryMask& lhs, CategoryMask rhs)
    {
        lhs = static_cast<CategoryMask>(static_cast<unsigned>(lhs) | static_cast<unsigned>(rhs));
        return lhs;
    }

    constexpr CategoryMask& operator|=(CategoryMask& lhs, Category rhs)
    {
        lhs = static_cast<CategoryMask>(static_cast<unsigned>(lhs) | (1 << static_cast<unsigned>(rhs)));
        return lhs;
    }

    constexpr CategoryMask& operator&=(CategoryMask& lhs, CategoryMask rhs)
    {
        lhs = static_cast<CategoryMask>(static_cast<unsigned>(lhs) & static_cast<unsigned>(rhs));
        return lhs;
    }

    constexpr CategoryMask& operator&=(CategoryMask& lhs, Category rhs)
    {
        lhs = static_cast<CategoryMask>(static_cast<unsigned>(lhs) & (1 << static_cast<unsigned>(rhs)));
        return lhs;
    }

    [[nodiscard]] constexpr CategoryMask asMask(Category cat)
    {
        return static_cast<CategoryMask>(1 << static_cast<unsigned>(cat));
    }

    // checks whether lhs contains rhs
    [[nodiscard]] constexpr bool contains(CategoryMask lhs, CategoryMask rhs)
    {
        return (lhs & rhs) == rhs;
    }

    [[nodiscard]] constexpr bool contains(CategoryMask lhs, Category rhs)
    {
        return (lhs & rhs) == asMask(rhs);
    }

    [[nodiscard]] constexpr bool isValid(CategoryMask mask)
    {
        return 
            mask == CategoryMask::OnlyContinuations
            || mask == CategoryMask::OnlyTranspositions
            || mask == CategoryMask::AllSeparate
            || mask == CategoryMask::AllCombined;
    }

    struct PositionQuerySet
    {
        struct EntryConstRef
        {
            const Position& position;
            const ReverseMove& reverseMove;
            const std::size_t rootId;
            const PositionQueryOrigin origin;
        };

        PositionQuerySet(
            std::vector<Position>&& positions,
            std::vector<ReverseMove>&& reverseMoves,
            std::vector<std::size_t>&& rootIds,
            std::vector<PositionQueryOrigin>&& origins
        ) :
            m_positions(std::move(positions)),
            m_reverseMoves(std::move(reverseMoves)),
            m_rootIds(std::move(rootIds)),
            m_origins(std::move(origins))
        {
        }

        const auto& positions() const
        {
            return m_positions;
        }

        const auto& reverseMoves() const
        {
            return m_reverseMoves;
        }

        const auto& rootIds() const
        {
            return m_rootIds;
        }

        const auto& origins() const
        {
            return m_origins;
        }

        const EntryConstRef operator[](std::size_t i) const
        {
            return {
                m_positions[i],
                m_reverseMoves[i],
                m_rootIds[i],
                m_origins[i]
            };
        }

    private:
        std::vector<Position> m_positions;
        std::vector<ReverseMove> m_reverseMoves;
        std::vector<std::size_t> m_rootIds;
        std::vector<PositionQueryOrigin> m_origins;
    };

    [[nodiscard]] PositionQuerySet gatherPositionsForRootPositions(const std::vector<RootPosition>& rootPositions, bool fetchChildren)
    {
        // NOTE: we don't remove duplicates because there should be no
        //       duplicates when we consider reverse move

        std::vector<Position> positions;
        std::vector<ReverseMove> reverseMoves;
        std::vector<std::size_t> fenIds;
        std::vector<PositionQueryOrigin> origins;
        for (std::size_t i = 0; i < rootPositions.size(); ++i)
        {
            const auto& rootPos = rootPositions[i];

            const auto posOpt = rootPos.tryGetWithHistory();
            if (!posOpt.has_value()) throw std::runtime_error("Invalid position in query");

            const auto& pos = posOpt->first;
            const auto& rev = posOpt->second;

            positions.emplace_back(pos);
            reverseMoves.emplace_back(rev);
            fenIds.emplace_back(i);
            origins.emplace_back(PositionQueryOrigin::Root);

            if (fetchChildren)
            {
                movegen::forEachLegalMove(pos, [&](Move move) {
                    auto posCpy = pos;
                    reverseMoves.emplace_back(posCpy.doMove(move));
                    positions.emplace_back(posCpy);
                    fenIds.emplace_back(i);
                    origins.emplace_back(PositionQueryOrigin::Child);
                });
            }
        }

        return { std::move(positions), std::move(reverseMoves), std::move(fenIds), std::move(origins) };
    }

    [[nodiscard]] PositionQuerySet gatherPositionsForRootPositions(const Request& query)
    {
        const bool fetchChildren = std::any_of(
            query.fetchingOptions.begin(), 
            query.fetchingOptions.end(), 
            [](auto&& v) {return v.second.fetchChildren; }
        );
        return gatherPositionsForRootPositions(query.positions, fetchChildren);
    }

    // This is the result type to be used by databases' query functions
    // It is flatter, allows easier in memory manipulation.
    using PositionQueryResultSet = std::vector<EnumMap<Category, Entries>>;

    [[nodiscard]] std::vector<Result> unflatten(PositionQueryResultSet&& raw, const Request& query, const PositionQuerySet& individialQueries)
    {
        std::vector<Result> results;
        for (auto&& rootPosition : query.positions)
        {
            results.emplace_back(rootPosition);
        }

        const std::size_t size = raw.size();
        for(std::size_t i = 0; i < size; ++i)
        {
            auto&& entriesByCategory = raw[i];
            auto [position, reverseMove, rootId, origin] = individialQueries[i];

            for (auto&& [cat, fetch] : query.fetchingOptions)
            {
                if (origin == PositionQueryOrigin::Child && !fetch.fetchChildren)
                {
                    continue;
                }

                auto&& entries = entriesByCategory[cat];

                auto& entriesDestination =
                    origin == PositionQueryOrigin::Child
                    ? results[rootId].resultsByCategory[cat].children[reverseMove.move]
                    : results[rootId].resultsByCategory[cat].root;

                entriesDestination = std::move(entries);
            }
        }

        return results;
    }
}
