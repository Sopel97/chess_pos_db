#define _CRT_SECURE_NO_WARNINGS

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iomanip>
#include <map>
#include <memory>

#include "Bitboard.h"
#include "Configuration.h"
#include "Enum.h"
#include "EnumMap.h"
#include "External.h"
#include "LocalStorageFormat.h"
#include "HddStorageFormat.h"
#include "MoveGenerator.h"
#include "Pgn.h"
#include "Position.h"
#include "PositionSignature.h"
#include "San.h"

#include "lib/json/json.hpp"

namespace app
{
    const static std::size_t importMemory = cfg::g_config["app"]["pgn_import_memory"].get<MemoryAmount>();

    void print(Bitboard bb)
    {
        std::cout << std::hex << std::setfill('0') << std::setw(16) << bb.bits() << '\n';
        for (Rank r = rank8; r >= rank1; --r)
        {
            for (File f = fileA; f <= fileH; ++f)
            {
                std::cout << (bb.isSet(Square(f, r)) ? 'X' : '.');
            }
            std::cout << '\n';
        }
        std::cout << "\n\n";
    }

    struct AggregatedQueryResult
    {
        // total, direct
        EnumMap2<GameLevel, GameResult, std::pair<std::size_t, std::size_t>> counts;
        EnumMap2<GameLevel, GameResult, std::optional<persistence::GameHeader>> games;
    };

    struct AggregatedQueryResults
    {
        Position mainPosition;
        AggregatedQueryResult main;
        std::vector<std::pair<Move, AggregatedQueryResult>> continuations;
    };

    std::optional<GameLevel> gameLevelFromString(const std::string& str)
    {
        if (str == "human"sv) return GameLevel::Human;
        if (str == "engine"sv) return GameLevel::Engine;
        if (str == "server"sv) return GameLevel::Server;
        return {};
    }

    persistence::local::PgnFiles parsePgnListFile(const std::filesystem::path& path)
    {
        persistence::local::PgnFiles pgns;

        std::ifstream file(path);
        std::string line;
        while (std::getline(file, line))
        {
            std::istringstream ss(line);
            std::string levelStr, pgnPath;
            std::getline(ss, levelStr, ';');
            if (levelStr.empty()) continue;
            const auto levelOpt = gameLevelFromString(levelStr);
            if (!levelOpt.has_value())
            {
                std::cerr << "Invalid level: " << levelStr << '\n';
                continue;
            }

            std::getline(ss, pgnPath, ';');
            pgns.emplace_back(pgnPath, *levelOpt);
        }

        return pgns;
    }

    std::string resultsToString(const EnumMap<GameResult, std::pair<std::size_t, std::size_t>>& results)
    {
        auto str = std::string("+") + std::to_string(results[GameResult::WhiteWin].first);
        str += std::string("=") + std::to_string(results[GameResult::Draw].first);
        str += std::string("-") + std::to_string(results[GameResult::BlackWin].first);
        str += '/';
        str += std::string("+") + std::to_string(results[GameResult::WhiteWin].second);
        str += std::string("=") + std::to_string(results[GameResult::Draw].second);
        str += std::string("-") + std::to_string(results[GameResult::BlackWin].second);
        return str;
    }

    AggregatedQueryResults queryAggregate(
        persistence::local::Database& db, // querying requires reads that need to flush streams, so this cannot be const
        const Position& pos, 
        bool queryContinuations, 
        bool fetchFirstGame, 
        bool fetchFirstGameForContinuations, 
        bool removeEmptyContinuations
    )
    {
        Position basePosition = pos;
        std::vector<Position> positions;
        std::vector<ReverseMove> reverseMoves;
        std::vector<Move> moves;
        if (queryContinuations)
        {
            movegen::forEachLegalMove(basePosition, [&](Move move) {
                auto& pos = positions.emplace_back(basePosition);
                reverseMoves.emplace_back(pos.doMove(move));
                moves.emplace_back(move);
                });
        }
        reverseMoves.emplace_back();
        positions.emplace_back(basePosition);

        AggregatedQueryResults aggResults;
        aggResults.mainPosition = pos;
        std::vector<std::uint32_t> gameQueries;
        auto results = db.queryRanges(positions, reverseMoves);
        for (int i = 0; i < moves.size(); ++i)
        {
            AggregatedQueryResult aggResult;
            std::size_t totalCount = 0;
            for (GameLevel level : values<GameLevel>())
            {
                for (GameResult result : values<GameResult>())
                {
                    const std::size_t count = results[level][result][i].count();
                    const std::size_t directCount = results[level][result][i].directCount();
                    aggResult.counts[level][result] = { count, directCount };
                    totalCount += count;

                    if (fetchFirstGameForContinuations && directCount > 0)
                    {
                        gameQueries.emplace_back(results[level][result][i].firstDirectGameIndex());
                    }
                }
            }

            if (removeEmptyContinuations && totalCount == 0)
            {
                continue;
            }

            aggResults.continuations.emplace_back(moves[i], aggResult);
        }

        {
            AggregatedQueryResult aggResult;
            for (GameLevel level : values<GameLevel>())
            {
                for (GameResult result : values<GameResult>())
                {
                    const std::size_t count = results[level][result].back().count();
                    const std::size_t directCount = results[level][result].back().directCount();
                    aggResult.counts[level][result] = { count, directCount };

                    // Main position doesn't have a history
                    if (fetchFirstGame && count > 0)
                    {
                        gameQueries.emplace_back(results[level][result].back().firstGameIndex());
                    }
                }
            }
            aggResults.main = aggResult;
        }

        {
            std::vector<persistence::PackedGameHeader> headers = db.queryHeadersByIndices(gameQueries);
            auto it = headers.begin();
            for (int i = 0; i < moves.size(); ++i)
            {
                AggregatedQueryResult& aggResult = aggResults.continuations[i].second;
                for (GameLevel level : values<GameLevel>())
                {
                    for (GameResult result : values<GameResult>())
                    {
                        const std::size_t directCount = aggResult.counts[level][result].second;

                        if (fetchFirstGameForContinuations && directCount > 0)
                        {
                            aggResult.games[level][result] = persistence::GameHeader(*it++);
                        }
                    }
                }
            }

            AggregatedQueryResult& aggResult = aggResults.main;
            for (GameLevel level : values<GameLevel>())
            {
                for (GameResult result : values<GameResult>())
                {
                    const std::size_t count = aggResult.counts[level][result].first;

                    if (fetchFirstGame && count > 0)
                    {
                        aggResult.games[level][result] = persistence::GameHeader(*it++);
                    }
                }
            }
        }

        return aggResults;
    }

    void printAggregatedResult(const AggregatedQueryResult& res)
    {
        std::size_t total = 0;
        std::size_t totalDirect = 0;
        for (auto& cc : res.counts)
        {
            for (auto& [c0, c1] : cc)
            {
                total += c0;
                totalDirect += c1;
            }
        }
        std::cout << std::setw(5) << total << ' ' << totalDirect << ' ';

        for (auto& cc : res.counts)
        {
            std::cout << std::setw(19) << resultsToString(cc) << ' ';
        }

        std::cout << '\n';

        const persistence::GameHeader* firstGame = nullptr;
        for (auto& gg : res.games)
        {
            for (auto& g : gg)
            {
                if (!g.has_value())
                {
                    continue;
                }

                if (firstGame == nullptr || g->gameIdx() < firstGame->gameIdx())
                {
                    firstGame = &*g;
                }
            }
        }

        if (firstGame)
        {
            std::string plyCount = firstGame->plyCount() ? std::to_string(*firstGame->plyCount()) : "-"s;
            std::cout
                << firstGame->date().toString()
                << ' ' << toString(GameResultWordFormat{}, firstGame->result())
                << ' ' << firstGame->eco().toString()
                << ' ' << firstGame->event()
                << ' ' << plyCount
                << ' ' << firstGame->white()
                << ' ' << firstGame->black()
                << '\n';
        }
    }

    std::pair<std::string, std::vector<std::string>> parseCommand(const std::string& cmd)
    {
        std::pair<std::string, std::vector<std::string>> parts;

        bool escaped = false;
        bool args = false;
        for (char c : cmd)
        {
            if (c == '`')
            {
                escaped = !escaped;
                continue;
            }

            if (!escaped && std::isspace(c))
            {
                parts.second.emplace_back();
                args = true;
            }
            else if (args)
            {
                parts.second.back() += c;
            }
            else
            {
                parts.first += c;
            }
        }

        return parts;
    }

    struct RemoteQuery
    {
        std::string token; // token can be used to match queries to results by the client
        std::vector<std::string> fens;
        std::vector<GameLevel> levels;
        std::vector<GameResult> results;
        bool fetchFirstGame;
        bool fetchLastGame;
        bool continuations;
        bool fetchFirstGameForEachContinuation;
        bool fetchLastGameForEachContinuation;
        bool excludeTranspositions;

        friend void to_json(nlohmann::json& j, const RemoteQuery& query)
        {
            std::vector<std::string_view> levelsStr;
            levelsStr.reserve(query.levels.size());
            for (auto&& level : query.levels)
            {
                levelsStr.emplace_back(toString(level));
            }

            std::vector<std::string_view> resultsStr;
            resultsStr.reserve(resultsStr.size());
            for (auto&& result : query.results)
            {
                resultsStr.emplace_back(toString(GameResultWordFormat{}, result));
            }

            j = nlohmann::json{ 
                { "token", query.token },
                { "fens", query.fens },
                { "levels", levelsStr },
                { "results", resultsStr },
                { "fetch_first_game", query.fetchFirstGame },
                { "fetch_last_game", query.fetchLastGame },
                { "continuations", query.continuations },
                { "fetch_first_game_for_each_continuation", query.fetchFirstGameForEachContinuation },
                { "fetch_last_game_for_each_continuation", query.fetchLastGameForEachContinuation },
                { "exclude_transpositions", query.excludeTranspositions }
            };
        }

        friend void from_json(const nlohmann::json& j, RemoteQuery& query)
        {
            query.fens.clear();
            query.levels.clear();
            query.results.clear();

            {
                std::vector<std::string_view> levelsStr;
                j["levels"].get_to(levelsStr);
                for (auto&& levelStr : levelsStr)
                {
                    query.levels.emplace_back(fromString<GameLevel>(levelStr));
                }
            }

            {
                std::vector<std::string_view> resultsStr;
                j["results"].get_to(resultsStr);
                for (auto&& resultStr : resultsStr)
                {
                    query.results.emplace_back(fromString<GameResult>(GameResultWordFormat{}, resultStr));
                }
            }

            j["token"].get_to(query.token);
            j["fens"].get_to(query.fens);
            j["fetch_first_game"].get_to(query.fetchFirstGame);
            j["fetch_last_game"].get_to(query.fetchLastGame);
            j["continuations"].get_to(query.continuations);
            j["fetch_first_game_for_each_continuation"].get_to(query.fetchFirstGameForEachContinuation);
            j["fetch_last_game_for_each_continuation"].get_to(query.fetchLastGameForEachContinuation);
            j["exclude_transpositions"].get_to(query.excludeTranspositions);
        }
    };

    struct RemoteQueryResultForPosition
    {
        RemoteQueryResultForPosition() :
            level(GameLevel::Human),
            result(GameResult::Draw),
            count(0),
            transpositionCount{}
        {
        }

        RemoteQueryResultForPosition(GameLevel level, GameResult result, std::size_t count) :
            level(level),
            result(result),
            count(count),
            transpositionCount{}
        {
        }

        RemoteQueryResultForPosition(GameLevel level, GameResult result, std::size_t count, std::size_t transpositionCount) :
            level(level),
            result(result),
            count(count),
            transpositionCount(transpositionCount)
        {
        }

        GameLevel level;
        GameResult result;
        std::size_t count;
        std::optional<std::size_t> transpositionCount;
        std::optional<persistence::GameHeader> firstGame;
        std::optional<persistence::GameHeader> lastGame;

        friend void to_json(nlohmann::json& j, const RemoteQueryResultForPosition& data)
        {
            nlohmann::json body = nlohmann::json::object({
                { "count", data.count }
            });
            if (data.transpositionCount.has_value())
            {
                body["transposition_count"] = *data.transpositionCount;
            }
            if (data.firstGame.has_value())
            {
                body["first_game"] = *data.firstGame;
            }
            if (data.lastGame.has_value())
            {
                body["last_game"] = *data.lastGame;
            }

            j = nlohmann::json{
                { 
                    toString(data.level), {
                        { toString(GameResultWordFormat{}, data.result), std::move(body) }
                    }
                }
            };
        }
    };

    struct RemoteQueryResultForFen
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

        RemoteQueryResultForFen(std::string fen) :
            fen(std::move(fen))
        {
        }

        std::string fen;
        std::vector<RemoteQueryResultForPosition> main;
        std::map<Move, std::vector<RemoteQueryResultForPosition>, MoveCompareLess> continuations;

        friend void to_json(nlohmann::json& j, const RemoteQueryResultForFen& data)
        {
            const std::optional<Position> positionOpt = Position::tryFromFen(data.fen.c_str());
            if (!positionOpt.has_value())
            {
                j = nlohmann::json{
                    { "fen", data.fen },

                    // We don't have to put this, we don't define it anywhere, but we may
                    { "error", "Invalid FEN"}
                };

                return;
            }
            const auto& position = positionOpt.value();

            j = nlohmann::json{
                { "fen", data.fen },
                { "main", data.main }
            };

            if (!data.continuations.empty())
            {
                auto& continuations = j["continuations"];
                for (auto&& [move, ress] : data.continuations)
                {
                    // Move as a key
                    auto sanStr = san::moveToSan<san::SanSpec::Capture | san::SanSpec::Check | san::SanSpec::Compact>(position, move);
                    auto& entry = continuations[sanStr];
                    for (auto&& res : ress)
                    {
                        entry.merge_patch(res);
                    }
                }
            }
        }
    };

    struct RemoteQueryResult
    {
        RemoteQuery query;
        std::vector<RemoteQueryResultForFen> subResults;

        friend void to_json(nlohmann::json& j, const RemoteQueryResult& data)
        {
            j = nlohmann::json{
                { "query", data.query },
                { "results", data.subResults }
            };
        }
    };

    [[nodiscard]] auto gatherPositionsForQuery(const std::vector<std::string>& fens, bool continuations)
    {
        // NOTE: we don't remove duplicates because there should be no
        //       duplicates when we consider reverse move

        std::vector<Position> positions;
        std::vector<std::size_t> fenIds;
        std::vector<ReverseMove> reverseMoves;
        for (std::size_t i = 0; i < fens.size(); ++i)
        {
            auto&& fen = fens[i];

            // If the fen is invalid we use the start position as a placeholder
            // Then when reading the results we will disregard them if they come from an invalid fen.
            const std::optional<Position> posOpt = Position::tryFromFen(fen.c_str());
            const Position pos = posOpt.value_or(Position::startPosition());
            positions.emplace_back(pos);
            fenIds.emplace_back(i);
            reverseMoves.emplace_back();

            if (continuations)
            {
                movegen::forEachLegalMove(pos, [&](Move move) {
                    auto posCpy = pos;
                    reverseMoves.emplace_back(posCpy.doMove(move));
                    positions.emplace_back(posCpy);
                    fenIds.emplace_back(i);
                });
            }
        }

        return std::make_tuple(std::move(positions), std::move(fenIds), std::move(reverseMoves));
    }

    [[nodiscard]] RemoteQueryResult executeQuery(persistence::local::Database& db, const RemoteQuery& query)
    {
        auto [positions, fenIds, reverseMoves] = gatherPositionsForQuery(query.fens, query.continuations);

        std::vector<persistence::local::QueryTarget> targets;
        for (auto&& level : query.levels)
        {
            for (auto&& result : query.results)
            {
                targets.emplace_back(persistence::local::QueryTarget{ level, result });
            }
        }

        // perform the query only for the chosen targets
        EnumMap2<GameLevel, GameResult, std::vector<persistence::local::QueryResult>> rangeResults = 
            db.queryRanges(targets, positions, 
                // If we pass empty vector then queries for direct instances are not made.
                query.excludeTranspositions 
                ? reverseMoves
                : std::vector<ReverseMove>{}
            );

        // Next we slowly populate the QueryResult with retrieved data.
        // This requires some remapping.
        RemoteQueryResult queryResult{ query };

        // Each initial fen has a result, NOT each distinct position.
        // Each fen 'owns' N positions.
        for (std::size_t i = 0; i < query.fens.size(); ++i)
        {
            queryResult.subResults.emplace_back(query.fens[i]);
        }

        // We want to batch the header queries so we have to do some bookkeeping
        // We have to know where to assign it later.
        std::vector<std::uint32_t> headerQueries;
        struct HeaderFor
        {
            using HeaderMemberPtr = std::optional<persistence::GameHeader> RemoteQueryResultForPosition::*;

            HeaderFor(std::size_t fenId, Move move, std::size_t bucketId, HeaderMemberPtr headerPtr) :
                fenId(fenId),
                move(move),
                bucketId(bucketId),
                headerPtr(headerPtr)
            {
            }

            std::size_t fenId;
            Move move;
            std::size_t bucketId;
            HeaderMemberPtr headerPtr;
        };

        // We have to know where to assign the header later
        // We cannot keep a vector of std::pairs because the query function
        // requires a vector of ints.
        // The relationship is implicated by indices.
        std::vector<HeaderFor> headerQueriesMappings;

        // We only care about the targets specified for the query.
        for (auto&& level : query.levels)
        {
            for (auto&& result : query.results)
            {
                for (std::size_t i = 0; i < positions.size(); ++i)
                {
                    const auto& rangeResult = rangeResults[level][result][i];
                    const auto count = rangeResult.count();
                    const auto directCount = rangeResult.directCount();
                    // We initialize the results even if count == 0.
                    // We want to present that information to the user.

                    // These are not always present but we don't want to defer getting the indices.
                    const auto firstGameIdx = count > 0 ? rangeResult.firstGameIndex() : 0;
                    const auto lastGameIdx = count > 0 ? rangeResult.lastGameIndex() : 0;
                    const auto firstDirectGameIdx = directCount > 0 ? rangeResult.firstDirectGameIndex() : 0;
                    const auto lastDirectGameIdx = directCount > 0 ? rangeResult.lastDirectGameIndex() : 0;

                    // One position maps to at least one fen.
                    // The mapping is only on our side to reduce queries, the client
                    // wants to see multiple instances of the same position.
                    // If move != Move::null() it is a continuation of a fen.
                    const auto& fenId = fenIds[i];
                    const auto& reverseMove = reverseMoves[i];
                    const auto& move = reverseMove.move;
                    auto& subResult = queryResult.subResults[fenId];

                    if (move == Move::null())
                    {
                        // Main position
                        const std::size_t id = subResult.main.size();
                        subResult.main.emplace_back(level, result, count);

                        if (count > 0)
                        {
                            if (query.fetchFirstGame)
                            {
                                headerQueries.emplace_back(firstGameIdx);
                                headerQueriesMappings.emplace_back(fenId, move, id, &RemoteQueryResultForPosition::firstGame);
                            }
                            if (query.fetchLastGame)
                            {
                                headerQueries.emplace_back(lastGameIdx);
                                headerQueriesMappings.emplace_back(fenId, move, id, &RemoteQueryResultForPosition::lastGame);
                            }
                        }
                    }
                    else
                    {
                        // Continuation
                        auto& bucket = subResult.continuations[move];

                        const std::size_t id = bucket.size();
                        // If we queried transposition info then we have to
                        // change some values as the expected query response is different.
                        if (query.excludeTranspositions)
                        {
                            bucket.emplace_back(level, result, directCount, count - directCount);
                        }
                        else
                        {
                            bucket.emplace_back(level, result, count);
                        }

                        if (query.excludeTranspositions && directCount > 0)
                        {
                            if (query.fetchFirstGameForEachContinuation)
                            {
                                headerQueries.emplace_back(firstDirectGameIdx);
                                headerQueriesMappings.emplace_back(fenId, move, id, &RemoteQueryResultForPosition::firstGame);
                            }
                            if (query.fetchLastGameForEachContinuation)
                            {
                                headerQueries.emplace_back(lastDirectGameIdx);
                                headerQueriesMappings.emplace_back(fenId, move, id, &RemoteQueryResultForPosition::lastGame);
                            }
                        }
                        else if (!query.excludeTranspositions && count > 0)
                        {
                            if (query.fetchFirstGameForEachContinuation)
                            {
                                headerQueries.emplace_back(firstGameIdx);
                                headerQueriesMappings.emplace_back(fenId, move, id, &RemoteQueryResultForPosition::firstGame);
                            }
                            if (query.fetchLastGameForEachContinuation)
                            {
                                headerQueries.emplace_back(lastGameIdx);
                                headerQueriesMappings.emplace_back(fenId, move, id, &RemoteQueryResultForPosition::lastGame);
                            }
                        }
                    }
                }
            }
        }

        // Query all headers at once and assign them to
        // their respective positions.
        auto packedHeaders = db.queryHeadersByIndices(headerQueries);
        for (std::size_t i = 0; i < packedHeaders.size(); ++i)
        {
            auto& packedHeader = packedHeaders[i];
            auto [fenId, move, bucketId, headerPtr] = headerQueriesMappings[i];

            auto& pos = move == Move::null()
                ? queryResult.subResults[fenId].main[bucketId]
                : queryResult.subResults[fenId].continuations[move][bucketId];

            (pos.*headerPtr).emplace(packedHeader);
        }

        return queryResult;
    }

    struct InvalidCommand : std::runtime_error
    {
        using std::runtime_error::runtime_error;
    };

    void assertDirectoryNotEmpty(const std::filesystem::path& path)
    {
        if (!std::filesystem::exists(path) || std::filesystem::is_empty(path))
        {
            throw InvalidCommand("Directory " + path.string() + " doesn't exist or is empty");
        }
    }

    void assertDirectoryEmpty(const std::filesystem::path& path)
    {
        if (std::filesystem::exists(path) && !std::filesystem::is_empty(path))
        {
            throw InvalidCommand("Directory " + path.string() + " is not empty");
        }
    }

    [[noreturn]] void invalidCommand(const std::string& command)
    {
        throw InvalidCommand("Invalid command: " + command);
    }

    [[noreturn]] void invalidArguments()
    {
        throw InvalidCommand("Invalid arguments. See help.");
    }

    void bench(const std::vector<std::filesystem::path>& paths)
    {
        std::size_t ct = 0;
        std::size_t size = 0;
        double time = 0;
        for (auto&& path : paths)
        {
            size += std::filesystem::file_size(path);

            pgn::LazyPgnFileReader reader(path, 4 * 1024 * 1024);
            std::size_t c = 0;
            auto t0 = std::chrono::high_resolution_clock::now();
            for (auto&& game : reader)
            {
                for (auto&& position : game.positions())
                {
                    ++c;
                }
            }
            auto t1 = std::chrono::high_resolution_clock::now();
            time += (t1 - t0).count() / 1e9;
            ct += c;
        }
        std::cout << ct << " positions in " << time << "s\n";
        std::cout << "Throughput of " << size / time / 1e6 << " MB/s\n";
    }

    [[nodiscard]] std::unique_ptr<persistence::local::Database> open(const std::filesystem::path& path)
    {
        assertDirectoryNotEmpty(path);

        return std::make_unique<persistence::local::Database>(path);
    }

    void query(persistence::local::Database& db, const Position& pos)
    {
        auto agg = queryAggregate(db, pos, true, true, true, false);

        printAggregatedResult(agg.main);
        std::cout << "\n";
        for (auto&& [move, res] : agg.continuations)
        {
            std::cout << std::setw(8) << san::moveToSan<san::SanSpec::Capture | san::SanSpec::Check | san::SanSpec::Compact>(pos, move) << " ";
            printAggregatedResult(res);
        }
    }

    void merge(persistence::local::Database& db, const std::filesystem::path& destination)
    {
        assertDirectoryNotEmpty(destination);

        db.replicateMergeAll(destination);
    }

    void merge(persistence::local::Database& db)
    {
        db.mergeAll();
    }

    [[nodiscard]] bool verifyPgnTags(const pgn::UnparsedGame& game, std::size_t idx)
    {
        const auto result = game.result();
        if (!result.has_value())
        {
            std::cerr << "Game " << idx << " has invalid result tag with value \"" << game.tag("Result"sv) << "\"\n";
            return false;
        }
        return true;
    }

    [[nodiscard]] bool verifyPgnMoves(const pgn::UnparsedGame& game, std::size_t idx)
    {
        Position pos = Position::startPosition();
        std::size_t moveCount = 0;
        for (auto&& san : game.moves())
        {
            const std::optional<Move> move = san::trySanToMove(pos, san);
            if (!move.has_value() || *move == Move::null())
            {
                std::cerr << "Game " << idx << " has an invalid move \"" << san << "\"\n";
                return false;
            }

            pos.doMove(*move);

            ++moveCount;
        }
        if (moveCount == 0)
        {
            std::cerr << "Game " << idx << " has no moves\n";
        }
        return true;
    }

    void verifyPgn(const std::filesystem::path& path)
    {
        constexpr std::size_t progressEvery = 100000;

        pgn::LazyPgnFileReader reader(path);
        std::size_t idx = 0; // 1-based index, increment at the start of the loop
        for (auto&& game : reader)
        {
            if (idx % progressEvery == 0)
            {
                std::cout << "So far verified " << idx << " games...\n";
            }

            ++idx;

            if (!verifyPgnTags(game, idx)) continue;
            if (!verifyPgnMoves(game, idx)) continue;
        }
        std::cerr << "Verified " << idx << " games.\n";
    }

    void info(const persistence::local::Database& db, std::ostream& out)
    {
        db.printInfo(out);
    }

    void create(const std::filesystem::path& destination, const persistence::local::PgnFiles& pgns, const std::filesystem::path& temp)
    {
        assertDirectoryEmpty(destination);
        assertDirectoryEmpty(temp);

        {
            persistence::local::Database db(temp);
            db.importPgns(pgns, importMemory);
            db.replicateMergeAll(destination);
        }
        std::filesystem::remove_all(temp);
    }

    void create(const std::filesystem::path& destination, const persistence::local::PgnFiles& pgns)
    {
        assertDirectoryEmpty(destination);

        persistence::local::Database db(destination);
        db.importPgns(pgns, importMemory);
    }

    void destroy(std::unique_ptr<persistence::local::Database> db)
    {
        if (db == nullptr)
        {
            return;
        }

        const auto path = db->path();
        db.reset();
        std::filesystem::remove_all(path);
    }

    struct App
    {
    private:
        using Args = std::vector<std::string>;

        using CommandFunction = std::function<void(App*, const Args&)>;

        void assertDatabaseOpened() const
        {
            if (m_database == nullptr)
            {
                throw InvalidCommand("No database opened.");
            }
        }

    public:

        App()
        {
        }

        void run()
        {
            for (;;)
            {
                std::string cmdline;
                std::getline(std::cin, cmdline);
                auto [cmd, args] = parseCommand(cmdline);

                if (cmd == "exit"sv)
                {
                    return;
                }

                try
                {
                    m_commands.at(cmd)(this, args);
                }
                catch (std::out_of_range&)
                {
                    std::cout << "Unknown command." << '\n';
                }
                catch (InvalidCommand& e)
                {
                    std::cout << e.what() << '\n';
                }
            }
        }

    private:
        std::unique_ptr<persistence::local::Database> m_database;

        void help(const Args& args) const
        {
            std::cout << "Commands:\n";
            std::cout << "bench, open, query, help, info, close, exit, merge, create, destroy\n";
            std::cout << "arguments are split at spaces\n";
            std::cout << "arguments with spaces can be escaped with `` (tilde)\n";
            std::cout << "for example bench `c:/pgn a.pgn`\n\n\n";

            std::cout << "bench <path> - counts the number of moves in pgn file at `path` and measures time taken\n\n";
            std::cout << "open <path> - opens an already existing database located at `path`\n\n";
            std::cout <<
                "query <fen> - queries the currently open database with a position specified by fen. "
                "NOTE: you most likely want to use `` as fens usually have spaces in them.\n\n";
            std::cout << "help - brings up this page\n\n";
            std::cout << "info - outputs information about the currently open database. For example file locations, sizes, partitions...\n\n";
            std::cout << "close - closes the currently open database\n\n";
            std::cout << "exit - gracefully exits the program, ensures everything is cleaned up\n\n";
            std::cout << "merge <path_to> - replicates the currently open database into `path_to`, and merges the files along the way.\n\n";
            std::cout << "merge - merges the files in the currently open database\n\n";
            std::cout << "verify <path> - verifies the pgn at the given path\n\n";
            std::cout <<
                "create <path> <pgn_list_file_path> [<path_temp>] - creates a database from files given in file at `pgn_list_file_path` (more about it below). "
                "If `path_temp` IS NOT specified then the files are not merged after the import is done. "
                "If `path_temp` IS specified then pgns are first imported into the temporary directory and then merged into the final directory. "
                "Both `path` and `path_temp` must either point to a non-esistent directory or the directory must be empty. "
                "A file at `pgn_list_file_path` specifies the pgn files to be imported. Each line contains 2 values separated by a semicolon (;). "
                "The first value is one of human, engine, server. The second value is the path to the pgn file.\n\n";
            std::cout << "destroy - closes and deletes the currently open database.\n\n";
        }

        void bench(const Args& args)
        {
            std::vector<std::filesystem::path> paths;
            for (auto&& path : args) paths.emplace_back(path);
            app::bench(paths);
        }

        void open(const Args& args)
        {
            if (args.size() != 1)
            {
                invalidArguments();
            }

            m_database = app::open(args[0]);
        }

        void query(const Args& args)
        {
            assertDatabaseOpened();

            if (args.size() != 1)
            {
                invalidArguments();
            }

            std::optional<Position> position = Position::tryFromFen(args[0]);
            if (!position.has_value())
            {
                throw InvalidCommand("Invalid fen.");
            }

            app::query(*m_database, *position);
        }

        void info(const Args& args)
        {
            assertDatabaseOpened();

            app::info(*m_database, std::cout);
        }

        void merge(const Args& args)
        {
            assertDatabaseOpened();

            if (args.size() > 1)
            {
                invalidArguments();
            }

            if (args.size() == 1)
            {
                app::merge(*m_database, args[0]);
            }
            else if (args.size() == 0)
            {
                app::merge(*m_database);
            }
        }

        void verify(const Args& args)
        {
            if (args.size() != 1)
            {
                invalidArguments();
            }

            verifyPgn(args[0]);
        }

        void close(const Args& args)
        {
            m_database.reset();
        }

        void create(const Args& args)
        {
            if (args.size() < 2 || args.size() > 3)
            {
                invalidArguments();
            }

            const std::filesystem::path destination(args[0]);
            auto pgns = parsePgnListFile(args[1]);
            if (args.size() == 3)
            {
                const std::filesystem::path temp(args[2]);
                app::create(destination, pgns, temp);
            }
            else if (args.size() == 2)
            {
                app::create(destination, pgns);
            }
        }

        void destroy(const Args& args)
        {
            assertDatabaseOpened();

            app::destroy(std::move(m_database));
        }

        static inline const std::map<std::string_view, CommandFunction> m_commands = {
            { "bench"sv, &App::bench },
            { "open"sv, &App::open },
            { "query"sv, &App::query },
            { "help"sv, &App::help },
            { "info"sv, &App::info },
            { "close"sv, &App::close },
            { "merge"sv, &App::merge },
            { "verify"sv, &App::verify },
            { "create"sv, &App::create },
            { "destroy"sv, &App::destroy }
        };
    };
}

void jsonExample()
{
    app::RemoteQuery q;
    q.token = "toktok";
    q.fens = { "asd", "dsa" };
    q.fetchFirstGame = false;
    q.fetchLastGame = true;
    q.fetchFirstGameForEachContinuation = false;
    q.fetchLastGameForEachContinuation = true;
    q.excludeTranspositions = false;
    q.continuations = false;
    q.levels = { GameLevel::Human, GameLevel::Engine, GameLevel::Server };
    q.results = { GameResult::WhiteWin, GameResult::BlackWin, GameResult::Draw };
    auto j = nlohmann::json(q);
    std::cout << j.dump() << '\n';
    q = j;
    std::cout << nlohmann::json(q).dump(4) << '\n';

    std::cout << "\n\n\n";
    cfg::g_config.print(std::cout);

    app::RemoteQueryResultForPosition data(GameLevel::Human, GameResult::Draw, 123, 321);
    data.lastGame = persistence::GameHeader(
        0,
        GameResult::Draw,
        Date(2000, 2, 3),
        Eco("A12"sv),
        123,
        "eventasd",
        "whiteaasc",
        "blackasd"
    );
    std::cout << nlohmann::json(data).dump(4) << '\n';

    app::RemoteQueryResultForFen forFen("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1");
    forFen.main.emplace_back(data);
    forFen.continuations[Move{ E2, E4 }].emplace_back(data);

    app::RemoteQueryResult result;
    result.query = q;
    result.subResults.emplace_back(forFen);
    result.subResults.emplace_back(forFen);
    std::cout << nlohmann::json(result).dump(4) << '\n';
}

void testQuery()
{
    app::RemoteQuery query;
    query.token = "toktok";
    //query.fens = { "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1" };
    query.fens = { "rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq - 0 2" };
    query.fetchFirstGame = true;
    query.fetchLastGame = true;
    query.fetchFirstGameForEachContinuation = true;
    query.fetchLastGameForEachContinuation = true;
    query.excludeTranspositions = true;
    query.continuations = true;
    query.levels = { GameLevel::Human, GameLevel::Engine, GameLevel::Server };
    query.results = { GameResult::WhiteWin, GameResult::BlackWin, GameResult::Draw };
    persistence::local::Database db("w:/catobase/.tmp");
    auto result = app::executeQuery(db, query);
    std::cout << nlohmann::json(result).dump(4) << '\n';
}

void newFormatTests()
{
    persistence::hdd::Database db("w:/catobase/.v2");
    //persistence::hdd::Database db("c:/dev/chess_pos_db/.v2");
    std::vector<persistence::hdd::PgnFile> files{
        { "w:/catobase/data/lichess_db_standard_rated_2013-01.pgn", GameLevel::Human },
        { "w:/catobase/data/lichess_db_standard_rated_2013-02.pgn", GameLevel::Engine },
        { "w:/catobase/data/lichess_db_standard_rated_2013-03.pgn", GameLevel::Server },
        { "w:/catobase/data/lichess_db_standard_rated_2013-04.pgn", GameLevel::Human },
        { "w:/catobase/data/lichess_db_standard_rated_2013-05.pgn", GameLevel::Engine },
        { "w:/catobase/data/lichess_db_standard_rated_2013-06.pgn", GameLevel::Server },
        { "w:/catobase/data/lichess_db_standard_rated_2013-07.pgn", GameLevel::Human },
        { "w:/catobase/data/lichess_db_standard_rated_2013-08.pgn", GameLevel::Engine },
        { "w:/catobase/data/lichess_db_standard_rated_2013-09.pgn", GameLevel::Server },
        { "w:/catobase/data/lichess_db_standard_rated_2013-10.pgn", GameLevel::Human },
        { "w:/catobase/data/lichess_db_standard_rated_2013-11.pgn", GameLevel::Engine },
        { "w:/catobase/data/lichess_db_standard_rated_2013-12.pgn", GameLevel::Server }
    };
    db.importPgns(files, app::importMemory);
}

int main()
{
    //newFormatTests();

    
    app::App app;
    app.run();
    

    return 0;
}
