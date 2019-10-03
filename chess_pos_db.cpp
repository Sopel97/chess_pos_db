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
#include "MoveGenerator.h"
#include "Pgn.h"
#include "Position.h"
#include "PositionSignature.h"
#include "San.h"

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
        EnumMap2<GameLevel, GameResult, std::size_t> counts;
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

    std::string resultsToString(const EnumMap<GameResult, std::size_t>& results)
    {
        auto str = std::string("+") + std::to_string(results[GameResult::WhiteWin]);
        str += std::string("=") + std::to_string(results[GameResult::Draw]);
        str += std::string("-") + std::to_string(results[GameResult::BlackWin]);
        return str;
    }

    std::string toString(GameResult res)
    {
        switch (res)
        {
        case GameResult::WhiteWin:
            return "win";
        case GameResult::BlackWin:
            return "loss";
        case GameResult::Draw:
            return "draw";
        }
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
        std::vector<Move> moves;
        if (queryContinuations)
        {
            movegen::forEachLegalMove(basePosition, [&](Move move) {
                positions.emplace_back(basePosition.afterMove(move));
                moves.emplace_back(move);
                });
        }
        positions.emplace_back(basePosition);

        AggregatedQueryResults aggResults;
        aggResults.mainPosition = pos;
        std::vector<std::uint32_t> gameQueries;
        auto results = db.queryRanges(positions);
        for (int i = 0; i < moves.size(); ++i)
        {
            AggregatedQueryResult aggResult;
            std::size_t totalCount = 0;
            for (GameLevel level : values<GameLevel>())
            {
                for (GameResult result : values<GameResult>())
                {
                    const std::size_t count = results[level][result][i].count();
                    aggResult.counts[level][result] = count;
                    totalCount += count;

                    if (fetchFirstGameForContinuations && count > 0)
                    {
                        gameQueries.emplace_back(results[level][result][i].firstGameIndex());
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
                    aggResult.counts[level][result] = count;

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
                        const std::size_t count = aggResult.counts[level][result];

                        if (fetchFirstGameForContinuations && count > 0)
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
                    const std::size_t count = aggResult.counts[level][result];

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
        for (auto& cc : res.counts)
        {
            for (auto& c : cc)
            {
                total += c;
            }
        }
        std::cout << std::setw(9) << total << ' ';

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

                if (firstGame == nullptr || g->date() < firstGame->date())
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
                << ' ' << toString(firstGame->result())
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
        std::vector<std::string> fens;
        std::vector<GameLevel> levels;
        std::vector<GameResult> results;
        bool fetchFirstGame;
        bool fetchLastGame;
        bool continuations;
        bool fetchFirstGameForEachContinuation;
        bool fetchLastGameForEachContinuation;
    };

    struct RemoteQueryResultForPosition
    {
        RemoteQueryResultForPosition() :
            level(GameLevel::Human),
            result(GameResult::Draw),
            count(0)
        {
        }

        RemoteQueryResultForPosition(GameLevel level, GameResult result, std::size_t count) :
            level(level),
            result(result),
            count(count)
        {
        }

        GameLevel level;
        GameResult result;
        std::size_t count;
        std::optional<persistence::GameHeader> firstGame;
        std::optional<persistence::GameHeader> lastGame;
    };

    struct RemoteQueryResultForFen
    {
        RemoteQueryResultForFen(std::string fen) :
            fen(std::move(fen))
        {
        }

        std::string fen;
        RemoteQueryResultForPosition main;
        std::vector<std::pair<Move, RemoteQueryResultForPosition>> continuations;
    };

    struct RemoteQueryResult
    {
        std::vector<RemoteQueryResultForFen> subResults;
    };

    // Gathers positions, removes duplicates, provides mapping to retrieve
    // which fen corresponds to which position and which position
    // arose as a continuation of a given fen.
    [[nodiscard]] auto gatherPositionsForQuery(const std::vector<std::string>& fens, bool continuations)
    {
        struct PositionOrigin
        {
            std::size_t fenId;

            // Not null move here means that it is the base position
            // straight from the provided fen.
            // It does NOT mean an actual null move where the side to move would change.
            Move move;
        };

        // One position can have multiple origins if it
        // shows up multiple times.
        using PositionOrigins = std::vector<PositionOrigin>;

        auto hasher = [](const Position& pos)
        {
            return std::hash<PositionSignature>{}(PositionSignature(pos));
        };

        // we store position in a map to remove duplicates - so we don't query duplicates
        // we also keep mappings so we know which position originated
        // from which fen
        std::unordered_map<Position, PositionOrigins, decltype(hasher)> positions({}, hasher);

        for (std::size_t i = 0; i < fens.size(); ++i)
        {
            auto&& fen = fens[i];
            Position pos = Position::fromFen(fen.c_str());
            positions[pos].emplace_back(PositionOrigin{ i, Move::null() });

            if (continuations)
            {
                movegen::forEachLegalMove(pos, [&](Move move) {
                    positions[pos.afterMove(move)].emplace_back(PositionOrigin{ i, move });
                });
            }
        }

        std::vector<Position> distinctPositions;
        std::vector<PositionOrigins> origins;
        for (auto&& [pos, origin] : positions)
        {
            distinctPositions.emplace_back(pos);
            origins.emplace_back(std::move(origin));
        }

        return std::make_pair(std::move(distinctPositions), std::move(origins));
    }

    [[nodiscard]] RemoteQueryResult executeQuery(persistence::local::Database& db, const RemoteQuery& query)
    {
        auto [positions, origins] = gatherPositionsForQuery(query.fens, query.continuations);

        std::vector<persistence::local::QueryTarget> targets;
        for (auto&& level : query.levels)
        {
            for (auto&& result : query.results)
            {
                targets.emplace_back(persistence::local::QueryTarget{ level, result });
            }
        }

        // perform the query only for the chosen targets
        EnumMap2<GameLevel, GameResult, std::vector<persistence::local::QueryResult>> rangeResults = db.queryRanges(targets, positions);

        // Next we slowly populate the QueryResult with retrieved data.
        // This requires some remapping.
        RemoteQueryResult queryResult;
        // Each initial fen has a result, NOT each distinct position.
        // Each fen 'owns' N positions.
        for (std::size_t i = 0; i < query.fens.size(); ++i)
        {
            queryResult.subResults.emplace_back(query.fens[i]);
        }

        // We want to batch the header queries so we have to do some bookkeeping
        std::vector<std::uint32_t> headerQueries;
        struct HeaderFor
        {
            using HeaderMemberPtr = std::optional<persistence::GameHeader> RemoteQueryResultForPosition::*;

            HeaderFor(std::size_t fenId, std::size_t continuationId, HeaderMemberPtr headerPtr) :
                fenId(fenId),
                continuationId(continuationId),
                headerPtr(headerPtr)
            {
            }

            std::size_t fenId;
            std::size_t continuationId; // if -1 then main
            HeaderMemberPtr headerPtr;
        };
        constexpr std::size_t noContinuationId = -1;

        // We have to know where to assign the header later
        // We cannot keep a vector of std::pairs because the query function
        // requires a vector of ints.
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
                    // We initialize the results even if count == 0.
                    // We want to present that information to the user.

                    // These are not always present but we don't want to defer getting the indices.
                    const auto firstGameIdx = count > 0 ? rangeResult.firstGameIndex() : 0;
                    const auto lastGameIdx = count > 0 ? rangeResult.lastGameIndex() : 0;

                    // One position maps to at least one fen.
                    // The mapping is only on our side to reduce queries, the client
                    // wants to see multiple instances of the same position.
                    // If move != Move::null() it is a continuation of a fen.
                    for (const auto& [fenId, move] : origins[i])
                    {
                        auto& subResult = queryResult.subResults[fenId];
                        if (move == Move::null())
                        {
                            // Main position
                            auto& positionResults = subResult.main;
                            positionResults.count = count;
                            positionResults.level = level;
                            positionResults.result = result;

                            // We schedule header queries if the client wants them.
                            // We pass -1 as a continuation index to signify that it's
                            // a main position. This is done so we don't complicate the function further.
                            if (count > 0)
                            {
                                if (query.fetchFirstGame)
                                {
                                    headerQueries.emplace_back(firstGameIdx);
                                    headerQueriesMappings.emplace_back(fenId, noContinuationId, &RemoteQueryResultForPosition::firstGame);
                                }
                                if (query.fetchLastGame)
                                {
                                    headerQueries.emplace_back(lastGameIdx);
                                    headerQueriesMappings.emplace_back(fenId, noContinuationId, &RemoteQueryResultForPosition::lastGame);
                                }
                            }
                        }
                        else
                        {
                            // Continuation
                            const auto id = subResult.continuations.size();

                            // We have to assign a move to
                            subResult.continuations.emplace_back(
                                std::piecewise_construct,
                                std::forward_as_tuple(move),
                                std::forward_as_tuple(level, result, count)
                            );

                            // We schedule header queries if the client wants them
                            if (count > 0)
                            {
                                if (query.fetchFirstGameForEachContinuation)
                                {
                                    headerQueries.emplace_back(firstGameIdx);
                                    headerQueriesMappings.emplace_back(fenId, id, &RemoteQueryResultForPosition::firstGame);
                                }
                                if (query.fetchLastGameForEachContinuation)
                                {
                                    headerQueries.emplace_back(lastGameIdx);
                                    headerQueriesMappings.emplace_back(fenId, id, &RemoteQueryResultForPosition::lastGame);
                                }
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
            auto [fenId, continuationId, headerPtr] = headerQueriesMappings[i];

            // Again, a special value for the main position. Refactoring it
            // would probably add too much complexity for nothing.
            auto& pos = continuationId == noContinuationId
                ? queryResult.subResults[fenId].main
                : queryResult.subResults[fenId].continuations[continuationId].second;

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

            app::query(*m_database, Position::fromFen(args[0].c_str()));
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
            { "create"sv, &App::create },
            { "destroy"sv, &App::destroy }
        };
    };
}

int main()
{
    app::App app;
    app.run();

    return 0;
}
