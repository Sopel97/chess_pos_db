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
#include "Query.h"
#include "San.h"
#include "Unsort.h"

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
            const auto levelOpt = fromString<GameLevel>(levelStr);
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

    void printAggregatedResult(const query::SegregatedEntries& entriesDirect, const query::SegregatedEntries& entriesTrans)
    {
        std::size_t total = 0;
        std::size_t totalDirect = 0;

        EnumMap2<GameLevel, GameResult, std::pair<std::size_t, std::size_t>> cc{};

        for (auto& [origin, e] : entriesDirect)
        {
            totalDirect += e.count;
            cc[origin.level][origin.result].first += e.count;
            cc[origin.level][origin.result].second += e.count;
        }
        total = totalDirect;
        for (auto& [origin, e] : entriesTrans)
        {
            total += e.count;
            cc[origin.level][origin.result].first += e.count;
        }
        std::cout << std::setw(5) << total << ' ' << totalDirect << ' ';

        for(auto&& c : cc)
            std::cout << std::setw(19) << resultsToString(c) << ' ';

        std::cout << '\n';

        const persistence::GameHeader* firstGame = nullptr;
        for (auto& [origin, e] : entriesDirect)
        {
            if (!e.firstGame.has_value())
            {
                continue;
            }

            if (firstGame == nullptr || e.firstGame->gameIdx() < firstGame->gameIdx())
            {
                firstGame = &*(e.firstGame);
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

    void printAggregatedResults(const query::Response& res)
    {
        for (auto&& result : res.results)
        {
            auto pos = *result.position.tryGet();
            auto direct = result.resultsBySelect.at(query::Select::Continuations);
            auto trans = result.resultsBySelect.at(query::Select::Transpositions);

            printAggregatedResult(direct.root, trans.root);
            for (auto&& [move, e] : direct.children)
            {
                auto&& transE = trans.children.at(move);
                std::cout << std::setw(8) << san::moveToSan<san::SanSpec::Capture | san::SanSpec::Check | san::SanSpec::Compact>(pos, move) << " ";
                printAggregatedResult(e, transE);
            }
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

    void query(persistence::local::Database& db, std::string fen)
    {
        /*
        auto agg = queryAggregate(db, pos, true, true, true, false);

        printAggregatedResult(agg.main);
        std::cout << "\n";
        for (auto&& [move, res] : agg.continuations)
        {
            std::cout << std::setw(8) << san::moveToSan<san::SanSpec::Capture | san::SanSpec::Check | san::SanSpec::Compact>(pos, move) << " ";
            printAggregatedResult(res);
        }
        */

        query::Request query;
        query.token = "toktok";
        query.positions = { { std::move(fen), std::nullopt } };
        //query.positions = { { "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1" , "e5"} };
        query.fetchingOptions[query::Select::Continuations].fetchFirstGame = true;
        query.fetchingOptions[query::Select::Continuations].fetchLastGame = false;
        query.fetchingOptions[query::Select::Continuations].fetchFirstGameForEachChild = true;
        query.fetchingOptions[query::Select::Continuations].fetchLastGameForEachChild = false;
        query.fetchingOptions[query::Select::Continuations].fetchChildren = true;
        query.fetchingOptions[query::Select::Transpositions].fetchFirstGame = true;
        query.fetchingOptions[query::Select::Transpositions].fetchLastGame = false;
        query.fetchingOptions[query::Select::Transpositions].fetchFirstGameForEachChild = true;
        query.fetchingOptions[query::Select::Transpositions].fetchLastGameForEachChild = false;
        query.fetchingOptions[query::Select::Transpositions].fetchChildren = true;
        query.levels = { GameLevel::Human, GameLevel::Engine, GameLevel::Server };
        query.results = { GameResult::WhiteWin, GameResult::BlackWin, GameResult::Draw };
        printAggregatedResults(db.executeQuery(query));
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

            app::query(*m_database, args[0]);
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

int main()
{
    app::App app;
    app.run();

    return 0;
}
