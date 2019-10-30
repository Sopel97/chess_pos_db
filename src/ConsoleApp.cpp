#include "ConsoleApp.h"

#include "chess/Bitboard.h"

#include "algorithm/Unsort.h"

#include "chess/Bitboard.h"
#include "chess/MoveGenerator.h"
#include "chess/Pgn.h"
#include "chess/Position.h"
#include "chess/San.h"

#include "data_structure/Enum.h"
#include "data_structure/EnumMap.h"

#include "external_storage/External.h"

#include "persistence/pos_db/alpha/DatabaseFormatAlpha.h"
#include "persistence/pos_db/beta/DatabaseFormatBeta.h"

#include "persistence/pos_db/Query.h"

#include "Configuration.h"
#include "Logger.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "json/json.hpp"

using namespace std::literals;

namespace console_app
{
    using DbType = persistence::db_beta::Database;

    const std::size_t importMemory = cfg::g_config["console_app"]["pgn_import_memory"].get<MemoryAmount>();

    [[nodiscard]] static persistence::ImportablePgnFiles parsePgnListFile(const std::filesystem::path& path)
    {
        persistence::ImportablePgnFiles pgns;

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

    [[nodiscard]] static std::string resultsToString(const EnumMap<GameResult, std::pair<std::size_t, std::size_t>>& results)
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

    static void printAggregatedResult(const query::SegregatedEntries& entriesDirect, const query::SegregatedEntries& entriesTrans)
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

        for (auto&& c : cc)
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

    static void printAggregatedResults(const query::Response& res)
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

    [[nodiscard]] static std::pair<std::string, std::vector<std::string>> parseCommand(const std::string& cmd)
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

    static void assertDirectoryNotEmpty(const std::filesystem::path& path)
    {
        if (!std::filesystem::exists(path) || std::filesystem::is_empty(path))
        {
            throw InvalidCommand("Directory " + path.string() + " doesn't exist or is empty");
        }
    }

    static void assertDirectoryEmpty(const std::filesystem::path& path)
    {
        if (std::filesystem::exists(path) && !std::filesystem::is_empty(path))
        {
            throw InvalidCommand("Directory " + path.string() + " is not empty");
        }
    }

    static void invalidCommand(const std::string& command)
    {
        throw InvalidCommand("Invalid command: " + command);
    }

    static void invalidArguments()
    {
        throw InvalidCommand("Invalid arguments. See help.");
    }

    static void bench(const std::vector<std::filesystem::path>& paths)
    {
        std::size_t ct = 0;
        std::size_t size = 0;
        double time = 0;
        for (auto&& path : paths)
        {
            size += std::filesystem::file_size(path);

            for (int i = 0; i < 2; ++i)
            {
                // warmup
                pgn::LazyPgnFileReader reader(path, 4 * 1024 * 1024);
                for (auto&& game : reader);
                std::cout << "warmup " << i << " finished\n";
            }

            std::this_thread::sleep_for(std::chrono::seconds{ 1 });

            auto t0 = std::chrono::high_resolution_clock::now();
            pgn::LazyPgnFileReader reader(path, 4 * 1024 * 1024);
            std::size_t c = 0;
            for (auto&& game : reader)
            {
                persistence::PackedGameHeader h(game, 0, 123);

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

    [[nodiscard]] static std::unique_ptr<persistence::Database> open(const std::filesystem::path& path)
    {
        assertDirectoryNotEmpty(path);

        return std::make_unique<DbType>(path);
    }

    static void query(persistence::Database& db, std::string fen, bool json = false)
    {
        query::Request query;
        query.token = "toktok";
        query.positions = { { std::move(fen), std::nullopt } };
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

        auto result = db.executeQuery(query);
        if (json)
        {
            std::cout << nlohmann::json(result).dump(4) << '\n';
        }
        else
        {
            printAggregatedResults(result);
        }
    }

    static void merge(persistence::Database& db, const std::filesystem::path& destination)
    {
        assertDirectoryEmpty(destination);

        db.replicateMergeAll(destination);
    }

    static void merge(persistence::Database& db)
    {
        db.mergeAll();
    }

    [[nodiscard]] static bool verifyPgnTags(const pgn::UnparsedGame& game, std::size_t idx)
    {
        const auto result = game.result();
        if (!result.has_value())
        {
            std::cerr << "Game " << idx << " has invalid result tag with value \"" << game.tag("Result"sv) << "\"\n";
            return false;
        }
        return true;
    }

    [[nodiscard]] static bool verifyPgnMoves(const pgn::UnparsedGame& game, std::size_t idx)
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

    static void verifyPgn(const std::filesystem::path& path)
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

    static void info(const persistence::Database& db, std::ostream& out)
    {
        //db.printInfo(out);
    }

    static void create(const std::filesystem::path& destination, const persistence::ImportablePgnFiles& pgns, const std::filesystem::path& temp)
    {
        assertDirectoryEmpty(destination);
        assertDirectoryEmpty(temp);

        {
            DbType db(temp);
            db.import(pgns, importMemory);
            db.replicateMergeAll(destination);
        }
        std::filesystem::remove_all(temp);
    }

    static void create(const std::filesystem::path& destination, const persistence::ImportablePgnFiles& pgns)
    {
        assertDirectoryEmpty(destination);

        DbType db(destination);
        db.import(pgns, importMemory);
    }

    static void destroy(std::unique_ptr<persistence::Database> db)
    {
        if (db == nullptr)
        {
            return;
        }

        const auto path = db->path();
        db.reset();
        std::filesystem::remove_all(path);
    }

    struct CompressedPosition
    {
        // Occupied bitboard has bits set for 
        // each square with a piece on it.
        // Each packedState byte holds 2 values (nibbles).
        // First one at low bits, second one at high bits.
        // Values correspond to consecutive squares
        // in bitboard iteration order.
        // Nibble values:
        // these are the same as for Piece
        // knights, bishops, queens can just be copied
        //  0 : white pawn
        //  1 : black pawn
        //  2 : white knight
        //  3 : black knight
        //  4 : white bishop
        //  5 : black bishop
        //  6 : white rook
        //  7 : black rook
        //  8 : white queen
        //  9 : black queen
        // 10 : white king
        // 11 : black king
        // 
        // these are special
        // 12 : pawn with ep square behind (white or black, depending on rank)
        // 13 : white rook with coresponding castling rights
        // 14 : black rook with coresponding castling rights
        // 15 : black king and black is side to move
        // 
        // Let N be the number of bits set in occupied bitboard.
        // Only N nibbles are present. (N+1)/2 bytes are initialized.

        Bitboard occupied;
        std::uint8_t packedState[16];

        [[nodiscard]] friend bool operator<(const CompressedPosition& lhs, const CompressedPosition& rhs)
        {
            if (lhs.occupied.bits() < rhs.occupied.bits()) return true;
            if (lhs.occupied.bits() > rhs.occupied.bits()) return false;

            return std::strcmp(reinterpret_cast<const char*>(lhs.packedState), reinterpret_cast<const char*>(rhs.packedState)) < 0;
        }

        [[nodiscard]] friend bool operator==(const CompressedPosition& lhs, const CompressedPosition& rhs)
        {
            return lhs.occupied == rhs.occupied
                && std::strcmp(reinterpret_cast<const char*>(lhs.packedState), reinterpret_cast<const char*>(rhs.packedState)) == 0;
        }
    };

    static CompressedPosition compressPosition(const Position& pos)
    {
        auto compressPiece = [&pos](Square sq, Piece piece) -> std::uint8_t {
            const PieceType type = piece.type();
            const Color color = piece.color();
            
            switch (type)
            {
            case PieceType::Knight:
            case PieceType::Bishop:
            case PieceType::Queen:
                return static_cast<std::uint8_t>(ordinal(piece));

            case PieceType::Pawn:
            {
                if (pos.epSquare() == Square::none())
                {
                    return static_cast<std::uint8_t>(ordinal(piece));
                }
                else
                {
                    const Rank rank = sq.rank();
                    const File file = sq.file();
                    if (file == pos.epSquare().file() && 
                        (
                            (rank == rank4 && pos.sideToMove() == Color::Black)
                            || (rank == rank5) && pos.sideToMove() == Color::White)
                        )
                    {
                        return 12;
                    }
                    else
                    {
                        return static_cast<std::uint8_t>(ordinal(piece));
                    }
                }
            }

            case PieceType::Rook:
            {
                const CastlingRights castlingRights = pos.castlingRights();
                if (color == Color::White
                    && (
                           (sq == A1 && contains(castlingRights, CastlingRights::WhiteQueenSide))
                        || (sq == H1 && contains(castlingRights, CastlingRights::WhiteKingSide))
                       )
                    )
                {
                    return 13;
                }
                else if(
                    color == Color::Black
                    && (
                           (sq == A8 && contains(castlingRights, CastlingRights::BlackQueenSide))
                           || (sq == H8 && contains(castlingRights, CastlingRights::BlackKingSide))
                       )
                       )
                {
                    return 14;
                }
                else
                {
                    return static_cast<std::uint8_t>(ordinal(piece));
                }
            }

            case PieceType::King:
            {
                if (color == Color::White)
                {
                    return 10;
                }
                else if (pos.sideToMove() == Color::White)
                {
                    return 11;
                }
                else
                {
                    return 15;
                }
            }
            }
        };

        const Bitboard occ = pos.piecesBB();

        CompressedPosition compressed;
        compressed.occupied = occ;

        auto it = occ.begin();
        auto end = occ.end();
        for (int i = 0;;++i)
        {
            if (it == end) break;
            compressed.packedState[i] = compressPiece(*it, pos.pieceAt(*it));
            ++it;

            if (it == end) break;
            compressed.packedState[i] |= compressPiece(*it, pos.pieceAt(*it)) << 4;
            ++it;
        }

        return compressed;
    }

    static Position decompressPosition(const CompressedPosition& compressed)
    {
        Position pos;
        pos.setCastlingRights(CastlingRights::None);

        auto decompressPiece = [&pos](Square sq, std::uint8_t nibble) {
            switch (nibble)
            {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
            case 9:
            case 10:
            case 11:
            {
                pos.place(fromOrdinal<Piece>(nibble), sq);
                return;
            }

            case 12:
            {
                const Rank rank = sq.rank();
                const File file = sq.file();
                if (rank == rank4)
                {
                    pos.place(whitePawn, sq);
                    pos.setEpSquareUnchecked(sq + Offset{ 0, -1 });
                }
                else // (rank == rank5)
                {
                    pos.place(blackPawn, sq);
                    pos.setEpSquareUnchecked(sq + Offset{ 0, 1 });
                }
                return;
            }

            case 13:
            {
                pos.place(whiteRook, sq);
                if (sq == A1)
                {
                    pos.addCastlingRights(CastlingRights::WhiteQueenSide);
                }
                else // (sq == H1)
                {
                    pos.addCastlingRights(CastlingRights::WhiteKingSide);
                }
                return;
            }

            case 14:
            {
                pos.place(blackRook, sq);
                if (sq == A8)
                {
                    pos.addCastlingRights(CastlingRights::BlackQueenSide);
                }
                else // (sq == H8)
                {
                    pos.addCastlingRights(CastlingRights::BlackKingSide);
                }
                return;
            }

            case 15:
            {
                pos.place(blackKing, sq);
                pos.setSideToMove(Color::Black);
                return;
            }

            }

            return;
        };

        const Bitboard occ = compressed.occupied;

        auto it = occ.begin();
        auto end = occ.end();
        for (int i = 0;; ++i)
        {
            if (it == end) break;
            decompressPiece(*it, compressed.packedState[i] & 0xF);
            ++it;

            if (it == end) break;
            decompressPiece(*it, compressed.packedState[i] >> 4);
            ++it;
        }

        return pos;
    }

    static void dump(const std::filesystem::path& pgnPath, const std::filesystem::path& outEpd, std::size_t minN)
    {
        std::vector<CompressedPosition> positions;

        pgn::LazyPgnFileReader reader(pgnPath);
        for (auto&& game : reader)
        {
            for (auto&& position : game.positions())
            {
                positions.emplace_back(compressPosition(position));
            }
        }

        std::sort(positions.begin(), positions.end());
    }

    void App::assertDatabaseOpened() const
    {
        if (m_database == nullptr)
        {
            throw InvalidCommand("No database opened.");
        }
    }

    App::App()
    {
    }

    void App::run()
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

    void App::help(const Args& args) const
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

    void App::bench(const Args& args)
    {
        std::vector<std::filesystem::path> paths;
        for (auto&& path : args) paths.emplace_back(path);
        console_app::bench(paths);
    }

    void App::open(const Args& args)
    {
        if (args.size() != 1)
        {
            invalidArguments();
        }

        m_database = console_app::open(args[0]);
    }

    void App::query(const Args& args)
    {
        assertDatabaseOpened();

        if (args.size() > 2 || args.size() < 1)
        {
            invalidArguments();
        }

        if (args.size() == 2 && args[0] != "json")
        {
            invalidArguments();
        }

        std::optional<Position> position = Position::tryFromFen(args.back());
        if (!position.has_value())
        {
            throw InvalidCommand("Invalid fen.");
        }

        console_app::query(*m_database, args.back(), args.size() == 2);
    }

    void App::info(const Args& args)
    {
        assertDatabaseOpened();

        console_app::info(*m_database, std::cout);
    }

    void App::merge(const Args& args)
    {
        assertDatabaseOpened();

        if (args.size() > 1)
        {
            invalidArguments();
        }

        if (args.size() == 1)
        {
            console_app::merge(*m_database, args[0]);
        }
        else if (args.size() == 0)
        {
            console_app::merge(*m_database);
        }
    }

    void App::verify(const Args& args)
    {
        if (args.size() != 1)
        {
            invalidArguments();
        }

        verifyPgn(args[0]);
    }

    void App::close(const Args& args)
    {
        m_database.reset();
    }

    void App::create(const Args& args)
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
            console_app::create(destination, pgns, temp);
        }
        else if (args.size() == 2)
        {
            console_app::create(destination, pgns);
        }
    }

    void App::destroy(const Args& args)
    {
        assertDatabaseOpened();

        console_app::destroy(std::move(m_database));
    }

    void App::dump(const Args& args)
    {
        if (args.size() < 3)
        {
            invalidArguments();
        }

        const std::filesystem::path pgnPath = args[0];
        const std::filesystem::path outPath = args[1];
        const std::size_t minN = std::stoll(args[2]);

        console_app::dump(pgnPath, outPath, minN);
    }

    const std::map<std::string_view, App::CommandFunction> App::m_commands = {
        { "bench"sv, &App::bench },
        { "open"sv, &App::open },
        { "query"sv, &App::query },
        { "help"sv, &App::help },
        { "info"sv, &App::info },
        { "close"sv, &App::close },
        { "merge"sv, &App::merge },
        { "verify"sv, &App::verify },
        { "create"sv, &App::create },
        { "destroy"sv, &App::destroy },
        { "dump"sv, &App::dump }
    };
}
