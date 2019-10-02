#define _CRT_SECURE_NO_WARNINGS

#include <iostream>
#include <iomanip>
#include <filesystem>
#include <cstdio>
#include <memory>
#include <unordered_map>
#include <chrono>
#include <fstream>
#include <map>
#include <execution>
#include <atomic>

#include "LocalStorageFormat.h"

#include "Bitboard.h"
#include "Enum.h"
#include "EnumMap.h"
#include "Intrinsics.h"
#include "Pgn.h"
#include "Position.h"
#include "PositionSignature.h"
#include "San.h"
#include "MoveGenerator.h"
#include "MoveGeneratorTest.h"
#include "External.h"
#include "Configuration.h"

#include "lib/xxhash/xxhash_cpp.h"

#include "lib/robin_hood/robin_hood.h"

#include "CodingTest.h"

const std::size_t pgnMemory = cfg::g_config["app"]["pgn_import_memory"].get<MemoryAmount>();

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

auto hash(const Position& pos)
{
    return xxhash::XXH3_128bits(pos.piecesRaw(), 64);
}

void print(xxhash::XXH128_hash_t h)
{
    std::cout << std::hex << h.high64 << std::hex << h.low64;
}

void header(std::ostream& out)
{
    out.write("PGCOPY\n\377\r\n\0", 11);
    out.write("\0\0\0\0", 4);
    out.write("\0\0\0\0", 4);
}

std::uint64_t bswap(std::uint64_t v)
{
    return _byteswap_uint64(v);
}

/*
void dump(const PositionSignature& s, const std::array<std::uint64_t, 3>& wdl, std::ostream& out)
{
    char data[] =
        "\x00\x04"
        "\x00\x00\x00\x10"
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        ;

    const std::uint64_t id[2] = { bswap(s.hash().high64), bswap(s.hash().low64) };
    const std::uint64_t w = bswap(wdl[0]);
    const std::uint64_t d = bswap(wdl[1]);
    const std::uint64_t l = bswap(wdl[2]);

    std::memcpy(data + 2 + 4, &id, 16);
    std::memcpy(data + 2 + 4 + 16 + 4 + (8 + 4) * 0, &w, 8);
    std::memcpy(data + 2 + 4 + 16 + 4 + (8 + 4) * 1, &d, 8);
    std::memcpy(data + 2 + 4 + 16 + 4 + (8 + 4) * 2, &l, 8);

    out.write(data, sizeof(data) - 1);

    char data[] =
        "\x00\x05"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        "\x00\x00\x00\x08"
        "\x00\x00\x00\x00\x00\x00\x00\x00"
        ;

    const std::uint64_t id[2] = { bswap(s.hash().high64), bswap(s.hash().low64) };
    const std::uint64_t w = bswap(wdl[0]);
    const std::uint64_t d = bswap(wdl[1]);
    const std::uint64_t l = bswap(wdl[2]);

    std::memcpy(data + 2 + 4 + (8 + 4) * 0, &id[0], 8);
    std::memcpy(data + 2 + 4 + (8 + 4) * 1, &id[1], 8);
    std::memcpy(data + 2 + 4 + (8 + 4) * 2, &w, 8);
    std::memcpy(data + 2 + 4 + (8 + 4) * 3, &d, 8);
    std::memcpy(data + 2 + 4 + (8 + 4) * 4, &l, 8);

    out.write(data, sizeof(data) - 1);
}
    */

void trailer(std::ostream& out)
{
    out.write("\xFF\xFF", 2);
}
std::size_t dumpPositions(const std::vector<std::filesystem::path>& froms, const std::filesystem::path& to, std::size_t reserve, bool unique)
{
    std::vector<PositionSignature> positions;
    positions.reserve(reserve / sizeof(PositionSignature));

    int numPositions = 0;
    for (auto& from : froms)
    {
        pgn::LazyPgnFileReader fr(from);
        if (!fr.isOpen())
        {
            std::cout << "Failed to open file " << from << '\n';
            break;
        }

        for (auto& game : fr)
        {
            for (auto& pos : game.positions())
            {
                positions.emplace_back(pos);
                ++numPositions;
            }
        }
    }

    std::sort(std::execution::par_unseq, positions.begin(), positions.end());

    if (unique) positions.erase(std::unique(positions.begin(), positions.end()), positions.end());

    std::string toStr = to.string();
    FILE* file = std::fopen(toStr.c_str(), "wb");
    std::fwrite(positions.data(), sizeof(PositionSignature), positions.size(), file);
    std::fclose(file);

    return numPositions;
}

std::size_t dumpPositions(const std::filesystem::path& from, const std::filesystem::path& to, std::size_t reserve, bool unique)
{
    pgn::LazyPgnFileReader fr(from);
    if (!fr.isOpen())
    {
        std::cout << "Failed to open file.\n";
        return 0;
    }
    std::vector<PositionSignature> positions;
    positions.reserve(reserve / sizeof(PositionSignature));

    int numPositions = 0;
    for (auto& game : fr)
    {
        for (auto& pos : game.positions())
        {
            positions.emplace_back(pos);
            ++numPositions;
        }
    }

    std::sort(std::execution::par_unseq, positions.begin(), positions.end());

    if (unique) positions.erase(std::unique(positions.begin(), positions.end()), positions.end());

    std::string toStr = to.string();
    FILE* file = std::fopen(toStr.c_str(), "wb");
    std::fwrite(positions.data(), sizeof(PositionSignature), positions.size(), file);
    std::fclose(file);

    return numPositions;
}

void build()
{
    //persistence::local::Database e("w:/catobase/.tmp", 4ull * 1024ull * 1024ull);
    persistence::local::Database e("c:/dev/chess_pos_db/.tmp", 4ull * 1024ull * 1024ull);
    /*
    e.importPgns(std::execution::par_unseq, {
        {"w:/catobase/data/lichess_db_standard_rated_2013-01.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-02.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-03.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-04.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-05.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-06.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-07.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-08.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-09.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-10.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-11.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-12.pgn", GameLevel::Human}
        }, pgnMemory);
        */
    //e.importPgns(std::execution::par_unseq, {
    e.importPgns(std::execution::seq, {
        {"w:/catobase/data/lichess_db_standard_rated_2013-01.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-02.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-03.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-04.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-05.pgn", GameLevel::Engine},
        {"w:/catobase/data/lichess_db_standard_rated_2013-06.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-07.pgn", GameLevel::Engine},
        {"w:/catobase/data/lichess_db_standard_rated_2013-08.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-09.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-10.pgn", GameLevel::Engine},
        {"w:/catobase/data/lichess_db_standard_rated_2013-11.pgn", GameLevel::Human},
        {"w:/catobase/data/lichess_db_standard_rated_2013-12.pgn", GameLevel::Engine}
        }, pgnMemory);
}

void buildccrl()
{
    persistence::local::Database e("c:/dev/chess_pos_db/.tmpccrl", 4ull * 1024ull * 1024ull);
    e.importPgns(std::execution::seq, {
        {"w:/catobase/data/CCRL-4040.[1086555].pgn", GameLevel::Human}
        }, pgnMemory);
}

void buildsmall()
{
    //persistence::local::Database e("w:/catobase/.tmp2_indexed", 4ull * 1024ull * 1024ull);
    persistence::local::Database e("c:/dev/chess_pos_db/.tmp", 4ull * 1024ull * 1024ull);
    e.importPgns(std::execution::seq, {
        {"w:/catobase/data/Server Games LiChess 2019-1.pgn", GameLevel::Human}
        }, pgnMemory);
}

void mergeAll()
{
    std::cout << "Loading db\n";
    //persistence::local::Database e("w:/catobase/.tmp_indexed", 4ull * 1024ull * 1024ull);
    persistence::local::Database e("c:/dev/chess_pos_db/.tmp", 4ull * 1024ull * 1024ull);
    std::cout << "Loaded db\n";

    e.mergeAll();
}

void buildbig()
{
    persistence::local::Database e("w:/catobase/.tmp_big", 4ull * 1024ull * 1024ull);

    e.importPgns(std::execution::seq, {
        {"w:/catobase/data/lichess_db_standard_rated_2013-01.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-02.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-03.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-04.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-05.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-06.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-07.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-08.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-09.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-10.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-11.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2013-12.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-01.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-02.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-03.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-04.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-05.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-06.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-07.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-08.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-09.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-10.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-11.pgn", GameLevel::Server},
        {"w:/catobase/data/lichess_db_standard_rated_2014-12.pgn", GameLevel::Server}
        }, pgnMemory);

    //e.mergeAll();
}

void replicateMergeAll()
{
    std::cout << "Loading db\n";
    persistence::local::Database e("w:/catobase/.tmp_big", 4ull * 1024ull * 1024ull);
    std::cout << "Loaded db\n";

    e.replicateMergeAll("c:/dev/chess_pos_db/.tmp");
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

struct App
{
    App()
    {
    }

    void bench(const std::vector<std::filesystem::path>& paths) const
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

    void load(const std::filesystem::path& path)
    {
        m_database = std::make_unique<persistence::local::Database>(path);
    }

    void query(const Position& pos)
    {
        if (m_database == nullptr)
        {
            std::cout << "You have to open a database first.\n";
            return;
        }

        auto agg = queryAggregate(*m_database, pos, true, true, true, false);

        printAggregatedResult(agg.main);
        std::cout << "\n";
        for (auto&& [move, res] : agg.continuations)
        {
            std::cout << std::setw(8) << san::moveToSan<san::SanSpec::Capture | san::SanSpec::Check>(pos, move) << " ";
            printAggregatedResult(res);
        }
    }

    void run()
    {
        for (;;)
        {
            std::string cmdline;
            std::getline(std::cin, cmdline);
            auto [cmd, args] = parseCommand(cmdline);

            if (cmd == "bench"sv)
            {
                std::vector<std::filesystem::path> paths;
                for (auto&& path : args) paths.emplace_back(path);
                bench(paths);
            }
            else if (cmd == "open"sv)
            {
                if (args.size() != 1)
                {
                    std::cout << "open takes one path as an argument.\n";
                }
                else
                {
                    load(args.front());
                }
            }
            else if (cmd == "query"sv)
            {
                if (args.size() != 1)
                {
                    std::cout << "query takes a fen as an argument.\n";
                }
                else
                {
                    query(Position::fromFen(args.front().c_str()));
                }
            }
            else if (cmd == "close"sv)
            {
                m_database.reset();
            }
            else if (cmd == "exit"sv)
            {
                return;
            }
            else
            {
                std::cout << "Unknown command: " << cmd << '\n';
            }
        }
    }

private:
    std::unique_ptr<persistence::local::Database> m_database;


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

    AggregatedQueryResults queryAggregate(persistence::local::Database& db, const Position& pos, bool queryContinuations, bool fetchFirstGame, bool fetchFirstGameForContinuations, bool removeEmptyContinuations)
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
            std::vector<persistence::PackedGameHeader> headers = db.queryHeaders(gameQueries);
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
};

int main()
{
    App app;
    app.run();

    {
        //std::vector<char>(4'000'000'000ull);
    }

    //benchPgnParse();
    //benchPgnParsePar();

    //testMoveGenerator();
    //return 0;
    //build();
    //buildccrl();
    //buildsmall();
    //mergeAll();
    //query2(Position::fromFen("r1b1kb1r/1pq2ppp/p1p1pn2/8/4P3/2NB4/PPP2PPP/R1BQ1RK1 w kq - 0 9"));
    //std::cout << "\n\n\n";
    //query2(Position::startPosition());

    /*
    std::cout << static_cast<std::size_t>(cfg::g_config["ext"]["index"]["builder_buffer_size"].get<MemoryAmount>()) << '\n';
    std::cout << persistence::local::detail::indexGranularity << '\n';

    ext::detail::ThreadPool::instance("");
    */

    //buildbig();
    //replicateMergeAll();

    return 0;
    /*
    persistence::Database e("w:/catobase/.tmp");
    e.importPgns({
        "data/lichess_db_standard_rated_2013-01.pgn"
        }, GameLevel::Human, pgnMemory);
        */
    {
        auto t0 = std::chrono::high_resolution_clock::now();
        std::vector<std::filesystem::path> infiles = {
            "data/lichess_db_standard_rated_2013-01.pgn",
            "data/lichess_db_standard_rated_2013-02.pgn",
            "data/lichess_db_standard_rated_2013-03.pgn",
            "data/lichess_db_standard_rated_2013-04.pgn",
            "data/lichess_db_standard_rated_2013-05.pgn",
            "data/lichess_db_standard_rated_2013-06.pgn",
            "data/lichess_db_standard_rated_2013-07.pgn",
            "data/lichess_db_standard_rated_2013-08.pgn",
            "data/lichess_db_standard_rated_2013-09.pgn",
            "data/lichess_db_standard_rated_2013-10.pgn",
            "data/lichess_db_standard_rated_2013-11.pgn",
            "data/lichess_db_standard_rated_2013-12.pgn"
        };
        std::vector<std::pair<std::filesystem::path, std::filesystem::path>> files = {
            { "data/lichess_db_standard_rated_2013-01.pgn", "out/lichess_db_standard_rated_2013-01.bin" },
            { "data/lichess_db_standard_rated_2013-02.pgn", "out/lichess_db_standard_rated_2013-02.bin" },
            { "data/lichess_db_standard_rated_2013-03.pgn", "out/lichess_db_standard_rated_2013-03.bin" },
            { "data/lichess_db_standard_rated_2013-04.pgn", "out/lichess_db_standard_rated_2013-04.bin" },
            { "data/lichess_db_standard_rated_2013-05.pgn", "out/lichess_db_standard_rated_2013-05.bin" },
            { "data/lichess_db_standard_rated_2013-06.pgn", "out/lichess_db_standard_rated_2013-06.bin" },
            { "data/lichess_db_standard_rated_2013-07.pgn", "out/lichess_db_standard_rated_2013-07.bin" },
            { "data/lichess_db_standard_rated_2013-08.pgn", "out/lichess_db_standard_rated_2013-08.bin" },
            { "data/lichess_db_standard_rated_2013-09.pgn", "out/lichess_db_standard_rated_2013-09.bin" },
            { "data/lichess_db_standard_rated_2013-10.pgn", "out/lichess_db_standard_rated_2013-10.bin" },
            { "data/lichess_db_standard_rated_2013-11.pgn", "out/lichess_db_standard_rated_2013-11.bin" },
            { "data/lichess_db_standard_rated_2013-12.pgn", "out/lichess_db_standard_rated_2013-12.bin" }
        };

        // 81.8045 225962527, 3 530 664k space
        // TODO: since we're not io bound maybe try parallelising the for loop, make sort sequential, and limit memory per batch to maxMemory/concurrency
        std::atomic<std::size_t> numPositions = 0;
        std::for_each(std::execution::seq, std::begin(files), std::end(files), [&numPositions](const std::pair<std::filesystem::path, std::filesystem::path>& paths){
            numPositions += dumpPositions(paths.first, paths.second, 1'000'000'000, false);
        });

        /*
        // or maybe we are io bound?
        // 72.3017 225962527, 2 843 451k space (0.8054 ratio, expected is 0.8138)
        auto numPositions = dumpPositions(infiles, "out/lichess_db_standard_rated_2013_unique.bin", 4'000'000'000, true);
        */

        auto t1 = std::chrono::high_resolution_clock::now();
        std::cout << (t1 - t0).count() / 1e9f << ' ' << numPositions << '\n';
        return 0;
    }

    test::runCodingTests();

    //pgn::LazyPgnFileReader fr("data/philidor.pgn"); //6 505 484
    //pgn::LazyPgnFileReader fr("data/problem2.pgn");
    pgn::LazyPgnFileReader fr("data/lichess_db_standard_rated_2013-01.pgn"); // 121332 8242561 7019204
    if (!fr.isOpen())
    {
        std::cout << "Failed to open file.\n";
        return 1;
    }
    auto t0 = std::chrono::high_resolution_clock::now();
    robin_hood::unordered_node_map<PositionSignature, std::array<std::uint64_t, 3>> hist;
    //std::map<PositionSignature, std::array<std::uint64_t, 3>> hist;
    hist.reserve(10'000'000);
    int numGames = 0;
    int numPositions = 0;
    for (auto& game : fr)
    {
        ++numGames;
        //std::cout << game.tagSection() << '\n';
        for (auto& pos : game.positions())
        {
            /*
            print(hash(pos));
            std::cout << '\n';

            pos.print(std::cout);
            std::cout << '\n';
            */

            ++hist[PositionSignature(pos)][0];
            ++numPositions;
        }
        if (numGames % 1000 == 0)
        {
            std::cout << numPositions << '\t' << hist.size() << '\n';
        }
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << (t1-t0).count() / 1e9f << ' ' << numGames << ' ' << numPositions << ' ' << hist.size() << '\n';

    /*
    std::ofstream out("out/100000_2_ordered_0.bin", std::ios::out | std::ios::binary);
    header(out);
    int i = 0;
    for (const auto& [key, value] : hist)
    {
        if (i % 2 == 0)
        {
            dump(key, value, out);
        }

        ++i;
        if (i >= 100'000) break;
    }
    trailer(out);
    
    std::ofstream out2("out/100000_2_ordered_1.bin", std::ios::out | std::ios::binary);
    header(out2);
    i = 0;
    for (const auto& [key, value] : hist)
    {
        if (i % 2 == 1)
        {
            dump(key, value, out2);
        }

        ++i;
        if (i >= 100'000) break;
    }
    trailer(out2);
    */
    return 0;
}
