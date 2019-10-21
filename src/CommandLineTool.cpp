#include "CommandLineTool.h"


#include "chess/GameClassification.h"

// IMPORTANT: If brynet is included AFTER nlohmann::json then linker requires
//            __imp_MapViewOfFileNuma2 to be present which breaks the build
//            (at least for windows <10)
//            Very peculiar behaviour. No cause nor solution found.
//            Cannot be reproduced in a clean project.
#define NOMINMAX
#include <brynet/net/SocketLibFunction.h>
#include <brynet/net/EventLoop.h>
#include <brynet/net/TCPService.h>
#include <brynet/net/ListenThread.h>
#include <brynet/net/Connector.h>
#include <brynet/net/Socket.h>

#include "chess/GameClassification.h"

#include "data_structure/EnumMap.h"

#include "persistence/pos_db/alpha/DatabaseFormatAlpha.h"
#include "persistence/pos_db/beta/DatabaseFormatBeta.h"
#include "persistence/pos_db/Database.h"
#include "persistence/pos_db/DatabaseFactory.h"
#include "persistence/pos_db/Query.h"

#include "util/MemoryAmount.h"

#include "Configuration.h"
#include "Logger.h"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "json/json.hpp"

namespace command_line_app
{
    using namespace brynet::net;

    using Args = std::vector<std::string>;
    using CommandHandler = void(*)(const Args&);
    using TcpCommandHandler = void(*)(
        std::unique_ptr<persistence::Database>&,
        const TcpConnection::Ptr&,
        const nlohmann::json&);

    const std::size_t importMemory = cfg::g_config["console_app"]["pgn_import_memory"].get<MemoryAmount>();

    static void assertDirectoryNotEmpty(const std::filesystem::path& path)
    {
        if (!std::filesystem::exists(path) || std::filesystem::is_empty(path))
        {
            throw Exception("Directory " + path.string() + " doesn't exist or is empty");
        }
    }

    static void assertDirectoryEmpty(const std::filesystem::path& path)
    {
        if (std::filesystem::exists(path) && !std::filesystem::is_empty(path))
        {
            throw Exception("Directory " + path.string() + " is not empty");
        }
    }

    static void assertFileExists(const std::filesystem::path& path)
    {
        if (!std::filesystem::exists(path) || !std::filesystem::is_regular_file(path))
        {
            throw Exception("File " + path.string() + " does not exist.");
        }
    }

    static void assertDatabaseOpen(const std::unique_ptr<persistence::Database>& db)
    {
        if (db == nullptr)
        {
            throw Exception("No database open.");
        }
    }

    static void assertNoDatabaseOpen(const std::unique_ptr<persistence::Database>& db)
    {
        if (db != nullptr)
        {
            throw Exception("Database already open.");
        }
    }

    static void throwInvalidCommand(const std::string& command)
    {
        throw Exception("Invalid command: " + command);
    }

    static void throwInvalidArguments()
    {
        throw Exception("Invalid arguments. See help.");
    }

    static const persistence::DatabaseFactory g_factory = []() {
        persistence::DatabaseFactory g_factory;

        g_factory.registerDatabaseType<persistence::db_alpha::Database>();
        g_factory.registerDatabaseType<persistence::db_beta::Database>();

        return g_factory;
    }();

    static auto instantiateDatabase(const std::string& key, const std::filesystem::path& destination)
    {
        auto ptr = g_factory.tryInstantiateByKey(key, destination);
        if (ptr == nullptr)
        {
            throw Exception("Invalid database type.");
        }
        return ptr;
    }

    static auto readKeyOfDatabase(const std::filesystem::path& path)
    {
        auto key = persistence::Database::tryReadKey(path);
        if (!key.has_value())
        {
            throw Exception("Directory " + path.string() + " does not contain a valid database.");
        }
        return *key;
    }

    static auto loadDatabase(const std::filesystem::path& path)
    {
        const auto key = readKeyOfDatabase(path);
        return instantiateDatabase(key, path);
    }

    [[nodiscard]] static Args convertCommandLineArguments(int argc, char* argv[])
    {
        std::vector<std::string> args;
        args.reserve(argc);
        for (int i = 0; i < argc; ++i)
        {
            args.emplace_back(argv[i]);
        }

        return args;
    }

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
                throw Exception("Invalid level: " + levelStr);
            }

            std::getline(ss, pgnPath, ';');
            pgns.emplace_back(pgnPath, *levelOpt);
        }

        return pgns;
    }

    static void help(const Args&)
    {
        std::cout << "create <type> <destination> <pgn_files> [<temp>]\n";
        std::cout << "merge <path> [<destination>]\n";
    }

    static void createImpl(
        const std::string& key,
        const std::filesystem::path& destination,
        const persistence::ImportablePgnFiles& pgns
    )
    {
        assertDirectoryEmpty(destination);

        auto db = instantiateDatabase(key, destination);
        db->import(pgns, importMemory);
    }

    static void createImpl(
        const std::string& key,
        const std::filesystem::path& destination,
        const persistence::ImportablePgnFiles& pgns,
        const std::filesystem::path& temp
    )
    {
        assertDirectoryEmpty(destination);
        assertDirectoryEmpty(temp);

        {
            auto db = instantiateDatabase(key, temp);
            db->import(pgns, importMemory);
            db->replicateMergeAll(destination);
        }

        std::filesystem::remove_all(temp);
    }

    static void create(const Args& args)
    {
        if (args.size() == 4)
        {
            createImpl(args[1], args[2], parsePgnListFile(args[3]));
        }
        else if (args.size() == 5)
        {
            createImpl(args[1], args[2], parsePgnListFile(args[3]), args[4]);
        }
        else
        {
            throwInvalidArguments();
        }
    }

    static void mergeImpl(const std::filesystem::path& path)
    {
        auto db = loadDatabase(path);
        db->mergeAll();
    }

    static void mergeImpl(const std::filesystem::path& fromPath, const std::filesystem::path& toPath)
    {
        assertDirectoryEmpty(toPath);

        auto db = loadDatabase(fromPath);
        db->replicateMergeAll(toPath);
    }

    static void merge(const Args& args)
    {
        if (args.size() == 2)
        {
            mergeImpl(args[1]);
        }
        else if (args.size() == 3)
        {
            mergeImpl(args[1], args[2]);
        }
    }

    static void handleTcpRequest(
        persistence::Database& db,
        const TcpConnection::Ptr& session,
        const char* data,
        std::size_t len
    )
    {
        std::cout << len << ' ' << std::strlen(data) << '\n';
        auto datastr = std::string(data, len);
        Logger::instance().logInfo("Received data: ", datastr);

        try
        {
            auto json = nlohmann::json::parse(datastr);
            query::Request request = json;
            if (request.isValid())
            {
                auto response = nlohmann::json(db.executeQuery(request)).dump();
                session->send(response.c_str(), response.size());
                Logger::instance().logInfo("Handled valid request. Response size: ", response.size());
                return;
            }
        }
        catch (...)
        {
            Logger::instance().logInfo("Error parsing request");
        }

        Logger::instance().logInfo("Invalid request");

        auto errorJson = nlohmann::json::object({ {"error", "InvalidRequest" } }).dump();
        auto packet = TcpConnection::makePacket(errorJson.c_str(), errorJson.size());
        session->send(packet);
    }

    static void tcpImpl(const std::filesystem::path& path, std::uint16_t port)
    {
        auto db = loadDatabase(path);

        auto server = TcpService::Create();
        auto listenThread = ListenThread::Create(false, "127.0.0.1", port, [&](TcpSocket::Ptr socket) {
            socket->setNodelay();

            auto enterCallback = [&db](const TcpConnection::Ptr& session) {
                Logger::instance().logInfo("TCP connection from ", session->getIP());

                session->setDataCallback([&db, session](const char* buffer, size_t len) {
                    handleTcpRequest(*db, session, buffer, len);
                    return len;
                });

                session->setDisConnectCallback([](const TcpConnection::Ptr& session) {
                });
            };

            server->addTcpConnection(std::move(socket),
                brynet::net::TcpService::AddSocketOption::AddEnterCallback(enterCallback),
                brynet::net::TcpService::AddSocketOption::WithMaxRecvBufferSize(1024 * 1024));
        });

        listenThread->startListen();
        server->startWorkerThread(1);

        EventLoop mainloop;

        /*
        auto client = TcpService::Create();
        client->startWorkerThread(1);

        auto connector = AsyncConnector::Create();
        connector->startWorkerThread();

        auto enterCallback = [client](TcpSocket::Ptr socket) {
            socket->setNodelay();

            auto enterCallback = [](const TcpConnection::Ptr& session) {
                session->setDataCallback([session](const char* buffer, size_t len) {
                    std::cerr << std::string(buffer, len) << '\n';
                    return len;
                    });

                query::Request query;
                query.token = "toktok";
                query.positions = { { "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1", std::nullopt } };
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

                auto queryjson = nlohmann::json(query).dump();
                session->send(queryjson.c_str(), queryjson.size());
            };

            client->addTcpConnection(std::move(socket),
                brynet::net::TcpService::AddSocketOption::AddEnterCallback(enterCallback),
                brynet::net::TcpService::AddSocketOption::WithMaxRecvBufferSize(1024 * 1024));
        };

        auto failedCallback = []() {
            std::cout << "connect failed" << std::endl;
        };

        connector->asyncConnect({
            AsyncConnector::ConnectOptions::WithAddr("127.0.0.1", port),
            AsyncConnector::ConnectOptions::WithTimeout(std::chrono::seconds(10)),
            AsyncConnector::ConnectOptions::WithCompletedCallback(enterCallback),
            AsyncConnector::ConnectOptions::WithFailedCallback(failedCallback) 
        });
        */

        for (;;)
        {
            std::string line;
            std::getline(std::cin, line);
            if (line == "exit"sv)
            {
                return;
            }
        }
    }

    static void sendProgressFinished(const TcpConnection::Ptr& session)
    {
        nlohmann::json finishedResponse = nlohmann::json{
            { "overall_progress", 1.0f },
            { "finised", true }
        };
        std::string finisedResponseStr = finishedResponse.dump();
        session->send(finisedResponseStr.c_str(), finisedResponseStr.size());
    }

    static void handleTcpCommandCreateImpl(
        std::unique_ptr<persistence::Database>&,
        const TcpConnection::Ptr& session,
        const std::string& key,
        const std::filesystem::path& destination,
        const persistence::ImportablePgnFiles& pgns,
        const std::filesystem::path& temp,
        bool doMerge,
        bool toReportProgress
    )
    {
        assertDirectoryEmpty(destination);
        assertDirectoryEmpty(temp);

        if (doMerge)
        {
            auto db = instantiateDatabase(key, temp);
            // TODO: progress reporting
            db->import(pgns, importMemory);
            db->replicateMergeAll(destination);
        }
        else
        {
            auto db = instantiateDatabase(key, destination);
            // TODO: progress reporting
            db->import(pgns, importMemory);
        }

        std::filesystem::remove_all(temp);

        // We have to always sent some info that we finished
        // TODO: also send stats
        sendProgressFinished(session);
    }

    static void handleTcpCommandCreateImpl(
        std::unique_ptr<persistence::Database>&,
        const TcpConnection::Ptr& session,
        const std::string& key,
        const std::filesystem::path& destination,
        const persistence::ImportablePgnFiles& pgns,
        bool doMerge,
        bool toReportProgress
    )
    {
        assertDirectoryEmpty(destination);

        {
            auto db = instantiateDatabase(key, destination);
            // TODO: progress reporting
            db->import(pgns, importMemory);

            if (doMerge)
            {
                db->mergeAll();
            }
        }

        // We have to always sent some info that we finished
        // TODO: also send stats
        sendProgressFinished(session);
    }

    static void handleTcpCommandCreate(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        const std::string destination = json["destination_path"].get<std::string>();
        const bool doMerge = json["merge"].get<bool>();
        const bool doReportProgress = json["report_progress"].get<bool>();
        
        persistence::ImportablePgnFiles pgns;
        for (auto& v : json["human_pgns"]) pgns.emplace_back(v.get<std::string>(), GameLevel::Human);
        for (auto& v : json["engine_pgns"]) pgns.emplace_back(v.get<std::string>(), GameLevel::Engine);
        for (auto& v : json["server_pgns"]) pgns.emplace_back(v.get<std::string>(), GameLevel::Server);

        const std::string databaseFormat = json["database_format"].get<std::string>();

        if (json.contains("temporary_path"))
        {
            const std::string temp = json["temporary_path"].get<std::string>();
            handleTcpCommandCreateImpl(db, session, databaseFormat, destination, pgns, temp, doMerge, doReportProgress);
        }
        else
        {
            handleTcpCommandCreateImpl(db, session, databaseFormat, destination, pgns, doMerge, doReportProgress);
        }
    }

    static void handleTcpCommandMergeImpl(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const std::filesystem::path& destination,
        bool doReportProgress
    )
    {
        assertDirectoryEmpty(destination);
        assertDatabaseOpen(db);

        db->replicateMergeAll(destination);

        // We have to always sent some info that we finished
        sendProgressFinished(session);
    }

    static void handleTcpCommandMergeImpl(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        bool doReportProgress
    )
    {
        assertDatabaseOpen(db);

        db->mergeAll();

        // We have to always sent some info that we finished
        sendProgressFinished(session);
    }

    static void handleTcpCommandMerge(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        const bool doReportProgress = json["report_progress"].get<bool>();
        if (json.contains("destination_path"))
        {
            const std::string destination = json["destination_path"].get<std::string>();
            handleTcpCommandMergeImpl(db, session, destination, doReportProgress);
        }
        else
        {
            handleTcpCommandMergeImpl(db, session, doReportProgress);
        }
    }

    static void handleTcpCommandOpen(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        assertNoDatabaseOpen(db);

        const std::string dbPath = json["database_path"].get<std::string>();

        db = loadDatabase(dbPath);

        sendProgressFinished(session);
    }

    static void handleTcpCommandClose(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        db.reset();

        sendProgressFinished(session);
    }

    static void handleTcpCommandQuery(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        assertDatabaseOpen(db);

        query::Request request = json["query"];
        auto response = db->executeQuery(request);
        auto responseStr = nlohmann::json(response).dump();

        session->send(responseStr.c_str(), responseStr.size());
    }

    static bool handleTcpCommand(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const char* data,
        std::size_t len
    )
    {
        static std::map<std::string, TcpCommandHandler> s_handlers{
            { "create", handleTcpCommandCreate },
            { "merge", handleTcpCommandMerge },
            { "open", handleTcpCommandOpen },
            { "close", handleTcpCommandClose },
            { "query", handleTcpCommandQuery }
        };

        std::cout << len << ' ' << std::strlen(data) << '\n';
        auto datastr = std::string(data, len);
        Logger::instance().logInfo("Received data: ", datastr);

        try
        {
            nlohmann::json json = nlohmann::json::parse(datastr);

            const std::string command = json["command"].get<std::string>();
            if (command == "exit") return true;

            s_handlers.at(command)(db, session, json);

            return false;
        }
        catch (...)
        {
            Logger::instance().logInfo("Error parsing request");
        }

        Logger::instance().logInfo("Invalid request");

        auto errorJson = nlohmann::json::object({ {"error", "InvalidRequest" } }).dump();
        auto packet = TcpConnection::makePacket(errorJson.c_str(), errorJson.size());
        session->send(packet);
    }

    static void tcpImpl(std::uint16_t port)
    {
        // TODO: Make it so only one connection is allowed.
        //       Or better, have one db per session

        std::unique_ptr<persistence::Database> db = nullptr;

        std::promise<void> doExitPromise;
        std::future<void> doExit = doExitPromise.get_future();

        auto server = TcpService::Create();
        auto listenThread = ListenThread::Create(false, "127.0.0.1", port, [&](TcpSocket::Ptr socket) {
            socket->setNodelay();

            auto enterCallback = [&db, &doExitPromise](const TcpConnection::Ptr& session) {
                Logger::instance().logInfo("TCP connection from ", session->getIP());

                session->setDataCallback([&db, &doExitPromise, session](const char* buffer, size_t len) {
                    if (handleTcpCommand(db, session, buffer, len))
                    {
                        doExitPromise.set_value();
                    }
                    return len;
                    });

                session->setDisConnectCallback([](const TcpConnection::Ptr& session) {
                    });
            };

            server->addTcpConnection(std::move(socket),
                brynet::net::TcpService::AddSocketOption::AddEnterCallback(enterCallback),
                brynet::net::TcpService::AddSocketOption::WithMaxRecvBufferSize(1024 * 1024));
            });

        listenThread->startListen();
        server->startWorkerThread(1);

        EventLoop mainloop;

        doExit.get();
    }

    static void tcp(const Args& args)
    {
        if (args.size() == 3)
        {
            const int port = std::stoi(args[2]);
            if (port <= 0 || port > std::numeric_limits<std::uint64_t>::max())
            {
                throwInvalidArguments();
            }

            tcpImpl(args[1], static_cast<std::uint16_t>(port));
        }
        else if (args.size() == 2)
        {
            const int port = std::stoi(args[1]);
            if (port <= 0 || port > std::numeric_limits<std::uint64_t>::max())
            {
                throwInvalidArguments();
            }

            tcpImpl(static_cast<std::uint16_t>(port));
        }
        else
        {
            throwInvalidArguments();
        }
    }

    void runCommand(int argc, char* argv[])
    {
        static const std::map<std::string, CommandHandler> s_commandHandlers = {
            { "help", help },
            { "create", create },
            { "merge", merge },
            { "tcp", tcp }
        };

        if (argc <= 0) return;

        auto args = convertCommandLineArguments(argc, argv);

        auto handlerIt = s_commandHandlers.find(args[0]);
        if (handlerIt == s_commandHandlers.end())
        {
            throwInvalidCommand(args[0]);
        }

        handlerIt->second(args);
    }
}
