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
#include <queue>
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

    static std::uint32_t receiveLength(const char* str)
    {
        constexpr std::uint32_t xorValue = 3173045653u;

        std::uint32_t size = 0;
        std::uint32_t xoredSize = 0;

        size += static_cast<std::uint32_t>(static_cast<unsigned char>(str[3])); size *= 256;
        size += static_cast<std::uint32_t>(static_cast<unsigned char>(str[2])); size *= 256;
        size += static_cast<std::uint32_t>(static_cast<unsigned char>(str[1])); size *= 256;
        size += static_cast<std::uint32_t>(static_cast<unsigned char>(str[0]));

        xoredSize += static_cast<std::uint32_t>(static_cast<unsigned char>(str[7])); xoredSize *= 256;
        xoredSize += static_cast<std::uint32_t>(static_cast<unsigned char>(str[6])); xoredSize *= 256;
        xoredSize += static_cast<std::uint32_t>(static_cast<unsigned char>(str[5])); xoredSize *= 256;
        xoredSize += static_cast<std::uint32_t>(static_cast<unsigned char>(str[4]));
        xoredSize ^= xorValue;

        if (size != xoredSize)
        {
            return 0;
        }
        return size;
    }

    // 4 bytes of size S in little endian
    // 4 bytes of size S xored with 3173045653u (for verification)
    // then S bytes
    static void sendMessage(
        const TcpConnection::Ptr& session,
        std::string message
    )
    {
        constexpr std::uint32_t xorValue = 3173045653u;

        std::uint32_t size = static_cast<std::uint32_t>(message.size());
        std::uint32_t xoredSize = size ^ xorValue;

        std::string sizeStr;
        sizeStr += static_cast<char>(size % 256); size /= 256;
        sizeStr += static_cast<char>(size % 256); size /= 256;
        sizeStr += static_cast<char>(size % 256); size /= 256;
        sizeStr += static_cast<char>(size);

        sizeStr += static_cast<char>(xoredSize % 256); xoredSize /= 256;
        sizeStr += static_cast<char>(xoredSize % 256); xoredSize /= 256;
        sizeStr += static_cast<char>(xoredSize % 256); xoredSize /= 256;
        sizeStr += static_cast<char>(xoredSize);
        session->send(sizeStr.c_str(), sizeStr.size());
        session->send(message.c_str(), message.size());
    }

    static void handleTcpRequest(
        persistence::Database& db,
        const TcpConnection::Ptr& session,
        const char* data,
        std::size_t len
    )
    {
        auto datastr = std::string(data, len);
        Logger::instance().logInfo("Received data: ", datastr);

        try
        {
            auto json = nlohmann::json::parse(datastr);
            query::Request request = json;
            if (request.isValid())
            {
                auto response = nlohmann::json(db.executeQuery(request)).dump();
                sendMessage(session, response);
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
        sendMessage(session, std::move(errorJson));
    }

    static void tcpImpl(const std::filesystem::path& path, std::uint16_t port)
    {
        struct Operation
        {
            TcpConnection::Ptr session;
            std::string data;
        };

        // TODO: Make it so only one connection is allowed.
        //       Or better, have one db per session

        std::queue<Operation> operations;
        std::condition_variable anyOperations;
        std::mutex mutex;

        auto db = loadDatabase(path);

        auto server = TcpService::Create();
        auto listenThread = ListenThread::Create(false, "127.0.0.1", port, [&](TcpSocket::Ptr socket) {
            socket->setNodelay();

            auto enterCallback = [&mutex, &anyOperations, &operations, &db](const TcpConnection::Ptr& session) {
                Logger::instance().logInfo("TCP connection from ", session->getIP());

                session->setDataCallback(
                    [&mutex, &anyOperations, &operations, &db, session, length = std::size_t(0), message = std::string("")]
                    (const char* buffer, size_t len) mutable {
                        constexpr std::uint32_t maxLength = 4 * 1024 * 1024;

                        if (length == 0)
                        {
                            if (len < 8)
                            {
                                sendMessage(session, "{\"error\":\"Message length was not received in one packet.\"");
                                return len;
                            }

                            length = receiveLength(buffer);
                            if (length > maxLength)
                            {
                                sendMessage(session, "{\"error\":\"Message is too long.\"");
                                return len;
                            }

                            message = "";

                            return std::size_t(8);
                        }
                        else
                        {
                            const std::size_t toRead = std::min(len, length);
                            message.append(buffer, toRead);
                            length -= toRead;

                            if (length == 0)
                            {
                                std::unique_lock lock(mutex);
                                operations.push(Operation{ session, message });
                                anyOperations.notify_one();
                            }

                            return toRead;
                        }
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

        std::thread workerThread([&]() {
            for (;;)
            {
                std::unique_lock lock(mutex);
                anyOperations.wait(lock, [&operations]() {return !operations.empty(); });

                auto operation = std::move(operations.front());
                operations.pop();

                lock.unlock();

                handleTcpRequest(*db, operation.session, operation.data.c_str(), operation.data.size());
            }
        });

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

    static void sendProgressFinished(const TcpConnection::Ptr& session, std::string operation, nlohmann::json additionalData = nlohmann::json::object())
    {
        nlohmann::json finishedResponse = nlohmann::json{
            { "overall_progress", 1.0f },
            { "finished", true },
            { "operation", operation }
        };
        finishedResponse.merge_patch(additionalData);
        std::string finisedResponseStr = finishedResponse.dump();

        sendMessage(session, std::move(finisedResponseStr));
    }

    static nlohmann::json statsToJson(persistence::ImportStats stats)
    {
        return nlohmann::json{
            { "num_games", stats.totalNumGames() },
            { "num_positions", stats.totalNumPositions() },
            { "num_skipped_games", stats.totalNumSkippedGames() }
        };
    }

    static auto makeImportProgressReportHandler(const TcpConnection::Ptr& session, bool doReportProgress = true)
    {
        return [session, doReportProgress](const persistence::Database::ImportProgressReport& report) {
            if (!doReportProgress) return;

            auto reportJson = nlohmann::json{
                { "operation", "import" },
                { "overall_progress", report.ratio() },
                { "finished", false }
            };
            if (report.importedPgnPath.has_value())
            {
                reportJson["imported_file_path"] = report.importedPgnPath->string();
            }

            auto reportStr = reportJson.dump();

            sendMessage(session, std::move(reportStr));
        };
    }

    static auto makeMergeProgressReportHandler(const TcpConnection::Ptr& session, bool doReportProgress = true)
    {
        return [session, doReportProgress](const persistence::Database::MergeProgressReport& report) {
            if (!doReportProgress) return;

            auto reportJson = nlohmann::json{
                { "operation", "merge" },
                { "overall_progress", report.ratio() },
                { "finished", false }
            };

            auto reportStr = reportJson.dump();
            sendMessage(session, reportStr);
        };
    }

    static void handleTcpCommandCreateImpl(
        std::unique_ptr<persistence::Database>&,
        const TcpConnection::Ptr& session,
        const std::string& key,
        const std::filesystem::path& destination,
        const persistence::ImportablePgnFiles& pgns,
        const std::filesystem::path& temp,
        bool doMerge,
        bool doReportProgress
    )
    {
        assertDirectoryEmpty(destination);
        assertDirectoryEmpty(temp);

        if (doMerge)
        {
            {
                auto db = instantiateDatabase(key, temp);

                {
                    auto callback = makeImportProgressReportHandler(session, doReportProgress);
                    auto stats = db->import(pgns, importMemory, callback);
                    sendProgressFinished(session, "import", statsToJson(stats));
                }

                {
                    auto callback = makeMergeProgressReportHandler(session, doReportProgress);
                    db->replicateMergeAll(destination, callback);
                }
            }

            std::filesystem::remove_all(temp);
            std::filesystem::create_directory(temp);
        }
        else
        {
            auto db = instantiateDatabase(key, destination);

            auto callback = makeImportProgressReportHandler(session, doReportProgress);
            auto stats = db->import(pgns, importMemory, callback);
            sendProgressFinished(session, "import", statsToJson(stats));
        }

        // We have to always sent some info that we finished
        sendProgressFinished(session, "create");
    }

    static void handleTcpCommandCreateImpl(
        std::unique_ptr<persistence::Database>&,
        const TcpConnection::Ptr& session,
        const std::string& key,
        const std::filesystem::path& destination,
        const persistence::ImportablePgnFiles& pgns,
        bool doMerge,
        bool doReportProgress
    )
    {
        assertDirectoryEmpty(destination);

        {
            auto db = instantiateDatabase(key, destination);

            auto callback = makeImportProgressReportHandler(session, doReportProgress);
            auto stats = db->import(pgns, importMemory, callback);
            sendProgressFinished(session, "import", statsToJson(stats));

            if (doMerge)
            {
                auto callback = makeMergeProgressReportHandler(session, doReportProgress);
                db->mergeAll(callback);
            }
        }

        // We have to always sent some info that we finished
        sendProgressFinished(session, "create");
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

        auto callback = makeMergeProgressReportHandler(session, doReportProgress);
        db->replicateMergeAll(destination, callback);

        // We have to always sent some info that we finished
        sendProgressFinished(session, "merge");
    }

    static void handleTcpCommandMergeImpl(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        bool doReportProgress
    )
    {
        assertDatabaseOpen(db);

        auto callback = makeMergeProgressReportHandler(session, doReportProgress);
        db->mergeAll(callback);

        // We have to always sent some info that we finished
        sendProgressFinished(session, "merge");
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

        sendProgressFinished(session, "open");
    }

    static void handleTcpCommandClose(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        db.reset();

        sendProgressFinished(session, "close");
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

        sendMessage(session, responseStr);
    }

    static void handleTcpCommandStats(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        assertDatabaseOpen(db);

        auto stats = db->stats();

        auto response = nlohmann::json{
            { "human", nlohmann::json{
                { "num_games", stats.statsByLevel[GameLevel::Human].numGames },
                { "num_positions", stats.statsByLevel[GameLevel::Human].numPositions }
            }},
            { "engine", nlohmann::json{
                { "num_games", stats.statsByLevel[GameLevel::Engine].numGames },
                { "num_positions", stats.statsByLevel[GameLevel::Engine].numPositions }
            }},
            { "server", nlohmann::json{
                { "num_games", stats.statsByLevel[GameLevel::Server].numGames },
                { "num_positions", stats.statsByLevel[GameLevel::Server].numPositions }
            }},
        };

        auto responseStr = nlohmann::json(response).dump();
        sendMessage(session, responseStr);
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
            { "query", handleTcpCommandQuery },
            { "stats", handleTcpCommandStats }
        };

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
        sendMessage(session, errorJson);

        return false;
    }

    static void tcpImpl(std::uint16_t port)
    {
        struct Operation
        {
            TcpConnection::Ptr session;
            std::string data;
        };

        // TODO: Make it so only one connection is allowed.
        //       Or better, have one db per session

        std::unique_ptr<persistence::Database> db = nullptr;

        std::queue<Operation> operations;
        std::condition_variable anyOperations;
        std::mutex mutex;

        auto server = TcpService::Create();
        auto listenThread = ListenThread::Create(false, "127.0.0.1", port, [&](TcpSocket::Ptr socket) {
            socket->setNodelay();

            auto enterCallback = [&mutex, &anyOperations, &operations, &db](const TcpConnection::Ptr& session) {
                Logger::instance().logInfo("TCP connection from ", session->getIP());

                session->setDataCallback(
                    [&mutex, &anyOperations, &operations, &db, session, length = std::size_t(0), message = std::string("")]
                    (const char* buffer, size_t len) mutable {
                        constexpr std::uint32_t maxLength = 4 * 1024 * 1024;

                        if (length == 0)
                        {
                            if (len < 8)
                            {
                                sendMessage(session, "{\"error\":\"Message length was not received in one packet.\"");
                                return len;
                            }

                            length = receiveLength(buffer);
                            if (length > maxLength)
                            {
                                sendMessage(session, "{\"error\":\"Message is too long.\"");
                                return len;
                            }

                            message = "";

                            return std::size_t(8);
                        }
                        else
                        {
                            const std::size_t toRead = std::min(len, length);
                            message.append(buffer, toRead);
                            length -= toRead;

                            if (length == 0)
                            {
                                std::unique_lock lock(mutex);
                                operations.push(Operation{ session, message });
                                anyOperations.notify_one();
                            }

                            return toRead;
                        }
                    });

                session->setDisConnectCallback([](const TcpConnection::Ptr& session) {
                    });
            };

            server->addTcpConnection(std::move(socket),
                brynet::net::TcpService::AddSocketOption::AddEnterCallback(enterCallback),
                brynet::net::TcpService::AddSocketOption::WithMaxRecvBufferSize(1024 * 1024));
            });

        listenThread->startListen();
        server->startWorkerThread(3);

        EventLoop mainloop;

        for (;;)
        {
            std::unique_lock lock(mutex);
            anyOperations.wait(lock, [&operations]() {return !operations.empty(); });

            auto operation = std::move(operations.front());
            operations.pop();

            lock.unlock();

            if (handleTcpCommand(db, operation.session, operation.data.c_str(), operation.data.size()))
            {
                break;
            }
        }
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
