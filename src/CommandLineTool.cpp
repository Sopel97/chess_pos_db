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
#include <brynet/net/Socket.h>

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
        Logger::instance().logInfo("Received data: ", data);

        try
        {
            auto json = nlohmann::json::parse(std::string(data, len));
            query::Request request = json;
            if (request.isValid())
            {
                Logger::instance().logInfo("Handled valid request");
                return;
            }
        }
        catch (...)
        {
        }

        Logger::instance().logInfo("Invalid request");

        auto errorJson = nlohmann::json::object({ "error", "InvalidRequest" });
        //connection->send(errorJson.dump());
    }

    static void tcpImpl(const std::filesystem::path& path, std::uint16_t port)
    {
        auto db = loadDatabase(path);

        auto server = TcpService::Create();
        auto listenThread = ListenThread::Create(false, "0.0.0.0", port, [&](TcpSocket::Ptr socket) {
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
