#define _SILENCE_CXX17_C_HEADER_DEPRECATION_WARNING

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

#include "chess/Bcgn.h"
#include "chess/GameClassification.h"
#include "chess/Pgn.h"
#include "chess/San.h"

#include "enum/EnumArray.h"

#include "persistence/pos_db/beta/DatabaseFormatBeta.h"
#include "persistence/pos_db/delta/DatabaseFormatDelta.h"
#include "persistence/pos_db/delta/DatabaseFormatDeltaSmeared.h"
#include "persistence/pos_db/epsilon/DatabaseFormatEpsilon.h"
#include "persistence/pos_db/epsilon/DatabaseFormatEpsilonSmeared.h"
#include "persistence/pos_db/Database.h"
#include "persistence/pos_db/DatabaseFactory.h"
#include "persistence/pos_db/Query.h"

#include "util/MemoryAmount.h"

#include "Configuration.h"
#include "Logger.h"

#include "ConsoleApp.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <queue>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "args/args.hxx"
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

    const MemoryAmount importMemory = cfg::g_config["command_line_app"]["import_memory"].get<MemoryAmount>();
    const MemoryAmount pgnParserMemory = cfg::g_config["command_line_app"]["pgn_parser_memory"].get<MemoryAmount>();
    const MemoryAmount bcgnParserMemory = cfg::g_config["command_line_app"]["bcgn_parser_memory"].get<MemoryAmount>();

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

        g_factory.registerDatabaseSchema<persistence::db_beta::Database>();
        g_factory.registerDatabaseSchema<persistence::db_delta::Database>();
        g_factory.registerDatabaseSchema<persistence::db_delta_smeared::Database>();
        g_factory.registerDatabaseSchema<persistence::db_epsilon::Database>();
        g_factory.registerDatabaseSchema<persistence::db_epsilon_smeared::Database>();

        return g_factory;
    }();

    static auto instantiateDatabase(const std::string& schema, const std::filesystem::path& destination)
    {
        auto ptr = g_factory.tryInstantiateBySchema(schema, destination);
        if (ptr == nullptr)
        {
            throw Exception("Invalid database type.");
        }
        return ptr;
    }

    static auto readSchemaOfDatabase(const std::filesystem::path& path)
    {
        auto schema = persistence::Database::tryReadSchema(path);
        if (!schema.has_value())
        {
            throw Exception("Directory " + path.string() + " does not contain a valid database.");
        }
        return *schema;
    }

    static auto loadDatabase(const std::filesystem::path& path)
    {
        const auto schema = readSchemaOfDatabase(path);
        return instantiateDatabase(schema, path);
    }

    [[nodiscard]] static persistence::ImportableFiles getPgnListFromPaths(
        const std::vector<std::string>& human,
        const std::vector<std::string>& engine,
        const std::vector<std::string>& server
    )
    {
        persistence::ImportableFiles pgns;
        
        for (auto&& path : human)
        {
            assertFileExists(path);

            pgns.emplace_back(path, GameLevel::Human);
        }

        for (auto&& path : engine)
        {
            assertFileExists(path);

            pgns.emplace_back(path, GameLevel::Engine);
        }

        for (auto&& path : server)
        {
            assertFileExists(path);

            pgns.emplace_back(path, GameLevel::Server);
        }

        return pgns;
    }

    [[nodiscard]] static persistence::ImportableFiles parsePgnListFile(const std::filesystem::path& path)
    {
        assertFileExists(path);

        persistence::ImportableFiles pgns;

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

    static void createImpl(
        const std::string& schema,
        const std::filesystem::path& destination,
        const persistence::ImportableFiles& pgns
    )
    {
        assertDirectoryEmpty(destination);

        auto db = instantiateDatabase(schema, destination);
        db->import(pgns, importMemory.bytes());
    }

    static void create(args::Subparser& parser)
    {
        args::Group requiredArgs(parser, "required arguments", args::Group::Validators::All);
        args::ValueFlag<std::string> schema(requiredArgs, "schema", "The schema (format/type) of the database", { "schema" });
        args::ValueFlag<std::string> output(requiredArgs, "path", "The output directory", { 'o', "output" });

        args::ValueFlagList<std::string> human(parser, "path", "Human files", { "human" });
        args::ValueFlagList<std::string> engine(parser, "path", "Engine files", { "engine" });
        args::ValueFlagList<std::string> server(parser, "path", "Server files", { "server" });
        args::ValueFlag<std::string> listInput(parser, "input definition path", "The input is a file which consists of a list of paths to .pgn or .bcgn files. Each line should contain 'server'/'human'/'engine'; path", { "list" });

        parser.Parse();

        const bool anyList = listInput.Get() != "";

        const bool anySeparate =
            !args::get(human).empty()
            || !args::get(engine).empty()
            || !args::get(server).empty();

        assertDirectoryEmpty(args::get(output));

        if (anyList && anySeparate)
        {
            std::cout << "Either list or separate. Not both.\n";
            return;
        }
        else if (!anyList && !anySeparate)
        {
            instantiateDatabase(args::get(schema), args::get(output));
        }
        else
        {
            auto pgns = 
                anyList
                ? parsePgnListFile(args::get(listInput))
                : getPgnListFromPaths(args::get(human), args::get(engine), args::get(server));

            createImpl(args::get(schema), args::get(output), pgns);
        }
    }

    static void appendImpl(
        const std::filesystem::path& path,
        const persistence::ImportableFiles& pgns
    )
    {
        auto db = loadDatabase(path);

        db->import(pgns, importMemory.bytes());
    }


    static void append(args::Subparser& parser)
    {
        args::Group requiredArgs(parser, "required arguments", args::Group::Validators::All);
        args::Positional<std::string> input(requiredArgs, "path", "The path to the database for which to merge files.");

        args::ValueFlagList<std::string> human(parser, "path", "Human files", { "human" });
        args::ValueFlagList<std::string> engine(parser, "path", "Engine files", { "engine" });
        args::ValueFlagList<std::string> server(parser, "path", "Server files", { "server" });
        args::ValueFlag<std::string> listInput(parser, "input definition path", "The input is a file which consists of a list of paths to .pgn or .bcgn files. Each line should contain 'server'/'human'/'engine'; path", { "list" });

        parser.Parse();

        const bool anyList = listInput.Get() != "";

        const bool anySeparate =
            !args::get(human).empty()
            || !args::get(engine).empty()
            || !args::get(server).empty();

        if (anyList && anySeparate)
        {
            std::cout << "Either list or separate. Not both.\n";
            return;
        }
        else if (!anyList && !anySeparate)
        {
            std::cout << "Nothing to do.\n";
        }
        else
        {
            auto pgns =
                anyList
                ? parsePgnListFile(args::get(listInput))
                : getPgnListFromPaths(args::get(human), args::get(engine), args::get(server));

            appendImpl(args::get(input), pgns);
        }
    }

    static void mergeImpl(
        const std::filesystem::path& path, 
        const std::vector<std::filesystem::path>& temps,
        std::optional<MemoryAmount> maxSpace
    )
    {
        auto db = loadDatabase(path);
        db->mergeAll(temps, maxSpace);
    }

    static void merge(args::Subparser& parser)
    {
        args::Group requiredArgs(parser, "required arguments", args::Group::Validators::All);
        args::Positional<std::string> input(requiredArgs, "path", "The path to the database for which to merge files.");

        args::ValueFlag<std::string> maxSpace(parser, "memory", "The amount of space to use in temporary dirs. For example \"100GiB\"", { "memory" });
        args::ValueFlagList<std::string> temp(parser, "path", "Temporary directories to use for merging", { "temp" });

        parser.Parse();

        std::filesystem::path inputPath = args::get(input);
        std::vector<std::filesystem::path> temps;
        for (auto&& t : temp)
        {
            temps.emplace_back(t);
        }
        std::optional<MemoryAmount> maxSpaceOpt;
        if (args::get(maxSpace) != "")
        {
            maxSpaceOpt = MemoryAmount(args::get(maxSpace));
        }

        mergeImpl(
            inputPath,
            temps,
            maxSpaceOpt
        );
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

    struct MessageReceiver
    {
        std::vector<std::string> onDataReceived(const char* buffer, std::size_t len)
        {
            constexpr std::uint32_t maxLength = 4 * 1024 * 1024;

            std::vector<std::string> messages;

            while (len != 0)
            {
                if (m_length == 0)
                {
                    if (len < 8)
                    {
                        throw Exception("Length did not arrive in one packet");
                    }

                    m_length = receiveLength(buffer);
                    if (m_length > maxLength)
                    {
                        throw Exception("Message too long");
                    }

                    m_message = "";

                    len -= 8;
                    buffer += 8;
                }
                else
                {
                    const std::size_t toRead = std::min(len, m_length);
                    m_message.append(buffer, toRead);
                    m_length -= toRead;

                    if (m_length == 0)
                    {
                        messages.emplace_back(std::move(m_message));
                        m_message.clear();
                    }

                    len -= toRead;
                    buffer += len;
                }
            }

            return messages;
        }

    private:
        std::string m_message;
        std::size_t m_length;
    };

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
                auto response = nlohmann::json(db.executeQuery(request)).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
                Logger::instance().logInfo("Handled valid request. Response size: ", response.size());
                sendMessage(session, response);
                return;
            }
        }
        catch (...)
        {
            Logger::instance().logInfo("Error parsing request");
        }

        Logger::instance().logInfo("Invalid request");

        auto errorJson = nlohmann::json::object({ {"error", "InvalidRequest" } }).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
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
                    [&mutex, &anyOperations, &operations, &db, session, messageReceiver = MessageReceiver()]
                (const char* buffer, size_t len) mutable {
                    try
                    {
                        auto messages = messageReceiver.onDataReceived(buffer, len);
                        for (auto&& message : messages)
                        {
                            std::unique_lock lock(mutex);
                            operations.emplace(Operation{ session, std::move(message) });
                        }
                        if (!messages.empty())
                        {
                            anyOperations.notify_one();
                        }
                    }
                    catch (Exception& ex)
                    {
                        sendMessage(session, std::string("{\"error\":\"") + ex.what() + "\"");
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
        std::string finisedResponseStr = finishedResponse.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);

        sendMessage(session, std::move(finisedResponseStr));
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

            auto reportStr = reportJson.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);

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

            auto reportStr = reportJson.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
            sendMessage(session, reportStr);
        };
    }

    static void handleTcpCommandCreateImpl(
        std::unique_ptr<persistence::Database>&,
        const TcpConnection::Ptr& session,
        const std::string& schema,
        const std::filesystem::path& destination,
        const persistence::ImportableFiles& pgns,
        const std::vector<std::filesystem::path>& temporaryPaths,
        std::optional<MemoryAmount> temporarySpace,
        bool doMerge,
        bool doReportProgress
    )
    {
        assertDirectoryEmpty(destination);
        for (auto& temp : temporaryPaths)
        {
            assertDirectoryEmpty(temp);
        }

        {
            auto db = instantiateDatabase(schema, destination);

            auto callback = makeImportProgressReportHandler(session, doReportProgress);
            auto stats = db->import(pgns, importMemory.bytes(), callback);
            auto statsJson = nlohmann::json::object();
            statsJson["stats"] = stats;
            sendProgressFinished(session, "import", statsJson);

            if (doMerge)
            {
                auto callback = makeMergeProgressReportHandler(session, doReportProgress);
                db->mergeAll(temporaryPaths, temporarySpace, callback);
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
        
        persistence::ImportableFiles pgns;
        for (auto& v : json["human_pgns"]) pgns.emplace_back(v.get<std::string>(), GameLevel::Human);
        for (auto& v : json["engine_pgns"]) pgns.emplace_back(v.get<std::string>(), GameLevel::Engine);
        for (auto& v : json["server_pgns"]) pgns.emplace_back(v.get<std::string>(), GameLevel::Server);

        const std::string schema = json["schema"].get<std::string>();

        const auto temporaryPathsStr =
            json.contains("temporary_paths")
            ? json["temporary_paths"].get<std::vector<std::string>>()
            : std::vector<std::string>{};

        std::vector<std::filesystem::path> temporaryPaths;
        temporaryPaths.reserve(temporaryPathsStr.size());
        for (auto& s : temporaryPathsStr)
        {
            temporaryPaths.emplace_back(s);
        }

        if (json.contains("temporary_path"))
        {
            temporaryPaths.emplace_back(json["temporary_path"].get<std::string>());
        }

        std::optional<MemoryAmount> temporarySpace = std::nullopt;
        if (json.contains("temporary_space"))
        {
            temporarySpace = json["temporary_space"].get<MemoryAmount>();
        }

        handleTcpCommandCreateImpl(db, session, schema, destination, pgns, temporaryPaths, temporarySpace, doMerge, doReportProgress);
    }

    enum struct AppendMergeType
    {
        None,
        All,
        New
    };

    // Calculates lhs - rhs
    static std::map<std::string, std::vector<persistence::MergableFile>> mergableFilesDifference(
        std::map<std::string, std::vector<persistence::MergableFile>> lhs,
        std::map<std::string, std::vector<persistence::MergableFile>> rhs
    )
    {
        std::map<std::string, std::vector<persistence::MergableFile>> result;

        for (auto& [partition, lhsFiles] : lhs)
        {
            auto& resultFiles = result[partition];

            auto& rhsFiles = rhs[partition];
            for (auto& lhsFile : lhsFiles)
            {
                if (std::find_if(
                    rhsFiles.begin(),
                    rhsFiles.end(),
                    [&lhsFile](const persistence::MergableFile& mf)
                    {
                        return mf.name == lhsFile.name;
                    }
                ) == rhsFiles.end())
                {
                    // not found in rhs, therefore not subtracted
                    resultFiles.emplace_back(lhsFile);
                }
            }
        }

        return result;
    }

    static void handleTcpCommandAppendImpl(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const persistence::ImportableFiles& pgns,
        const std::vector<std::filesystem::path>& temporaryPaths,
        std::optional<MemoryAmount> temporarySpace,
        AppendMergeType merge,
        bool doReportProgress
    )
    {
        assertDatabaseOpen(db);
        for (auto& temp : temporaryPaths)
        {
            assertDirectoryEmpty(temp);
        }

        // If we only want to merge the appended files we need
        // to remember the old ones.
        std::map<std::string, std::vector<persistence::MergableFile>> oldMergableFiles;
        if (merge == AppendMergeType::New)
        {
            oldMergableFiles = db->mergableFiles();
        }

        {
            auto callback = makeImportProgressReportHandler(session, doReportProgress);
            auto stats = db->import(pgns, importMemory.bytes(), callback);
            auto statsJson = nlohmann::json::object();
            statsJson["stats"] = stats;
            sendProgressFinished(session, "import", statsJson);

            if (merge == AppendMergeType::All)
            {
                auto callback = makeMergeProgressReportHandler(session, doReportProgress);
                db->mergeAll(temporaryPaths, temporarySpace, callback);
            }
            else if (merge == AppendMergeType::New)
            {
                auto allMergebaleFilesNow = db->mergableFiles();
                auto newMergableFiles = mergableFilesDifference(allMergebaleFilesNow, oldMergableFiles);

                auto callback = makeMergeProgressReportHandler(session, doReportProgress);
                // TODO: proper progress report if multiple partitions.
                for (auto& [partition, files] : newMergableFiles)
                {
                    if (files.empty())
                    {
                        continue;
                    }

                    std::vector<std::string> names;
                    names.reserve(files.size());
                    for (auto& file : files)
                    {
                        names.emplace_back(file.name);
                    }

                    db->merge(temporaryPaths, temporarySpace, partition, names, callback);
                }
            }
        }

        // We have to always sent some info that we finished
        sendProgressFinished(session, "append");
    }

    static void handleTcpCommandAppend(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        AppendMergeType merge = AppendMergeType::None;
        if (json.contains("merge"))
        {
            if (json["merge"] == "all")
            {
                merge = AppendMergeType::All;
            }
            else if (json["merge"] == "new")
            {
                merge = AppendMergeType::New;
            }
        }

        const bool doReportProgress = json["report_progress"].get<bool>();

        persistence::ImportableFiles pgns;
        for (auto& v : json["human_pgns"]) pgns.emplace_back(v.get<std::string>(), GameLevel::Human);
        for (auto& v : json["engine_pgns"]) pgns.emplace_back(v.get<std::string>(), GameLevel::Engine);
        for (auto& v : json["server_pgns"]) pgns.emplace_back(v.get<std::string>(), GameLevel::Server);

        const auto temporaryPathsStr =
            json.contains("temporary_paths")
            ? json["temporary_paths"].get<std::vector<std::string>>()
            : std::vector<std::string>{};

        std::vector<std::filesystem::path> temporaryPaths;
        temporaryPaths.reserve(temporaryPathsStr.size());
        for (auto& s : temporaryPathsStr)
        {
            temporaryPaths.emplace_back(s);
        }

        if (json.contains("temporary_path"))
        {
            temporaryPaths.emplace_back(json["temporary_path"].get<std::string>());
        }

        std::optional<MemoryAmount> temporarySpace = std::nullopt;
        if (json.contains("temporary_space"))
        {
            temporarySpace = json["temporary_space"].get<MemoryAmount>();
        }

        handleTcpCommandAppendImpl(db, session, pgns, temporaryPaths, temporarySpace, merge, doReportProgress);
    }

    static void handleTcpCommandMerge(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        assertDatabaseOpen(db);

        const bool doReportProgress = json["report_progress"].get<bool>();

        const auto temporaryPathsStr =
            json.contains("temporary_paths")
            ? json["temporary_paths"].get<std::vector<std::string>>()
            : std::vector<std::string>{};

        std::vector<std::filesystem::path> temporaryPaths;
        temporaryPaths.reserve(temporaryPathsStr.size());
        for (auto& s : temporaryPathsStr)
        {
            temporaryPaths.emplace_back(s);
        }

        std::optional<MemoryAmount> temporarySpace = std::nullopt;
        if (json.contains("temporary_space"))
        {
            temporarySpace = json["temporary_space"].get<MemoryAmount>();
        }

        auto callback = makeMergeProgressReportHandler(session, doReportProgress);

        if (json.contains("partition"))
        {
            const auto partition = json["partition"].get<std::string>();

            if (json.contains("files"))
            {
                const auto files = json["files"].get<std::vector<std::string>>();
                db->merge(temporaryPaths, temporarySpace, partition, files, callback);
            }
            else
            {
                throw std::runtime_error("partition specified but no files.");
            }
        }
        else
        {
            db->mergeAll(temporaryPaths, temporarySpace, callback);
        }

        // We have to always sent some info that we finished
        sendProgressFinished(session, "merge");
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
        auto responseStr = nlohmann::json(response).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);

        Logger::instance().logInfo("Handled valid request. Response size: ", responseStr.size());

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

        auto response = nlohmann::json(stats);

        auto responseStr = nlohmann::json(response).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
        sendMessage(session, responseStr);
    }

    namespace detail
    {
        struct AsyncStorePipeline
        {
        private:
            using EntryType = CompressedPosition;
            using BufferType = std::vector<EntryType>;
            using PromiseType = std::promise<std::filesystem::path>;
            using FutureType = std::future<std::filesystem::path>;

            struct Job
            {
                Job(std::filesystem::path path, BufferType&& buffer, PromiseType&& promise) :
                    path(std::move(path)),
                    buffer(std::move(buffer)),
                    promise(std::move(promise))
                {
                }

                std::filesystem::path path;
                BufferType buffer;
                PromiseType promise;
            };

        public:
            AsyncStorePipeline(std::vector<BufferType>&& buffers, std::size_t numSortingThreads = 1) :
                m_sortingThreadFinished(false),
                m_writingThreadFinished(false),
                m_writingThread([this]() { runWritingThread(); })
            {
                ASSERT(numSortingThreads >= 1);
                ASSERT(!buffers.empty());

                m_sortingThreads.reserve(numSortingThreads);
                for (std::size_t i = 0; i < numSortingThreads; ++i)
                {
                    m_sortingThreads.emplace_back([this]() { runSortingThread(); });
                }

                for (auto&& buffer : buffers)
                {
                    m_bufferQueue.emplace(std::move(buffer));
                }
            }

            AsyncStorePipeline(const AsyncStorePipeline&) = delete;

            ~AsyncStorePipeline()
            {
                waitForCompletion();
            }

            [[nodiscard]] FutureType scheduleUnordered(const std::filesystem::path& path, BufferType&& elements)
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                PromiseType promise;
                FutureType future = promise.get_future();
                m_sortQueue.emplace(path, std::move(elements), std::move(promise));

                lock.unlock();
                m_sortQueueNotEmpty.notify_one();

                return future;
            }

            [[nodiscard]] BufferType getEmptyBuffer()
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                m_bufferQueueNotEmpty.wait(lock, [this]() {return !m_bufferQueue.empty(); });

                auto buffer = std::move(m_bufferQueue.front());
                m_bufferQueue.pop();

                buffer.clear();

                return buffer;
            }

            void waitForCompletion()
            {
                if (!m_sortingThreadFinished.load())
                {
                    m_sortingThreadFinished.store(true);
                    m_sortQueueNotEmpty.notify_one();
                    for (auto& th : m_sortingThreads)
                    {
                        th.join();
                    }

                    m_writingThreadFinished.store(true);
                    m_writeQueueNotEmpty.notify_one();
                    m_writingThread.join();
                }
            }

        private:
            std::queue<Job> m_sortQueue;
            std::queue<Job> m_writeQueue;
            std::queue<BufferType> m_bufferQueue;

            std::condition_variable m_sortQueueNotEmpty;
            std::condition_variable m_writeQueueNotEmpty;
            std::condition_variable m_bufferQueueNotEmpty;

            std::mutex m_mutex;

            std::atomic_bool m_sortingThreadFinished;
            std::atomic_bool m_writingThreadFinished;

            std::vector<std::thread> m_sortingThreads;
            std::thread m_writingThread;

            void runSortingThread()
            {
                for (;;)
                {
                    std::unique_lock<std::mutex> lock(m_mutex);
                    m_sortQueueNotEmpty.wait(lock, [this]() {return !m_sortQueue.empty() || m_sortingThreadFinished.load(); });

                    if (m_sortQueue.empty())
                    {
                        lock.unlock();
                        m_sortQueueNotEmpty.notify_one();
                        return;
                    }

                    Job job = std::move(m_sortQueue.front());
                    m_sortQueue.pop();

                    lock.unlock();

                    sort(job.buffer);

                    lock.lock();
                    m_writeQueue.emplace(std::move(job));
                    lock.unlock();

                    m_writeQueueNotEmpty.notify_one();
                }
            }

            void runWritingThread()
            {
                for (;;)
                {
                    std::unique_lock<std::mutex> lock(m_mutex);
                    m_writeQueueNotEmpty.wait(lock, [this]() {return !m_writeQueue.empty() || m_writingThreadFinished.load(); });

                    if (m_writeQueue.empty())
                    {
                        lock.unlock();
                        m_writeQueueNotEmpty.notify_one();
                        return;
                    }

                    Job job = std::move(m_writeQueue.front());
                    m_writeQueue.pop();

                    lock.unlock();

                    (void)ext::writeFile(job.path, job.buffer.data(), job.buffer.size());
                    job.promise.set_value(job.path);

                    job.buffer.clear();

                    lock.lock();
                    m_bufferQueue.emplace(std::move(job.buffer));
                    lock.unlock();

                    m_bufferQueueNotEmpty.notify_one();
                }
            }

            void sort(BufferType& buffer)
            {
                std::sort(buffer.begin(), buffer.end());
            }
        };
    }

    static void handleTcpCommandDumpImpl(
        const TcpConnection::Ptr& session,
        const std::vector<std::filesystem::path>& pgns,
        const std::filesystem::path& output,
        const std::vector<std::filesystem::path>& temps,
        std::size_t minN,
        std::size_t maxPly,
        bool doReportProgress
    )
    {
        static const MemoryAmount pgnParserMemory = cfg::g_config["command_line_app"]["dump"]["pgn_parser_memory"].get<MemoryAmount>();
        static const MemoryAmount bcgnParserMemory = cfg::g_config["command_line_app"]["dump"]["bcgn_parser_memory"].get<MemoryAmount>();
        static const MemoryAmount importMemory = cfg::g_config["command_line_app"]["dump"]["import_memory"].get<MemoryAmount>();

        if (temps.empty())
        {
            throw std::runtime_error("No temp folders specified");
        }

        for (auto& temp : temps)
        {
            assertDirectoryEmpty(temp);
        }

        std::size_t numPosOut = 0;
        std::size_t numPosIn = 0;
        std::size_t numGamesIn = 0;

        // this has to be destroyed last
        ext::TemporaryPaths tempPaths(temps[0]);

        auto makeBuffers = [](std::size_t numBuffers)
        {
            ASSERT(numBuffers > 0);

            const std::size_t size = ext::numObjectsPerBufferUnit<CompressedPosition>(importMemory.bytes(), numBuffers);

            std::vector<std::vector<CompressedPosition>> buffers;
            buffers.resize(numBuffers);
            for (auto& buffer : buffers)
            {
                buffer.reserve(size);
            }
            return buffers;
        };

        {
            std::vector<std::future<std::filesystem::path>> futureParts;

            {
                detail::AsyncStorePipeline pipeline(makeBuffers(4), 2);

                auto callback = makeImportProgressReportHandler(session, doReportProgress);

                std::size_t i = 0;
                auto positions = pipeline.getEmptyBuffer();

                auto processGame = [&](auto& reader)
                {
                    for (auto&& game : reader)
                    {
                        ++numGamesIn;

                        std::size_t numPly = 0;
                        for (auto&& position : game.positions())
                        {
                            ++numPosIn;
                            ++numPly;

                            positions.emplace_back(position.compress());

                            if (positions.size() == positions.capacity())
                            {
                                auto path = tempPaths.next();
                                futureParts.emplace_back(pipeline.scheduleUnordered(path, std::move(positions)));
                                positions = pipeline.getEmptyBuffer();
                                Logger::instance().logInfo("Created temp file ", path);
                            }

                            if (numPly >= maxPly)
                            {
                                break;
                            }
                        }
                    }
                };

                for (auto&& pgn : pgns)
                {
                    const auto extension = pgn.extension();

                    if (extension == ".pgn")
                    {
                        pgn::LazyPgnFileReader reader(pgn, pgnParserMemory.bytes());
                        processGame(reader);
                    }
                    else if (extension == ".bcgn")
                    {
                        bcgn::BcgnFileReader reader(pgn, bcgnParserMemory.bytes());
                        processGame(reader);
                    }
                    else
                    {
                        Logger::instance().logError("Invalid input file type ", extension);
                    }

                    ++i;
                    callback({
                        i,
                        pgns.size(),
                        pgn
                        });

                    Logger::instance().logInfo("Finished file ", pgn);
                }

                if (!positions.empty())
                {
                    auto path = tempPaths.next();
                    futureParts.emplace_back(pipeline.scheduleUnordered(path, std::move(positions)));
                    Logger::instance().logInfo("Created temp file ", path);
                }

                sendProgressFinished(session, "import");
            }

            {
                std::vector<ext::ImmutableSpan<CompressedPosition>> files;
                for (auto&& f : futureParts)
                {
                    auto path = f.get();
                    files.emplace_back(ext::ImmutableBinaryFile(ext::Pooled{}, path));
                    Logger::instance().logInfo("Commited file ", path);
                }

                auto progressCallback = [session, doReportProgress](const ext::Progress& report) {
                    if (!doReportProgress) return;

                    auto reportJson = nlohmann::json{
                        { "operation", "dump" },
                        { "overall_progress", report.ratio() },
                        { "finished", false }
                    };

                    auto reportStr = reportJson.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
                    sendMessage(session, reportStr);
                }; 
                
                bool first = true;
                CompressedPosition pos{};
                std::size_t count = 0;
                auto outEpdFile = std::ofstream(output, std::ios_base::out | std::ios_base::app);
                auto append = [
                        &numPosOut,
                        minN,
                        &outEpdFile, 
                        &first, 
                        &pos, 
                        &count
                    ](const CompressedPosition& position) mutable {
                    if (first)
                    {
                        first = false;
                        pos = position;
                        count = 1;
                    }
                    else if (pos == position)
                    {
                        ++count;
                    }
                    else
                    {
                        if (count >= minN)
                        {
                            ++numPosOut;
                            outEpdFile << pos.decompress().fen() << ";\n";
                        }

                        pos = position;
                        count = 1;
                    }
                };

                const auto& temp1 = temps.front();
                const auto& temp2 = temps.back();
                ext::MergePlan plan = ext::make_merge_plan(files, temp1, temp2);

                auto cleanup = [&files, &tempPaths]() {
                    files.clear();
                    tempPaths.clear();
                };

                ext::MergeCallbacks callbacks{
                    progressCallback,
                    [&cleanup](int passId) {
                        if (passId == 0)
                        {
                            cleanup();
                        }
                    }
                };
                ext::merge_for_each(plan, callbacks, files, append, std::less<>{});

                if (!files.empty())
                {
                    cleanup();
                }

                if (!first && count >= minN) // if we did anything, ie. accumulator holds something from merge
                {
                    ++numPosOut;
                    outEpdFile << pos.decompress().fen() << ";\n";
                }
            }

            auto stats = nlohmann::json{
                { "num_games", numGamesIn },
                { "num_in_positions", numPosIn },
                { "num_out_positions", numPosOut }
            };
            sendProgressFinished(session, "dump", stats);
        }
    }

    static void handleTcpCommandDump(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        std::vector<std::filesystem::path> pgns;
        for (auto& v : json["pgns"]) pgns.emplace_back(v.get<std::string>());

        const std::filesystem::path epdOut = json["output_path"].get<std::string>();
        const bool reportProgress = json["report_progress"];
        const std::size_t minN = json["min_count"].get<std::size_t>();
        const std::size_t maxPly = json["max_ply"].get<std::size_t>();

        if (minN == 0) throw Exception("Min count must be positive.");

        const std::vector<std::string> tempPathsStr =
            json.contains("temporary_paths")
            ? json["temporary_paths"].get<std::vector<std::string>>()
            : std::vector<std::string>{};

        std::vector<std::filesystem::path> tempPaths;
        for (auto& pathstr : tempPathsStr)
        {
            tempPaths.emplace_back(pathstr);
        }
        if (tempPaths.empty())
        {
            tempPaths.emplace_back(epdOut.parent_path() / ".tmp");
        }
        handleTcpCommandDumpImpl(session, pgns, epdOut, tempPaths, minN, maxPly, reportProgress);
    }

    static void handleTcpCommandSupport(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        auto manifests = g_factory.supportManifests();
        auto manifestsJson = nlohmann::json::object();
        for (auto&& [name, manifest] : manifests)
        {
            manifestsJson[name] = manifest;
        }

        auto response = nlohmann::json{
            { "support_manifests", manifestsJson }
        };

        auto responseStr = nlohmann::json(response).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
        sendMessage(session, responseStr);
    }

    static void handleTcpCommandManifest(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        assertDatabaseOpen(db);

        auto manifest = db->manifest();
        auto manifestJson = nlohmann::json(manifest);

        auto response = nlohmann::json{
            { "manifest", manifestJson }
        };

        auto responseStr = nlohmann::json(response).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
        sendMessage(session, responseStr);
    }

    static void handleTcpCommandMergableFiles(
        std::unique_ptr<persistence::Database>& db,
        const TcpConnection::Ptr& session,
        const nlohmann::json& json
    )
    {
        assertDatabaseOpen(db);

        auto mergableFiles = db->mergableFiles();
        auto mergableFilesJson = nlohmann::json(mergableFiles);

        auto response = nlohmann::json{
            { "mergable_files", std::move(mergableFiles) }
        };

        auto responseStr = nlohmann::json(response).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
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
            { "append", handleTcpCommandAppend },
            { "merge", handleTcpCommandMerge },
            { "open", handleTcpCommandOpen },
            { "close", handleTcpCommandClose },
            { "query", handleTcpCommandQuery },
            { "stats", handleTcpCommandStats },
            { "dump", handleTcpCommandDump },
            { "support", handleTcpCommandSupport },
            { "manifest", handleTcpCommandManifest },
            { "mergable_files", handleTcpCommandMergableFiles }
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
        catch (std::runtime_error& ex)
        {
            Logger::instance().logError("Error while trying to perform request: " + std::string(ex.what()));

            auto errorJson = nlohmann::json::object({ {"error", ex.what() } }).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
            sendMessage(session, errorJson);
        }
        catch (nlohmann::json::exception& ex)
        {
            Logger::instance().logError("Error while trying to perform request: " + std::string(ex.what()));

            auto errorJson = nlohmann::json::object({ {"error", ex.what() } }).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
            sendMessage(session, errorJson);
        }
        catch (std::out_of_range& ex)
        {
            Logger::instance().logError("Error while trying to perform request: " + std::string(ex.what()));

            auto errorJson = nlohmann::json::object({ {"error", ex.what() } }).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
            sendMessage(session, errorJson);
        }
        catch (...)
        {
            Logger::instance().logError("Unknown error");

            auto errorJson = nlohmann::json::object({ {"error", "Unknown error" } }).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
            sendMessage(session, errorJson);
        }

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
                    [&mutex, &anyOperations, &operations, &db, session, messageReceiver = MessageReceiver()]
                    (const char* buffer, size_t len) mutable {
                        try
                        {
                            auto messages = messageReceiver.onDataReceived(buffer, len);
                            for (auto&& message : messages)
                            {
                                std::unique_lock lock(mutex);
                                operations.emplace(Operation{ session, std::move(message) });
                            }
                            if (!messages.empty())
                            {
                                anyOperations.notify_one();
                            }
                        }
                        catch (Exception& ex)
                        {
                            sendMessage(session, std::string("{\"error\":\"") + ex.what() + "\"");
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

    static void tcp(args::Subparser& parser)
    {
        args::Group requiredArgs(parser, "required arguments", args::Group::Validators::All);
        args::ValueFlag<std::uint16_t> port(requiredArgs, "port", "The local port to use", { "port" });

        args::ValueFlag<std::string> open(parser, "path", "Optional database path. If specified it will only allow queries to be made.", { "open" });
        
        parser.Parse();

#if defined(__clang__)
        throw std::runtime_error("Problems with brynet with clang-cl. Not available right now.");
#else

        auto portValue = args::get(port);
        if (open.Get() != "")
        {
            Logger::instance().logInfo(std::string("Running TCP with open database on port ") + std::to_string(portValue));
            tcpImpl(args::get(open), portValue);
        }
        else
        {
            Logger::instance().logInfo(std::string("Running TCP on port ") + std::to_string(portValue));
            tcpImpl(portValue);
        }
#endif
    }

    static void convertPgnToBcgnImpl(
        const std::filesystem::path& pgn, 
        const std::filesystem::path& bcgn,
        const bcgn::BcgnFileHeader& header,
        bcgn::BcgnFileWriter::FileOpenMode mode)
    {
        pgn::LazyPgnFileReader pgnReader(pgn, pgnParserMemory.bytes());
        bcgn::BcgnFileWriter bcgnWriter(bcgn, header, mode, bcgnParserMemory.bytes());

        constexpr std::size_t reportEvery = 100'000;

        std::size_t nextReport = 0;
        std::size_t totalCount = 0;
        for (auto&& game : pgnReader)
        {
            const std::string_view fenTag = game.tag("FEN");
            const bool hasCustomStartpos = !fenTag.empty();

            Position pos = 
                hasCustomStartpos
                ? Position::fromFen(fenTag.data())
                : Position::startPosition();

            bcgnWriter.beginGame();

            if (header.isHeaderless)
            {
                const auto result = game.result();
                if (result.has_value())
                {
                    bcgnWriter.setResult(*result);
                }
            }
            else
            {
                std::optional<GameResult> result;
                Date date;
                Eco eco;
                std::string_view event;
                std::string_view white;
                std::string_view black;
                game.getResultDateEcoEventWhiteBlack(
                    result,
                    date,
                    eco,
                    event,
                    white,
                    black
                    );

                bcgnWriter.setWhiteElo(game.whiteElo());
                bcgnWriter.setBlackElo(game.blackElo());
                bcgnWriter.setDate(date);
                bcgnWriter.setEco(eco);
                bcgnWriter.setRound(game.round());
                bcgnWriter.setWhitePlayer(white);
                bcgnWriter.setBlackPlayer(black);
                bcgnWriter.setEvent(event);
                bcgnWriter.setSite(game.tag("Site"sv));
                if (result.has_value())
                {
                    bcgnWriter.setResult(*result);
                }
            }

            if (hasCustomStartpos && pos != Position::startPosition())
            {
                bcgnWriter.setCustomStartPos(pos);
            }

            for (auto&& san : game.moves())
            {
                const auto move = san::sanToMove(pos, san);

                bcgnWriter.addMove(pos, move);

                pos.doMove(move);
            }

            bcgnWriter.endGame();

            ++totalCount;
            if (totalCount >= nextReport)
            {
                std::cout << "Converted " << totalCount << " games...\n";
                nextReport += reportEvery;
            }
        }
        std::cout << "Converted " << totalCount << " games...\n";
    }

    static void convert(args::Subparser& parser)
    {
        args::Flag headerless(parser, "headerless", "Only store critical information in the game headers", { 'h', "headerless" });
        args::Flag append(parser, "append", "Append to an already existing file.", { 'a', "append" });
        args::ValueFlag<std::uint32_t> compressionLevel(parser, "compression", "The compression level to use for BCGN files. Currently supports 0, 1, or 2. For further info see BCGN documentation.", { 'c', "compression" }, 0u);

        args::Group requiredArgs(parser, "required arguments", args::Group::Validators::All);
        args::Positional<std::string> input(requiredArgs, "input path", "File to convert from.");
        args::Positional<std::string> output(requiredArgs, "output path", "File to convert to.");

        parser.Parse();

        const std::filesystem::path from = args::get(input);
        const std::filesystem::path to = args::get(output);

        if (from.extension() == ".pgn" && to.extension() == ".bcgn")
        {
            bcgn::BcgnFileHeader header{};
            auto mode = bcgn::BcgnFileWriter::FileOpenMode::Truncate;

            switch (args::get(compressionLevel))
            {
            case 0:
                header.compressionLevel = bcgn::BcgnCompressionLevel::Level_0;
                break;

            case 1:
                header.compressionLevel = bcgn::BcgnCompressionLevel::Level_1;
                break;

            case 2:
                header.compressionLevel = bcgn::BcgnCompressionLevel::Level_2;
                break;
            }

            if (headerless)
            {
                header.isHeaderless = true;
            }

            if (append)
            {
                mode = bcgn::BcgnFileWriter::FileOpenMode::Append;
            }
            
            convertPgnToBcgnImpl(from, to, header, mode);
        }
        else
        {
            std::cout << "Only PGN -> BCGN is supported right now.\n";
        }
    }

    static void countPgnGames(const std::filesystem::path& path)
    {
        pgn::LazyPgnFileReader reader(path, pgnParserMemory.bytes());

        constexpr std::size_t reportEvery = 100'000;

        std::size_t nextReport = 0;
        std::size_t totalCount = 0;
        for (auto&& game : reader)
        {
            ++totalCount;
            if (totalCount >= nextReport)
            {
                std::cout << "Found " << totalCount << " games...\n";
                nextReport += reportEvery;
            }
        }
        std::cout << "Found " << totalCount << " games...\n";
    }

    static void countBcgnGames(const std::filesystem::path& path)
    {
        bcgn::BcgnFileReader reader(path, bcgnParserMemory.bytes());

        constexpr std::size_t reportEvery = 100'000;

        std::size_t nextReport = 0;
        std::size_t totalCount = 0;
        for (auto&& game : reader)
        {
            ++totalCount;
            if (totalCount >= nextReport)
            {
                std::cout << "Found " << totalCount << " games...\n";
                nextReport += reportEvery;
            }
        }
        std::cout << "Found " << totalCount << " games...\n";
    }

    static void countGames(args::Subparser& parser)
    {
        args::Group requiredArgs(parser, "required arguments", args::Group::Validators::All);
        args::Positional<std::string> input(requiredArgs, "input path", "The path to a PGN or BCGN file.");

        parser.Parse();

        const std::filesystem::path path = args::get(input);
        if (path.extension() == ".pgn")
        {
            countPgnGames(path);
        }
        else if (path.extension() == ".bcgn")
        {
            countBcgnGames(path);
        }
        else
        {
            throwInvalidArguments();
        }
    }

    template <typename ReaderT>
    static void benchReader(const std::filesystem::path& path, std::size_t memory)
    {
        const auto size = std::filesystem::file_size(path);
        std::cout << "File size: " << size << '\n';

        for (int i = 0; i < 2; ++i)
        {
            // warmup
            ReaderT reader(path, memory);
            for (auto&& game : reader);
            std::cout << "warmup " << i << " finished\n";
        }

        std::this_thread::sleep_for(std::chrono::seconds{ 1 });

        const auto t0 = std::chrono::high_resolution_clock::now();
        ReaderT reader(path, memory);
        std::size_t numGames = 0;
        std::size_t numPositions = 0;
        for (auto&& game : reader)
        {
            numGames += 1;
            for (auto&& position : game.positions())
            {
                numPositions += 1;
            }
        }
        const auto t1 = std::chrono::high_resolution_clock::now();
        const double time = (t1 - t0).count() / 1e9;

        std::cout << numGames << " games in " << time << "s\n";
        std::cout << (std::uint64_t)(numGames / time) << " games/s\n";
        std::cout << numPositions << " positions in " << time << "s\n";
        std::cout << (std::uint64_t)(numPositions / time) << " positions/s\n";
        std::cout << "Throughput of " << size / time / 1e6 << " MB/s\n";
    }

    static void benchPgn(const std::filesystem::path& path)
    {
        benchReader<pgn::LazyPgnFileReader>(path, pgnParserMemory.bytes());
    }

    static void benchBcgn(const std::filesystem::path& path)
    {
        benchReader<bcgn::BcgnFileReader>(path, bcgnParserMemory.bytes());
    }

    static void bench(args::Subparser& parser)
    {
        args::Group requiredArgs(parser, "required arguments", args::Group::Validators::All);
        args::Positional<std::string> input(requiredArgs, "input path", "The path to a PGN or BCGN file.");

        parser.Parse();

        const std::filesystem::path path = args::get(input);
        if (path.extension() == ".pgn")
        {
            benchPgn(path);
        }
        else if (path.extension() == ".bcgn")
        {
            benchBcgn(path);
        }
        else
        {
            throwInvalidArguments();
        }
    }

    template <typename ReaderT>
    static void statsImpl(const std::filesystem::path& path, std::size_t memory)
    {
        ReaderT reader(path, memory);
        std::size_t numGames = 0;
        std::size_t numPositions = 0;
        std::size_t numWhiteWins = 0;
        std::size_t numDraws = 0;
        std::size_t numBlackWins = 0;
        for (auto&& game : reader)
        {
            auto result = game.result();
            if (result.has_value())
            {
                if (*result == GameResult::WhiteWin) numWhiteWins += 1;
                else if (*result == GameResult::Draw) numDraws += 1;
                else numBlackWins += 1;
            }

            numGames += 1;
            if constexpr (std::is_same_v<ReaderT, pgn::LazyPgnFileReader>)
            {
                for (auto&& san : game.moves())
                {
                    numPositions += 1;
                }
            }
            else
            {
                numPositions += game.numPlies() + 1;
            }
        }

        const auto size = std::filesystem::file_size(path);
        std::cout << "Path      : " << path << '\n';
        std::cout << "File size : " << size << '\n';
        std::cout << "Games     : " << numGames << '\n';
        std::cout << "Positions : " << numPositions << '\n';
        std::cout << "Wins      : " << numWhiteWins << '\n';
        std::cout << "Draws     : " << numDraws << '\n';
        std::cout << "Losses    : " << numBlackWins << '\n';
        std::cout << '\n';
    }

    static void statsPgn(const std::filesystem::path& path)
    {
        statsImpl<pgn::LazyPgnFileReader>(path, pgnParserMemory.bytes());
    }

    static void statsBcgn(const std::filesystem::path& path)
    {
        statsImpl<bcgn::BcgnFileReader>(path, bcgnParserMemory.bytes());
    }

    static void stats(args::Subparser& parser)
    {
        args::Group requiredArgs(parser, "required arguments", args::Group::Validators::All);
        args::Positional<std::string> input(requiredArgs, "input path", "The path to a PGN or BCGN file.");

        parser.Parse();

        const std::filesystem::path path = args::get(input);
        if (path.extension() == ".pgn")
        {
            statsPgn(path);
        }
        else if (path.extension() == ".bcgn")
        {
            statsBcgn(path);
        }
        else
        {
            throwInvalidArguments();
        }
    }

    static void interactive(args::Subparser& parser)
    {
        parser.Parse();

        console_app::App console_app;
        console_app.run();
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
        Position pos = game.startPosition();
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

    static void verifyPgn(const std::filesystem::path& path, bool bail)
    {
        constexpr std::size_t progressEvery = 100000;

        pgn::LazyPgnFileReader reader(path);
        std::size_t gameId = 0;
        for (auto&& game : reader)
        {
            ++gameId;

            if (!verifyPgnTags(game, gameId))
            {
                if (bail) return;
                else continue;
            }
            if (!verifyPgnMoves(game, gameId))
            {
                if (bail) return;
                else continue;
            }

            if (gameId % progressEvery == 0)
            {
                std::cout << "So far verified " << gameId << " games...\n";
            }
        }
        std::cerr << "Verified " << gameId << " games.\n";
    }

    static void verify(args::Subparser& parser)
    {
        args::Group requiredArgs(parser, "required arguments", args::Group::Validators::All);
        args::Positional<std::string> input(requiredArgs, "input path", "The path to a PGN or BCGN file.");

        args::Flag bail(parser, "bail", "Bail on first error.", { "bail" });

        parser.Parse();

        const std::filesystem::path path = args::get(input);
        if (path.extension() == ".pgn")
        {
            verifyPgn(path, args::get(bail));
        }
        else
        {
            std::cerr << "Only .pgn files are supported for now.\n";
        }
    }

    template <typename IterT>
    struct SharedPtrIterator
    {
        SharedPtrIterator(IterT&& it) noexcept :
            m_iter(std::make_shared<IterT>(std::move(it)))
        {
        }

        SharedPtrIterator(const SharedPtrIterator&) = default;
        SharedPtrIterator(SharedPtrIterator&&) noexcept = default;

        SharedPtrIterator& operator=(const SharedPtrIterator&) = default;
        SharedPtrIterator& operator=(SharedPtrIterator&&) noexcept = default;

        SharedPtrIterator& operator=(IterT&& it) noexcept
        {
            m_iter = std::make_shared<IterT>(std::move(it));
        }

        template <typename T>
        [[nodiscard]] bool operator==(const T& t) const noexcept
        {
            return *m_iter == t;
        }

        template <typename T>
        [[nodiscard]] bool operator==(const SharedPtrIterator<T>& t) const noexcept
        {
            return *m_iter == *(t.m_iter);
        }

        template <typename T>
        [[nodiscard]] bool operator!=(const T& t) const noexcept
        {
            return *m_iter != t;
        }

        template <typename T>
        [[nodiscard]] bool operator!=(const SharedPtrIterator<T>& t) const noexcept
        {
            return *m_iter != *(t.m_iter);
        }

        [[nodiscard]] decltype(auto) operator*() const
        {
            return **m_iter;
        }

        decltype(auto) operator++() const
        {
            return ++*m_iter;
        }

    private:
        std::shared_ptr<IterT> m_iter;
    }; 

    template <typename IterT>
    SharedPtrIterator(IterT&&) -> SharedPtrIterator<std::remove_reference_t<IterT>>;

    struct EpdDumpInputIterator
    {
        using value_type = CompressedPosition;
        using reference = const CompressedPosition&;
        using iterator_category = std::input_iterator_tag;
        using pointer = const CompressedPosition*;

        EpdDumpInputIterator() noexcept :
            m_path{}
        {
        }

        EpdDumpInputIterator(EpdDumpInputIterator&& other) noexcept :
            m_path(std::move(other.m_path)),
            m_file(std::move(other.m_file)),
            m_pos(std::move(other.m_pos))
        {

        }

        EpdDumpInputIterator(std::filesystem::path path) :
            m_path(std::move(path))
        {
            if (!m_path.empty())
            {
                m_file = std::ifstream(m_path);
            }

            m_pos = nextPos();
        }

        [[nodiscard]] friend bool operator==(const EpdDumpInputIterator& lhs, const EpdDumpInputIterator& rhs) noexcept
        {
            if (rhs.m_path.empty())
            {
                return lhs.m_file.eof();
            }
            else
            {
                return false;
            }
        }

        [[nodiscard]] friend bool operator!=(const EpdDumpInputIterator& lhs, const EpdDumpInputIterator& rhs) noexcept
        {
            return !(lhs == rhs);
        }

        EpdDumpInputIterator& operator++()
        {
            m_pos = nextPos();
            return *this;
        }

        [[nodiscard]] const CompressedPosition& operator*() const
        {
            return m_pos;
        }

    private:
        std::filesystem::path m_path;
        std::ifstream m_file;
        CompressedPosition m_pos;

        [[nodiscard]] CompressedPosition nextPos()
        {
            constexpr std::size_t bufferSize = 256;
            constexpr char lineDelimiter = ';';

            char buffer[bufferSize];
            m_file.getline(buffer, bufferSize, lineDelimiter);

            return Position::fromFen(buffer).compress();
        }
    };

    struct EpdDumpOutputIterator
    {
        using value_type = CompressedPosition;
        using reference = const CompressedPosition&;
        using iterator_category = std::output_iterator_tag;
        using pointer = const CompressedPosition*;

        struct AssignProxy
        {
            EpdDumpOutputIterator* parent;

            void operator=(const CompressedPosition& pos)
            {
                parent->assign(pos);
            }
        };

        EpdDumpOutputIterator(std::filesystem::path path) :
            m_path(std::move(path)),
            m_file(m_path)
        {
        }

        EpdDumpOutputIterator(EpdDumpOutputIterator&& other) noexcept :
            m_path(std::move(other.m_path)),
            m_file(std::move(other.m_file)),
            m_nextLine(std::move(other.m_nextLine))
        {

        }

        EpdDumpOutputIterator& operator++()
        {
            writeback();
            return *this;
        }

        [[nodiscard]] AssignProxy operator*()
        {
            return AssignProxy{ this };
        }

        ~EpdDumpOutputIterator()
        {
            writeback();
        }

    private:
        std::filesystem::path m_path;
        std::ofstream m_file;
        std::string m_nextLine;

        void assign(const CompressedPosition& pos)
        {
            m_nextLine = pos.decompress().fen() + ";\n";
        }

        void writeback()
        {
            if (!m_nextLine.empty())
            {
                m_file << m_nextLine;
                m_nextLine.clear();
            }
        }
    };

    static void epdDumpUnionImpl(const std::string& outputPath, const std::vector<std::string>& inputPaths)
    {
        if (inputPaths.size() != 2)
        {
            throw std::runtime_error("Only two epd dumps can be merged at a time for now.");
        }

        std::set_union(
            SharedPtrIterator(EpdDumpInputIterator(inputPaths[0])), SharedPtrIterator(EpdDumpInputIterator{}),
            SharedPtrIterator(EpdDumpInputIterator(inputPaths[1])), SharedPtrIterator(EpdDumpInputIterator{}),
            SharedPtrIterator(EpdDumpOutputIterator(outputPath)),
            std::less<>{}
        );
    }

    static void epdDump(args::Subparser& parser)
    {
        args::Group requiredArgs(parser, "required arguments", args::Group::Validators::All);

        args::PositionalList<std::string> input(requiredArgs, "input paths", "Paths to EPD dumps.");

        args::Flag unionFlag(parser, "union", "Union of EPD dumps.", { "union" });
        args::ValueFlag<std::string> output(requiredArgs, "path", "Output path", { 'o', "output" });

        parser.Parse();

        if (unionFlag)
        {
            epdDumpUnionImpl(args::get(output), args::get(input));
        }
        else
        {
            throwInvalidArguments();
        }
    }

    void run(int argc, char* argv[])
    {
        args::Group arguments("arguments");
        args::HelpFlag help(arguments, "help", "help", { 'h', "help" });

        args::ArgumentParser parser("Command line application...");
        args::Group commands(parser, "commands"); 

        args::Command create(commands, "create", "Create a database from input files", &create);
        args::Command append(commands, "append", "Append files to an already existing database", &append);
        args::Command merge(commands, "merge", "Merge (optimize) files of an already existing database", &merge);
        args::Command tcp(commands, "tcp", "Run a local TCP server allowing other processes to execute commands", &tcp);
        args::Command convert(commands, "convert", "Convert between PGN, BCGN file formats", &convert);
        args::Command countGames(commands, "count_games", "Count games in a PGN/BCGN file", &countGames);
        args::Command stats(commands, "stats", "Calculate statistics for a PGN/BCGN file", &stats);
        args::Command bench(commands, "bench", "Benchmark processing speed of PGN/BCGN file", &bench);
        args::Command interactive(commands, "interactive", "Launch an interactive, stateful command line for extended operation.", &interactive);
        args::Command verify(commands, "verify", "Very a PGN/BCGN file.", &verify);
        args::Command epdDump(commands, "epd_dump", "Various stuff about EPD position files", &epdDump);

        args::GlobalOptions globals(parser, arguments);

        try
        {
            parser.ParseCLI(argc, argv);
        }
        catch (args::Help)
        {
            std::cout << parser;
        }
        catch (args::Error& e)
        {
            Logger::instance().logError(e.what());
            std::cerr << e.what() << std::endl << parser;
        }
        catch (std::runtime_error& e)
        {
            Logger::instance().logError(e.what());
        }
        catch (...)
        {
            Logger::instance().logError("Unknown error.");
        }
    }
}
