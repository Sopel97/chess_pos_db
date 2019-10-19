#include "CommandLineTool.h"

#include "chess/GameClassification.h"

#include "persistence/pos_db/alpha/DatabaseFormatAlpha.h"
#include "persistence/pos_db/beta/DatabaseFormatBeta.h"
#include "persistence/pos_db/Database.h"
#include "persistence/pos_db/DatabaseFactory.h"

#include "util/MemoryAmount.h"

#include "Configuration.h"

#include <filesystem>
#include <fstream>
#include <map>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace command_line_app
{
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

    void runCommand(int argc, char* argv[])
    {
        static const std::map<std::string, CommandHandler> s_commandHandlers = {
            { "help", help },
            { "create", create },
            { "merge", merge }
        };

        if (argc <= 0)
        {
            std::cerr << "No work requested. See help.\n";
            return;
        }

        auto args = convertCommandLineArguments(argc, argv);

        auto handlerIt = s_commandHandlers.find(args[0]);
        if (handlerIt == s_commandHandlers.end())
        {
            throwInvalidCommand(args[0]);
        }

        handlerIt->second(args);
    }
}
