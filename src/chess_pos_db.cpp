#include "ConsoleApp.h"
#include "CommandLineTool.h"

#include <iostream>
#include <vector>
#include <string>

#include "Logger.h"

[[nodiscard]] static std::vector<std::string> parseCommand(const std::string& cmd)
{
    std::vector<std::string> parts;
    if (!cmd.empty()) parts.emplace_back();

    bool escaped = false;
    for (char c : cmd)
    {
        if (c == '`')
        {
            escaped = !escaped;
            continue;
        }

        if (!escaped && std::isspace(c))
        {
            parts.emplace_back();
        }
        else
        {
            parts.back() += c;
        }
    }

    return parts;
}

extern void testBcgnWriter();

int main(int argc, char* argv[])
{
    testBcgnWriter();
    return 0;
    if (argc == 1)
    {
        console_app::App console_app;
        console_app.run();
    }
    else
    {
        try
        {
            command_line_app::runCommand(argc - 1, argv + 1);
        }
        catch (command_line_app::Exception& e)
        {
            Logger::instance().logError(e.what());
            return 1;
        }
        catch (std::runtime_error& e)
        {
            Logger::instance().logError(e.what());
            return 2;
        }
        catch (...)
        {
            Logger::instance().logError("Unknown error.");
            return 3;
        }
    }

    return 0;
}
