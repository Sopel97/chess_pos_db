#include "ConsoleApp.h"
#include "CommandLineTool.h"

#include <iostream>
#include <vector>
#include <string>

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

int main(int argc, char* argv[])
{
    /*
    console_app::App console_app;
    console_app.run();
    */

    std::string cmdline;
    std::getline(std::cin, cmdline);
    auto args = parseCommand(cmdline);

    std::vector<char*> c;
    for (auto& arg : args)
    {
        c.emplace_back(arg.data());
    }

    try
    {
        command_line_app::runCommand(args.size(), c.data());
    }
    catch (command_line_app::Exception& e)
    {
        std::cerr << e.what();
        return 1;
    }
    catch (...)
    {
        std::cerr << "Unknown error.";
        return 2;
    }

    return 0;
}
