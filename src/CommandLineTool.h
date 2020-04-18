#pragma once

#include <stdexcept>
#include <vector>

namespace command_line_app
{
    using namespace std::literals;

    struct Exception : std::runtime_error
    {
        using BaseType = std::runtime_error;

        using BaseType::BaseType;
    };

    void runCommand(const std::vector<std::string>& args);
}
