#pragma once

#include "data_structure/Enum.h"

#include <string_view>

namespace command_line_app
{
    using namespace std::literals;

    struct Exception : std::runtime_error
    {
        using BaseType = std::runtime_error;

        using BaseType::BaseType;
    };

    void runCommand(int argc, char* argv[]);
}
