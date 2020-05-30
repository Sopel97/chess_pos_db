#pragma once

#include <stdexcept>

namespace command_line_app
{
    using namespace std::literals;

    struct Exception : std::runtime_error
    {
        using BaseType = std::runtime_error;

        using BaseType::BaseType;
    };

    void run(int argc, char* argv[]);
}
