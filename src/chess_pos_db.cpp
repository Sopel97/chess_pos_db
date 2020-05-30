#include "CommandLineTool.h"

#include <iostream>
#include <vector>
#include <string>

#include "Logger.h"

int main(int argc, char* argv[])
{
    try
    {
        command_line_app::run(argc, argv);
    }
    catch (...)
    {
        return 1;
    }

    return 0;
}