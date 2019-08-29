#pragma once

#include <cassert>

#if defined(_MSC_VER) && defined(NDEBUG)

// TODO: investigate.
// MSVC seems to do weird things when __assume is used
// When compiler in release mode the __assume breaks the code, results in unpredictable behaviour
// But when checking for an error and throwing it never happens
#define ASSERT(e)

#else

#define ASSERT(e) assert(e)

#endif
