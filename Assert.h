#pragma once

#include <cassert>

#if defined(_MSC_VER) && defined(NDEBUG)

// TODO: investigate.
// MSVC seems to do weird things when __assume is used
// When compiler in release mode the __assume breaks the code, results in unpredictable behaviour
// But when checking for an error and throwing it never happens
// Current code hits an infinite look when looking up pseudoAttacks with sq computed at San.h:273.
// Uncommenting line San.h:274 makes it run through and produce correct output
#define ASSERT(e) __assume(e)

#else

#define ASSERT(e) assert(e)

#endif

#define TEST_ASSERT(e) assert(e)
