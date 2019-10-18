#pragma once

#include <cassert>

#if defined(_MSC_VER) && defined(NDEBUG)

// TODO: investigate.
// MSVC seems to do weird things when __assume is used
// When compiler in release mode the __assume breaks the code, results in unpredictable behaviour
// But when checking for an error and throwing it never happens
// NOTE: left as terminate until the problem with __assume is not resolved
#define ASSERT(e) /*if (!(e)) throw std::runtime_error("");*/

#else

#define ASSERT(e) assert(e)

#endif

#define TEST_ASSERT(e) if (!(e)) throw std::runtime_error("Test failed.");
