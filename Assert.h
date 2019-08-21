#pragma once

#include <cassert>

#if defined(_MSC_VER) && defined(NDEBUG)

#define ASSERT(e) __assume(e)

#else

#define ASSERT(e) assert(e)

#endif
