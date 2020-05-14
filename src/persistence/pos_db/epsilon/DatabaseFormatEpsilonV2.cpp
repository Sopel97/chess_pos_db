#include "DatabaseFormatEpsilonV2.h"

namespace persistence
{
    namespace db_epsilon_v2
    {
        template struct persistence::db::OrderedEntrySetPositionDatabase<
            Key,
            Entry,
            Traits
        >;
    }
}