#include "DatabaseFormatEpsilon.h"

namespace persistence
{
    namespace db_epsilon
    {
        template struct persistence::pos_db::OrderedEntrySetPositionDatabase<
            Key,
            Entry,
            Traits
        >;
    }
}