#include "DatabaseFormatDeltaV2.h"

namespace persistence
{
    namespace db_delta_v2
    {
        template struct persistence::db::OrderedEntrySetPositionDatabase<
            Key,
            Entry,
            Traits
        >;
    }
}