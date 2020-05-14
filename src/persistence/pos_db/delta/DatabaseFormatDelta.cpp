#include "DatabaseFormatDelta.h"

namespace persistence
{
    namespace db_delta
    {
        template struct persistence::pos_db::OrderedEntrySetPositionDatabase<
            Key,
            Entry,
            Traits
        >;
    }
}