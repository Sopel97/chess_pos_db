#include "DatabaseFormatDeltaSmeared.h"

namespace persistence
{
    namespace db_delta_smeared
    {
        template struct persistence::pos_db::OrderedEntrySetPositionDatabase<
            Key,
            UnsmearedEntry,
            Traits
        >;
    }
}