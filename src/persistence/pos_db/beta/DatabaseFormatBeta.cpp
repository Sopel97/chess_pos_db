#include "DatabaseFormatBeta.h"

namespace persistence
{
    namespace db_beta
    {
        template struct persistence::pos_db::OrderedEntrySetPositionDatabase<
            Key,
            Entry,
            Traits
        >;
    }
}