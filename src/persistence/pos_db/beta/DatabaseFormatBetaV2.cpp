#include "DatabaseFormatBetaV2.h"

namespace persistence
{
    namespace db_beta_v2
    {
        template struct persistence::db::OrderedEntrySetPositionDatabase<
            Key,
            Entry,
            Traits
        >;
    }
}