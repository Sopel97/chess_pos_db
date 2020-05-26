#include "DatabaseFormatEpsilonSmeared.h"

namespace persistence
{
    namespace db_epsilon_smeared
    {
        template struct persistence::pos_db::OrderedEntrySetPositionDatabase<
            Key,
            UnsmearedEntry,
            Traits
        >;
    }
}