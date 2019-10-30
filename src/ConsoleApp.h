#include "persistence/pos_db/Database.h"

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "json/json.hpp"

using namespace std::literals;

namespace console_app
{
    struct InvalidCommand : std::runtime_error
    {
        using std::runtime_error::runtime_error;
    };

    struct App
    {
    private:
        using Args = std::vector<std::string>;

        using CommandFunction = std::function<void(App*, const Args&)>;

        void assertDatabaseOpened() const;

    public:

        App();

        void run();

    private:
        std::unique_ptr<persistence::Database> m_database;

        void help(const Args& args) const;

        void bench(const Args& args);

        void open(const Args& args);

        void query(const Args& args);

        void info(const Args& args);

        void merge(const Args& args);

        void verify(const Args& args);

        void close(const Args& args);

        void create(const Args& args);

        void destroy(const Args& args);

        void dump(const Args& args);

        static const std::map<std::string_view, CommandFunction> m_commands;
    };
}
