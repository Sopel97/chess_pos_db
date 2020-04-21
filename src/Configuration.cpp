#include "Configuration.h"

#include "util/MemoryAmount.h"

#include "Logger.h"

#include <fstream>
#include <cstdint>
#include <string>
#include <iostream>

#include "json/json.hpp"

namespace cfg
{
    namespace detail
    {
        enum struct CommentType
        {
            None,
            Single,
            Multi
        };

        static std::string stripComments(const std::string& str)
        {
            /*
            MIT License
            Copyright (c) Sindre Sorhus <sindresorhus@gmail.com> (sindresorhus.com)
            Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
            The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
            THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
            */

            std::string ret;
            ret.reserve(str.length());

            bool insideString = false;
            auto commentType = CommentType::None;

            std::size_t begin = 0;
            const std::size_t length = str.size();
            for (std::size_t i = 0; i < length; ++i)
            {
                const char currentChar = str[i];

                // we sometimes access the 0 terminator but it's well defined
                const char nextChar = str[i + 1];

                // check if we're entering a string
                if (
                    commentType == CommentType::None
                    && currentChar == '"'
                    && (
                        // check if the " is not escaped
                        i < 2
                        || str[i - 1] != '\\'
                        || str[i - 2] == '\\'
                        )
                    )
                {
                    insideString = !insideString;
                }

                if (insideString)
                {
                    continue;
                }

                // check if we encountered a single comment
                if (commentType == CommentType::None && currentChar == '/' && nextChar == '/')
                {
                    ret.append(str, begin, i - begin);
                    begin = i;
                    commentType = CommentType::Single;

                    ++i;
                }
                else if (commentType == CommentType::Single && currentChar == '\n')
                {
                    commentType = CommentType::None;
                    begin = i;
                }
                else if (commentType == CommentType::None && currentChar == '/' && nextChar == '*')
                {
                    ret.append(str, begin, i - begin);
                    begin = i;
                    commentType = CommentType::Multi;

                    ++i;
                }
                else if (commentType == CommentType::Multi && currentChar == '*' && nextChar == '/') {

                    ++i;

                    commentType = CommentType::None;
                    begin = i + 1;
                }
            }

            ret.append(str, begin, str.length() - begin);
            ret.shrink_to_fit();
            return ret;
        }
    }

    const Configuration& Configuration::instance()
    {
        static Configuration s_instance;
        return s_instance;
    }

    void Configuration::print(std::ostream& out) const
    {
        out << m_json.dump(4);
    }

    const nlohmann::json& Configuration::defaultJson()
    {
        static const nlohmann::json s_defaultJson =
            R"({
"ext" : {
    "default_thread_pool" : {
        "threads" : 8
    },

    "max_concurrent_open_pooled_files" : 256,
    "max_concurrent_open_unpooled_files" : 128,

    "merge" : {
        "max_batch_size" : 192,

        "max_output_buffer_size_multiplier" : 8
    },

    "equal_range" : {
        "max_random_read_size" : "32KiB"
    },

    "index" : {
        "builder_buffer_size" : "8MiB"
    }
},

"persistence" : {
    "header_writer_memory" : "16MiB",

    "db_alpha" : {
        "index_granularity" : 1024,
        "max_merge_buffer_size" : "1GiB",
        "pgn_parser_memory" : "4MiB"
    },

    "db_beta" : {
        "index_granularity" : 1024,
        "max_merge_buffer_size" : "1GiB",
        "pgn_parser_memory" : "4MiB"
    },

    "db_delta" : {
        "index_granularity" : 1024,
        "max_merge_buffer_size" : "1GiB",
        "pgn_parser_memory" : "4MiB"
    },

    "db_epsilon" : {
        "index_granularity" : 1024,
        "max_merge_buffer_size" : "1GiB",
        "pgn_parser_memory" : "4MiB",
        "bcgn_parser_memory" : "4MiB"
    }
},

"command_line_app" : {
    "import_memory" : "2GiB",
    "pgn_parser_memory" : "4MiB",
    "bcgn_parser_memory" : "4MiB",
    "dump" : {
        "import_memory" : "2GiB",
        "pgn_parser_memory" : "4MiB",
        "bcgn_parser_memory" : "4MiB",
        "max_merge_buffer_size" : "1GiB"
    }
},

"console_app" : {
    "import_memory" : "2GiB",
    "pgn_parser_memory" : "4MiB",
    "bcgn_parser_memory" : "4MiB"
}
})"_json;

        return s_defaultJson;
    }

    Configuration::Configuration()
    {
        std::ifstream t("cfg/config.json");
        std::string str(
            (std::istreambuf_iterator<char>(t)),
            std::istreambuf_iterator<char>()
        );

        m_json = defaultJson();

        try
        {
            if (!str.empty())
            {
                str = detail::stripComments(str);
                m_json.merge_patch(nlohmann::json::parse(str));
            }
        }
        catch (...)
        {
            Logger::instance().logError("Invalid configuration file. Keeping default.");
        }
    }
}
