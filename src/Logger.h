#pragma once

#include <array>
#include <chrono>
#include <iostream>
#include <string>
#include <string_view>
#include <fstream>
#include <optional>
#include <filesystem>
#include <ctime>
#include <iomanip>

// member functions should not be called directly
// use LOG_* macros
struct Logger
{
    template <typename FuncT>
    struct Lazy : FuncT
    {
        using ResultType = decltype(std::declval<FuncT>());

        Lazy(FuncT func) :
            FuncT(std::move(func))
        {
        }

        [[nodiscard]] operator ResultType() const
        {
            return FuncT::operator()();
        }
    };

    enum Level
    {
        None,

        Debug,
        Info,
        Warning,
        Error,
        Fatal,
        Always
    };

    static Logger& instance()
    {
        static Logger s_logger;
        return s_logger;
    }

    [[nodiscard]] Level minLevel() const
    {
        return m_minLevel;
    }

    [[nodiscard]] bool isEnabled() const
    {
        return m_isEnabled;
    }

    void setOutputStream(std::ostream& s)
    {
        m_stream = &s;
    }

    void setOutputFile(const std::filesystem::path& path)
    {
        m_fileStream.emplace(path, std::ios_base::out | std::ios_base::app);
    }

    void resetOutputStream()
    {
        m_fileStream.reset();
    }

    template <typename... ArgTs>
    void log(Level level, ArgTs&& ... args)
    {
        if (!shouldBeLogged((level))) return;

        write("[", time(), " ", levelToString(level), "] ");
        write(std::forward<ArgTs>(args)...);
        write('\n');
    }

    template <typename... ArgTs>
    void logDebug(ArgTs&& ... args)
    {
        log(Debug, std::forward<ArgTs>(args)...);
    }

    template <typename... ArgTs>
    void logInfo(ArgTs&& ... args)
    {
        log(Info, std::forward<ArgTs>(args)...);
    }

    template <typename... ArgTs>
    void logWarning(ArgTs&& ... args)
    {
        log(Warning, std::forward<ArgTs>(args)...);
    }

    template <typename... ArgTs>
    void logError(ArgTs&& ... args)
    {
        log(Error, std::forward<ArgTs>(args)...);
    }

    template <typename... ArgTs>
    void logFatal(ArgTs&& ... args)
    {
        log(Fatal, std::forward<ArgTs>(args)...);
    }

    template <typename... ArgTs>
    void logAlways(ArgTs&& ... args)
    {
        log(Always, std::forward<ArgTs>(args)...);
    }

private:
    bool m_isEnabled;
    Level m_minLevel;
    std::ostream* m_stream;
    std::optional<std::ofstream> m_fileStream;

    Logger() :
        m_isEnabled(true),
        m_minLevel(Info),
        m_stream(&std::cout)
    {
        setOutputFile("log.txt");
    }

    [[nodiscard]] static decltype(auto) time()
    {
        auto time_now = std::time(nullptr);
        return std::put_time(std::localtime(&time_now), "%Y-%m-%d %OH:%OM:%OS");
    }

    [[nodiscard]] static constexpr const char* levelToString(Level level)
    {
        constexpr std::array<const char*, 7> v{
            "NONE",
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "FATAL",
            "ALWAYS"
        };

        return v[level];
    }

    template <typename... ArgTs>
    void write(ArgTs&&... args)
    {
        ((*m_stream) << ... << args);
        if (m_fileStream.has_value()) ((*m_fileStream) << ... << args);
    }

    [[nodiscard]] bool shouldBeLogged(Level level) const
    {
        return m_isEnabled && level >= m_minLevel;
    }
};
