#pragma once

#include "EnumMap.h"
#include "External.h"
#include "GameClassification.h"
#include "Pgn.h"
#include "PositionSignature.h"
#include "StorageHeader.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <execution>
#include <filesystem>
#include <future>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace persistence
{
    struct LocalStorageFormatEntry
    {
        LocalStorageFormatEntry() = default;

        LocalStorageFormatEntry(const Position& pos, std::uint32_t gameIdx) :
            m_positionSignature(pos),
            m_gameIdx(gameIdx)
        {
        }

        LocalStorageFormatEntry(const LocalStorageFormatEntry&) = default;
        LocalStorageFormatEntry(LocalStorageFormatEntry&&) = default;
        LocalStorageFormatEntry& operator=(const LocalStorageFormatEntry&) = default;
        LocalStorageFormatEntry& operator=(LocalStorageFormatEntry&&) = default;

        [[nodiscard]] friend bool operator<(const LocalStorageFormatEntry& lhs, const LocalStorageFormatEntry& rhs) noexcept
        {
            return lhs.m_positionSignature < rhs.m_positionSignature;
        }

        [[nodiscard]] std::uint32_t hashMod(std::uint32_t d) const
        {
            return m_positionSignature.hash()[0] % d;
        }

    private:
        PositionSignature m_positionSignature;
        std::uint32_t m_gameIdx;
    };

    static_assert(sizeof(LocalStorageFormatEntry) == 20);

    struct LocalStorageFormatFile
    {
        LocalStorageFormatFile(std::filesystem::path path) :
            m_entries(std::move(path)),
            m_id(std::stoi(m_entries.path().filename().string()))
        {
        }

        LocalStorageFormatFile(ext::ImmutableSpan<LocalStorageFormatEntry>&& entries) :
            m_entries(std::move(entries)),
            m_id(std::stoi(m_entries.path().filename().string()))
        {
        }

        [[nodiscard]] friend bool operator<(const LocalStorageFormatFile& lhs, const LocalStorageFormatFile& rhs) noexcept
        {
            return lhs.m_id < rhs.m_id;
        }

        [[nodiscard]] int id() const
        {
            return m_id;
        }

    private:
        ext::ImmutableSpan<LocalStorageFormatEntry> m_entries;
        int m_id;
    };

    struct LocalStorageFormatFutureFile
    {
        LocalStorageFormatFutureFile(std::future<void>&& future, std::filesystem::path path) :
            m_future(std::move(future)),
            m_path(std::move(path)),
            m_id(std::stoi(m_path.filename().string()))
        {
        }

        [[nodiscard]] friend bool operator<(const LocalStorageFormatFutureFile& lhs, const LocalStorageFormatFutureFile& rhs) noexcept
        {
            return lhs.m_id < rhs.m_id;
        }

        [[nodiscard]] int id() const
        {
            return m_id;
        }

        [[nodiscard]] LocalStorageFormatFile get()
        {
            m_future.get();
            return { m_path };
        }

    private:
        std::future<void> m_future;
        std::filesystem::path m_path;
        int m_id;
    };

    struct LocalStorageFormatAsyncStorePipeline
    {
    private:
        struct Job
        {
            Job(std::filesystem::path path, std::vector<LocalStorageFormatEntry>&& buffer, std::promise<void>&& promise) :
                path(std::move(path)),
                buffer(std::move(buffer)),
                promise(std::move(promise))
            {
            }

            std::filesystem::path path;
            std::vector<LocalStorageFormatEntry> buffer;
            std::promise<void> promise;
        };

    public:
        LocalStorageFormatAsyncStorePipeline(std::vector<std::vector<LocalStorageFormatEntry>>&& buffers) :
            m_sortingThreadFinished(false),
            m_writingThreadFinished(false),
            m_sortingThread([this]() { runSortingThread(); }),
            m_writingThread([this]() { runWritingThread(); })
        {
            for (auto&& buffer : buffers)
            {
                m_bufferQueue.emplace(std::move(buffer));
            }
        }

        LocalStorageFormatAsyncStorePipeline(const LocalStorageFormatAsyncStorePipeline&) = delete;

        ~LocalStorageFormatAsyncStorePipeline()
        {
            waitForCompletion();
        }

        std::future<void> scheduleUnordered(const std::filesystem::path& path, std::vector<LocalStorageFormatEntry>&& elements)
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            std::promise<void> promise;
            std::future<void> future = promise.get_future();
            m_sortQueue.emplace(path, std::move(elements), std::move(promise));

            lock.unlock();
            m_sortQueueNotEmpty.notify_one();

            return future;
        }

        std::future<void> scheduleOrdered(const std::filesystem::path& path, std::vector<LocalStorageFormatEntry>&& elements)
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            std::promise<void> promise;
            std::future<void> future = promise.get_future();
            m_sortQueue.emplace(path, std::move(elements), std::move(promise));

            lock.unlock();
            m_writeQueueNotEmpty.notify_one();

            return future;
        }

        std::vector<LocalStorageFormatEntry> getEmptyBuffer()
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            m_bufferQueueNotEmpty.wait(lock, [this]() {return !m_bufferQueue.empty(); });

            auto buffer = std::move(m_bufferQueue.front());
            m_bufferQueue.pop();

            return buffer;
        }

        void waitForCompletion()
        {
            if (!m_sortingThreadFinished.load())
            {
                m_sortingThreadFinished.store(true);
                m_sortQueueNotEmpty.notify_one();
                m_sortingThread.join();

                m_writingThreadFinished.store(true);
                m_writeQueueNotEmpty.notify_one();
                m_writingThread.join();
            }
        }

    private:
        std::queue<Job> m_sortQueue;
        std::queue<Job> m_writeQueue;
        std::queue<std::vector<LocalStorageFormatEntry>> m_bufferQueue;

        std::condition_variable m_sortQueueNotEmpty;
        std::condition_variable m_writeQueueNotEmpty;
        std::condition_variable m_bufferQueueNotEmpty;

        std::mutex m_mutex;

        std::atomic_bool m_sortingThreadFinished;
        std::atomic_bool m_writingThreadFinished;

        std::thread m_sortingThread;
        std::thread m_writingThread;

        void runSortingThread()
        {
            for (;;)
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_sortQueueNotEmpty.wait(lock, [this]() {return !m_sortQueue.empty() || m_sortingThreadFinished.load(); });

                if (m_sortQueue.empty())
                {
                    return;
                }

                Job job = std::move(m_sortQueue.front());
                m_sortQueue.pop();

                lock.unlock();

                std::stable_sort(job.buffer.begin(), job.buffer.end());

                lock.lock();
                m_writeQueue.emplace(std::move(job));
                lock.unlock();

                m_writeQueueNotEmpty.notify_one();
            }
        }

        void runWritingThread()
        {
            for (;;)
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_writeQueueNotEmpty.wait(lock, [this]() {return !m_writeQueue.empty() || m_writingThreadFinished.load(); });

                if (m_writeQueue.empty())
                {
                    return;
                }

                Job job = std::move(m_writeQueue.front());
                m_writeQueue.pop();

                lock.unlock();

                ext::writeFile(job.path, job.buffer.data(), job.buffer.size());
                job.buffer.clear();

                lock.lock();
                m_bufferQueue.emplace(std::move(job.buffer));
                lock.unlock();

                m_bufferQueueNotEmpty.notify_one();
                job.promise.set_value();
            }
        }
    };

    struct LocalStorageFormatPartition
    {
        LocalStorageFormatPartition() = default;

        LocalStorageFormatPartition(std::filesystem::path path)
        {
            setPath(std::move(path));
        }

        void setPath(std::filesystem::path path)
        {
            m_path = std::move(path);
            std::filesystem::create_directories(m_path);

            discoverFiles();
        }

        // data has to be sorted in ascending order
        void storeOrdered(const LocalStorageFormatEntry* data, std::size_t count)
        {
            ASSERT(!m_path.empty());

            auto path = nextPath();
            ext::writeFile(path, data, count);
            m_files.emplace_back(path);
        }

        // entries have to be sorted in ascending order
        void storeOrdered(const std::vector<LocalStorageFormatEntry>& entries)
        {
            storeOrdered(entries.data(), entries.size());
        }

        // entries have to be sorted in ascending order
        void storeUnordered(LocalStorageFormatAsyncStorePipeline& pipeline, std::vector<LocalStorageFormatEntry>&& entries)
        {
            ASSERT(!m_path.empty());

            auto path = nextPath();
            m_futureFiles.emplace_back(pipeline.scheduleUnordered(path, std::move(entries)), path);
        }

        // collects future files
        void updateFiles()
        {
            for (auto& f : m_futureFiles)
            {
                m_files.emplace_back(f.get());
            }
            m_futureFiles.clear();
            m_futureFiles.shrink_to_fit();
        }

    private:
        std::filesystem::path m_path;
        std::vector<LocalStorageFormatFile> m_files;
        std::vector<LocalStorageFormatFutureFile> m_futureFiles;

        [[nodiscard]] int nextId() const
        {
            if (!m_futureFiles.empty())
            {
                return m_futureFiles.back().id() + 1;
            }

            if (!m_files.empty())
            {
                return m_files.back().id() + 1;
            }

            return 0;
        }

        [[nodiscard]] std::filesystem::path nextPath() const
        {
            return m_path / std::to_string(nextId());
        }

        void discoverFiles()
        {
            m_files.clear();

            for (auto& entry : std::filesystem::directory_iterator(m_path))
            {
                if (!entry.is_regular_file())
                {
                    continue;
                }

                m_files.emplace_back(entry.path());
            }

            std::sort(m_files.begin(), m_files.end());
        }
    };

    struct LocalStorageFormat
    {
    private:
        static inline const std::string m_name = "local";

        static constexpr std::uint32_t m_numPartitionsByHashModulo = 4;

        template <typename T>
        using PerDirectory = EnumMap2<GameLevel, GameResult, std::array<T, m_numPartitionsByHashModulo>>;

        template <typename T>
        using PerGameLevel = EnumMap<GameResult, std::array<T, m_numPartitionsByHashModulo>>;

        using FilesStorageType = PerDirectory<LocalStorageFormatPartition>;

        using DirectoryPathsStorageType = PerDirectory<std::filesystem::path>;

        static inline const EnumMap<GameLevel, std::filesystem::path> m_pathByGameLevel = {
            "human",
            "engine",
            "server"
        };

        static inline const EnumMap<GameResult, std::filesystem::path> m_pathByGameResult = {
            "w",
            "l",
            "d"
        };

        static constexpr int m_totalNumDirectories =
            cardinality<GameLevel>()
            * cardinality<GameResult>()
            * m_numPartitionsByHashModulo;

        static constexpr std::size_t m_headerBufferMemory = 4 * 1024 * 1024;

    public:
        struct ImportStats
        {
            std::size_t numGames = 0;
            std::size_t numSkippedGames = 0; // We skip games with an unknown result.
            std::size_t numPositions = 0;
        };

        LocalStorageFormat(std::filesystem::path path) :
            m_path(path),
            m_header(path, m_headerBufferMemory)
        {
            initializePartitions();
        }

        [[nodiscard]] const std::string& name() const
        {
            return m_name;
        }

        // returns number of positions added
        ImportStats importPgns(const std::vector<std::filesystem::path>& paths, GameLevel level, std::size_t memory)
        {
            constexpr int numAdditionalBuffers = cardinality<GameResult>() * m_numPartitionsByHashModulo;

            const std::size_t bucketSize = 
                ext::numObjectsPerBufferUnit<LocalStorageFormatEntry>(
                    memory, 
                    cardinality<GameResult>() * m_numPartitionsByHashModulo + numAdditionalBuffers
                );

            PerGameLevel<std::vector<LocalStorageFormatEntry>> entries;
            forEach(entries, [bucketSize](auto& bucket, GameResult result, int idx) {
                bucket.reserve(bucketSize);
            });
            std::vector<std::vector<LocalStorageFormatEntry>> additionalBuffers;
            for (int i = 0; i < numAdditionalBuffers; ++i)
            {
                additionalBuffers.emplace_back();
                additionalBuffers.back().reserve(bucketSize);
            }

            LocalStorageFormatAsyncStorePipeline pipeline(std::move(additionalBuffers));

            ImportStats stats{};
            for (auto& path : paths)
            {
                pgn::LazyPgnFileReader fr(path);
                if (!fr.isOpen())
                {
                    std::cout << "Failed to open file " << path << '\n';
                    break;
                }

                for (auto& game : fr)
                {
                    const pgn::GameResult pgnResult = game.result();
                    if (pgnResult == pgn::GameResult::Unknown)
                    {
                        stats.numSkippedGames += 1;
                        continue;
                    }

                    const GameResult result = convertResult(pgnResult);

                    const std::uint32_t gameIdx = m_header.nextGameId();

                    std::size_t numPositionsInGame = 0;
                    for (auto& pos : game.positions())
                    {
                        LocalStorageFormatEntry entry(pos, gameIdx);
                        const int partitionIdx = entry.hashMod(m_numPartitionsByHashModulo);
                        auto& bucket = entries[result][partitionIdx];
                        bucket.emplace_back(entry);
                        numPositionsInGame += 1;

                        if (bucket.size() == bucketSize)
                        {
                            store(pipeline, bucket, level, result, partitionIdx);
                            bucket.clear();
                        }
                    }

                    ASSERT(numPositionsInGame > 0);

                    const std::uint32_t actualGameIdx = m_header.addGame(game, static_cast<std::uint16_t>(numPositionsInGame - 1u));
                    ASSERT(actualGameIdx == gameIdx);

                    stats.numGames += 1;
                    stats.numPositions += numPositionsInGame;
                }
            }

            forEach(entries, [this, &pipeline, level](auto& bucket, GameResult result, int idx) {
                store(pipeline, bucket, level, result, idx);
            });

            pipeline.waitForCompletion();
            forEach(entries, [this, level](auto& bucket, GameResult result, int idx) {
                m_files[level][result][idx].updateFiles();
            });

            return stats;
        }

        ImportStats importPgn(const std::filesystem::path& path, GameLevel level, std::size_t memory)
        {
            return importPgns({ path }, level, memory);
        }

    private:
        std::filesystem::path m_path;

        Header m_header;
        FilesStorageType m_files;

        // this is nontrivial to do in the constructor initializer list
        void initializePartitions()
        {
            for (const auto& level : values<GameLevel>())
            {
                const std::filesystem::path levelPath = m_pathByGameLevel[level];
                for (const auto& result : values<GameResult>())
                {
                    const std::filesystem::path resultPath = levelPath / m_pathByGameResult[result];
                    for (std::uint32_t partitionIdx = 0; partitionIdx < m_numPartitionsByHashModulo; ++partitionIdx)
                    {
                        const std::filesystem::path partitionPath = resultPath / std::to_string(partitionIdx);
                        m_files[level][result][partitionIdx].setPath(m_path / partitionPath);
                    }
                }
            }
        }

        void store(LocalStorageFormatAsyncStorePipeline& pipeline, std::vector<LocalStorageFormatEntry>& entries, GameLevel level, GameResult result, int partitionIdx)
        {
            auto newBuffer = pipeline.getEmptyBuffer();
            entries.swap(newBuffer);
            m_files[level][result][partitionIdx].storeUnordered(pipeline, std::move(newBuffer));
        }

        template <typename T, typename FuncT>
        static void forEach(PerDirectory<T>& data, FuncT&& f)
        {
            for (const auto& level : values<GameLevel>())
            {
                for (const auto& result : values<GameResult>())
                {
                    for (std::uint32_t partitionIdx = 0; partitionIdx < m_numPartitionsByHashModulo; ++partitionIdx)
                    {
                        f(data[level][result][partitionIdx], level, result, partitionIdx);
                    }
                }
            }
        }

        template <typename T, typename FuncT>
        static void forEach(PerGameLevel<T>& data, FuncT&& f)
        {
            for (const auto& result : values<GameResult>())
            {
                for (std::uint32_t partitionIdx = 0; partitionIdx < m_numPartitionsByHashModulo; ++partitionIdx)
                {
                    f(data[result][partitionIdx], result, partitionIdx);
                }
            }
        }

        [[nodiscard]] static inline GameResult convertResult(pgn::GameResult res)
        {
            ASSERT(res != pgn::GameResult::Unknown);

            return static_cast<GameResult>(static_cast<int>(res));
        }
    };
}