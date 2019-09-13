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
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace persistence
{
    namespace local
    {
        struct Entry
        {
            Entry() = default;

            Entry(const Position& pos, std::uint32_t gameIdx) :
                m_positionSignature(pos),
                m_gameIdx(gameIdx)
            {
            }

            Entry(const Entry&) = default;
            Entry(Entry&&) = default;
            Entry& operator=(const Entry&) = default;
            Entry& operator=(Entry&&) = default;

            [[nodiscard]] friend bool operator<(const Entry& lhs, const Entry& rhs) noexcept
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

        static_assert(sizeof(Entry) == 20);

        struct File
        {
            File(std::filesystem::path path) :
                m_entries(std::move(path)),
                m_id(std::stoi(m_entries.path().filename().string()))
            {
            }

            File(ext::ImmutableSpan<Entry>&& entries) :
                m_entries(std::move(entries)),
                m_id(std::stoi(m_entries.path().filename().string()))
            {
            }

            [[nodiscard]] friend bool operator<(const File& lhs, const File& rhs) noexcept
            {
                return lhs.m_id < rhs.m_id;
            }

            [[nodiscard]] std::uint32_t id() const
            {
                return m_id;
            }

        private:
            ext::ImmutableSpan<Entry> m_entries;
            std::uint32_t m_id;
        };

        struct FutureFile
        {
            FutureFile(std::future<void>&& future, std::filesystem::path path) :
                m_future(std::move(future)),
                m_path(std::move(path)),
                m_id(std::stoi(m_path.filename().string()))
            {
            }

            [[nodiscard]] friend bool operator<(const FutureFile& lhs, const FutureFile& rhs) noexcept
            {
                return lhs.m_id < rhs.m_id;
            }

            [[nodiscard]] std::uint32_t id() const
            {
                return m_id;
            }

            [[nodiscard]] File get()
            {
                m_future.get();
                return { m_path };
            }

        private:
            std::future<void> m_future;
            std::filesystem::path m_path;
            std::uint32_t m_id;
        };

        struct AsyncStorePipeline
        {
        private:
            struct Job
            {
                Job(std::filesystem::path path, std::vector<Entry>&& buffer, std::promise<void>&& promise) :
                    path(std::move(path)),
                    buffer(std::move(buffer)),
                    promise(std::move(promise))
                {
                }

                std::filesystem::path path;
                std::vector<Entry> buffer;
                std::promise<void> promise;
            };

        public:
            AsyncStorePipeline(std::vector<std::vector<Entry>>&& buffers, std::size_t numSortingThreads = 1) :
                m_sortingThreadFinished(false),
                m_writingThreadFinished(false),
                m_writingThread([this]() { runWritingThread(); })
            {
                ASSERT(numSortingThreads >= 1);
                ASSERT(!buffers.empty());

                m_sortingThreads.reserve(numSortingThreads);
                for (std::size_t i = 0; i < numSortingThreads; ++i)
                {
                    m_sortingThreads.emplace_back([this]() { runSortingThread(); });
                }

                for (auto&& buffer : buffers)
                {
                    m_bufferQueue.emplace(std::move(buffer));
                }
            }

            AsyncStorePipeline(const AsyncStorePipeline&) = delete;

            ~AsyncStorePipeline()
            {
                waitForCompletion();
            }

            std::future<void> scheduleUnordered(const std::filesystem::path& path, std::vector<Entry>&& elements)
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                std::promise<void> promise;
                std::future<void> future = promise.get_future();
                m_sortQueue.emplace(path, std::move(elements), std::move(promise));

                lock.unlock();
                m_sortQueueNotEmpty.notify_one();

                return future;
            }

            std::future<void> scheduleOrdered(const std::filesystem::path& path, std::vector<Entry>&& elements)
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                std::promise<void> promise;
                std::future<void> future = promise.get_future();
                m_sortQueue.emplace(path, std::move(elements), std::move(promise));

                lock.unlock();
                m_writeQueueNotEmpty.notify_one();

                return future;
            }

            std::vector<Entry> getEmptyBuffer()
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
                    for (auto& th : m_sortingThreads)
                    {
                        th.join();
                    }

                    m_writingThreadFinished.store(true);
                    m_writeQueueNotEmpty.notify_one();
                    m_writingThread.join();
                }
            }

        private:
            std::queue<Job> m_sortQueue;
            std::queue<Job> m_writeQueue;
            std::queue<std::vector<Entry>> m_bufferQueue;

            std::condition_variable m_sortQueueNotEmpty;
            std::condition_variable m_writeQueueNotEmpty;
            std::condition_variable m_bufferQueueNotEmpty;

            std::mutex m_mutex;

            std::atomic_bool m_sortingThreadFinished;
            std::atomic_bool m_writingThreadFinished;

            std::vector<std::thread> m_sortingThreads;
            std::thread m_writingThread;

            void runSortingThread()
            {
                for (;;)
                {
                    std::unique_lock<std::mutex> lock(m_mutex);
                    m_sortQueueNotEmpty.wait(lock, [this]() {return !m_sortQueue.empty() || m_sortingThreadFinished.load(); });

                    if (m_sortQueue.empty())
                    {
                        lock.unlock();
                        m_sortQueueNotEmpty.notify_one();
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
                        lock.unlock();
                        m_writeQueueNotEmpty.notify_one();
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

        struct Partition
        {
            Partition() = default;

            Partition(std::filesystem::path path)
            {
                ASSERT(!path.empty());

                setPath(std::move(path));
            }

            void setPath(std::filesystem::path path)
            {
                ASSERT(m_futureFiles.empty());

                m_path = std::move(path);
                std::filesystem::create_directories(m_path);

                discoverFiles();
            }

            // data has to be sorted in ascending order
            void storeOrdered(const Entry* data, std::size_t count)
            {
                ASSERT(!m_path.empty());

                auto path = nextPath();
                ext::writeFile(path, data, count);
                m_files.emplace_back(path);
            }

            // entries have to be sorted in ascending order
            void storeOrdered(const std::vector<Entry>& entries)
            {
                storeOrdered(entries.data(), entries.size());
            }

            // Uses the passed id.
            // It is required that the file with this id doesn't exist already.
            void storeUnordered(AsyncStorePipeline& pipeline, std::vector<Entry>&& entries, std::uint32_t id)
            {
                ASSERT(!m_path.empty());

                std::unique_lock<std::mutex>(m_mutex);
                auto path = pathForId(id);
                m_futureFiles.emplace(pipeline.scheduleUnordered(path, std::move(entries)), path);
            }

            void storeUnordered(AsyncStorePipeline& pipeline, std::vector<Entry>&& entries)
            {
                storeUnordered(pipeline, std::move(entries), nextId());
            }

            void collectFutureFiles()
            {
                while (!m_futureFiles.empty())
                    m_files.emplace_back(
                        std::move(
                            m_futureFiles.extract(
                                m_futureFiles.begin()
                            )
                            .value()
                            .get()
                        )
                    );
                m_futureFiles.clear();
            }

            [[nodiscard]] std::uint32_t nextId() const
            {
                if (!m_futureFiles.empty())
                {
                    return std::prev(m_futureFiles.end())->id() + 1;
                }

                if (!m_files.empty())
                {
                    return m_files.back().id() + 1;
                }

                return 0;
            }

        private:
            std::filesystem::path m_path;
            std::vector<File> m_files;

            // We store it in a set because the we can change insertion
            // order through forcing ids. It's easier to keep it
            // ordered like that.
            std::set<FutureFile> m_futureFiles;

            std::mutex m_mutex;

            [[nodiscard]] std::filesystem::path pathForId(std::uint32_t id) const
            {
                return m_path / std::to_string(id);
            }

            [[nodiscard]] std::filesystem::path nextPath() const
            {
                return pathForId(nextId());
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

        template <typename T>
        std::vector<std::vector<T>> createBuffers(std::size_t numBuffers, std::size_t size)
        {
            ASSERT(size > 0);

            std::vector<std::vector<T>> buffers;
            buffers.resize(numBuffers);
            for (auto& buffer : buffers)
            {
                buffer.reserve(size);
            }
            return buffers;
        }

        struct ImportStats
        {
            std::size_t numGames = 0;
            std::size_t numSkippedGames = 0; // We skip games with an unknown result.
            std::size_t numPositions = 0;

            ImportStats& operator+=(const ImportStats& rhs)
            {
                numGames += rhs.numGames;
                numSkippedGames += rhs.numSkippedGames;
                numPositions += rhs.numPositions;

                return *this;
            }
        };

        using PgnFilePath = std::filesystem::path;
        using PgnFilePaths = std::vector<std::filesystem::path>;

        struct PgnFile
        {
            PgnFile(std::filesystem::path path, GameLevel level) :
                m_path(std::move(path)),
                m_level(level)
            {
            }

            const auto& path() const &
            {
                return m_path;
            }

            PgnFilePath path() &&
            {
                return std::move(m_path);
            }

            GameLevel level() const
            {
                return m_level;
            }

        private:
            PgnFilePath m_path;
            GameLevel m_level;
        };

        using PgnFiles = std::vector<PgnFile>;

        EnumMap<GameLevel, PgnFilePaths> partitionPathsByLevel(PgnFiles files)
        {
            EnumMap<GameLevel, PgnFilePaths> partitioned;
            for (auto&& file : files)
            {
                partitioned[file.level()].emplace_back(std::move(file).path());
            }
            return partitioned;
        }

        struct Database
        {
        private:
            static inline const std::string m_name = "local";

            static constexpr std::uint32_t m_numPartitionsByHashModulo = 4;

            template <typename T>
            using PerPartition = EnumMap2<GameLevel, GameResult, std::array<T, m_numPartitionsByHashModulo>>;

            template <typename T>
            using PerPartitionWithSpecificGameLevel = EnumMap<GameResult, std::array<T, m_numPartitionsByHashModulo>>;

            using PartitionStorageType = PerPartition<Partition>;

            using PartitionPathsStorageType = PerPartition<std::filesystem::path>;

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

            static constexpr std::size_t m_totalNumDirectories =
                cardinality<GameLevel>()
                * cardinality<GameResult>()
                * m_numPartitionsByHashModulo;

            // TODO: maybe make it configurable, though it doesn't need to be big.
            static constexpr std::size_t m_pgnParserMemory = 16ull * 1024ull * 1024ull;

        public:

            Database(std::filesystem::path path, std::size_t headerBufferMemory) :
                m_path(path),
                m_header(path, headerBufferMemory)
            {
                initializePartitions();
            }

            [[nodiscard]] const std::string& name() const
            {
                return m_name;
            }

            ImportStats importPgns(
                std::execution::parallel_policy,
                const PgnFiles& pgns,
                std::size_t memory
            )
            {
                constexpr std::size_t numSortingThreads = 3;

                if (pgns.empty())
                {
                    return {};
                }

                auto pathsByLevel = partitionPathsByLevel(pgns);
                const auto numDifferentLevels = std::count_if(
                    pathsByLevel.begin(),
                    pathsByLevel.end(),
                    [](auto& v) { return v.size() > 0; }
                );

                const std::size_t numBuffers = cardinality<GameResult>() * m_numPartitionsByHashModulo * numDifferentLevels;
                const std::size_t numAdditionalBuffers = numBuffers;

                const std::size_t bucketSize =
                    ext::numObjectsPerBufferUnit<Entry>(
                        memory,
                        numBuffers + numAdditionalBuffers
                        );

                ASSERT(bucketSize > 0);

                AsyncStorePipeline pipeline(
                    createBuffers<Entry>(numBuffers + numAdditionalBuffers, bucketSize),
                    numSortingThreads
                );

                std::vector<std::future<ImportStats>> stats;
                stats.reserve(numDifferentLevels);
                for (auto level : values<GameLevel>())
                {
                    if (pathsByLevel[level].empty())
                    {
                        continue;
                    }

                    stats.emplace_back(
                        std::async(
                            std::launch::async,
                            [&, level]() {
                                return importPgnsImpl(pipeline, pathsByLevel[level], level);
                            }
                        )
                    );
                }

                ImportStats statsTotal{};
                for (auto& f : stats)
                {
                    statsTotal += f.get();
                }

                pipeline.waitForCompletion();
                discoverFutureFiles();

                return statsTotal;
            }

            ImportStats importPgns(
                std::execution::parallel_unsequenced_policy,
                const PgnFiles& pgns,
                std::size_t memory,
                std::size_t numThreads = std::thread::hardware_concurrency()
            )
            {
                if (pgns.empty())
                {
                    return {};
                }

                if (numThreads <= 2)
                {
                    return importPgns(std::execution::seq, pgns, memory);
                }

                const std::size_t numSortingThreads = numThreads / 2;
                const std::size_t numWorkerThreads = numThreads - numSortingThreads;

                auto pathsByLevel = partitionPathsByLevel(pgns);

                const std::size_t numBuffers = cardinality<GameResult>() * m_numPartitionsByHashModulo * numWorkerThreads;

                const std::size_t numAdditionalBuffers = numBuffers;

                const std::size_t bucketSize =
                    ext::numObjectsPerBufferUnit<Entry>(
                        memory,
                        numBuffers + numAdditionalBuffers
                        );

                AsyncStorePipeline pipeline(
                    createBuffers<Entry>(numBuffers + numAdditionalBuffers, bucketSize),
                    numSortingThreads
                );

                // We do different game levels sequentially because
                // importing is parallelized on file granularity.
                ImportStats stats;
                for (auto level : values<GameLevel>())
                {
                    if (pathsByLevel[level].empty())
                    {
                        continue;
                    }

                    stats += importPgnsImpl(std::execution::par_unseq, pipeline, pathsByLevel[level], level, bucketSize, numWorkerThreads);
                }

                pipeline.waitForCompletion();
                discoverFutureFiles();

                return stats;
            }

            ImportStats importPgns(
                std::execution::sequenced_policy,
                const PgnFiles& pgns,
                std::size_t memory
            )
            {
                constexpr std::size_t numSortingThreads = 1;

                if (pgns.empty())
                {
                    return {};
                }

                auto pathsByLevel = partitionPathsByLevel(pgns);

                const std::size_t numBuffers = cardinality<GameResult>() * m_numPartitionsByHashModulo;

                const std::size_t numAdditionalBuffers = numBuffers;

                const std::size_t bucketSize =
                    ext::numObjectsPerBufferUnit<Entry>(
                        memory,
                        numBuffers + numAdditionalBuffers
                        );

                AsyncStorePipeline pipeline(
                    createBuffers<Entry>(numBuffers + numAdditionalBuffers, bucketSize),
                    numSortingThreads
                );

                ImportStats statsTotal{};
                for (auto level : values<GameLevel>())
                {
                    if (pathsByLevel[level].empty())
                    {
                        continue;
                    }

                    statsTotal += importPgnsImpl(pipeline, pathsByLevel[level], level);
                }

                pipeline.waitForCompletion();
                discoverFutureFiles();

                return statsTotal;
            }

            ImportStats importPgns(const PgnFiles& pgns, std::size_t memory)
            {
                return importPgns(std::execution::seq, pgns, memory);
            }

        private:
            std::filesystem::path m_path;

            Header m_header;
            PartitionStorageType m_partitions;

            void discoverFutureFiles()
            {
                forEach(m_partitions, [this](auto& bucket, GameLevel level, GameResult result, std::uint32_t idx) {
                    m_partitions[level][result][idx].collectFutureFiles();
                    });
            }

            ImportStats importPgnsImpl(
                AsyncStorePipeline& pipeline,
                const PgnFilePaths& paths,
                GameLevel level
            )
            {
                // create buffers
                PerPartitionWithSpecificGameLevel<std::vector<Entry>> buckets;
                forEach(buckets, [&](auto& bucket, GameResult result, std::uint32_t idx) {
                    bucket = pipeline.getEmptyBuffer();
                    });

                ImportStats stats{};
                for (auto& path : paths)
                {
                    pgn::LazyPgnFileReader fr(path, m_pgnParserMemory);
                    if (!fr.isOpen())
                    {
                        std::cerr << "Failed to open file " << path << '\n';
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

                        // NOTE: we cannot get plies after loading the positions
                        //       because this function may be called in parallel.
                        //       We have to rely on tags.
                        const std::uint32_t gameIdx = m_header.addGame(game);

                        std::size_t numPositionsInGame = 0;
                        for (auto& pos : game.positions())
                        {
                            Entry entry(pos, gameIdx);
                            const std::uint32_t partitionIdx = entry.hashMod(m_numPartitionsByHashModulo);
                            auto& bucket = buckets[result][partitionIdx];
                            bucket.emplace_back(entry);
                            numPositionsInGame += 1;

                            if (bucket.size() == bucket.capacity())
                            {
                                store(pipeline, bucket, level, result, partitionIdx);
                                bucket.clear();
                            }
                        }

                        ASSERT(numPositionsInGame > 0);

                        stats.numGames += 1;
                        stats.numPositions += numPositionsInGame;
                    }
                }

                // flush buffers and return them to the pipeline for later use
                forEach(buckets, [this, &pipeline, level](auto& bucket, GameResult result, std::uint32_t idx) {
                    store(pipeline, std::move(bucket), level, result, idx);
                    });

                return stats;
            }

            struct Block
            {
                typename std::vector<std::filesystem::path>::const_iterator begin;
                typename std::vector<std::filesystem::path>::const_iterator end;
                PerPartitionWithSpecificGameLevel<std::uint32_t> nextIds;
            };

            std::vector<Block> divideIntoBlocks(
                const PgnFilePaths& paths,
                GameLevel level,
                std::size_t bufferSize,
                std::size_t numBlocks
            )
            {
                constexpr std::size_t minPgnBytesPerMove = 4;

                // We compute the total size of the files
                std::vector<std::size_t> fileSizes;
                fileSizes.reserve(paths.size());
                std::size_t totalFileSize = 0;
                for (auto& path : paths)
                {
                    const std::size_t size = std::filesystem::file_size(path);
                    totalFileSize += size;
                    fileSizes.emplace_back(size);
                }

                // and try to divide it as equal as possible into exactly numBlocks blocks
                const std::size_t blockSizeThreshold = ext::ceilDiv(totalFileSize, numBlocks);

                std::vector<Block> blocks;
                blocks.reserve(numBlocks);
                {
                    // we prepare the next free file id for each partition
                    // and store just one global offset because we 
                    // don't know the distribution of the games
                    // and have to assume that all positions could go
                    // into one partition
                    std::uint32_t idOffset = 0;
                    PerPartitionWithSpecificGameLevel<std::uint32_t> baseNextIds{};
                    forEach(baseNextIds, [&](auto& nextId, GameResult result, std::uint32_t idx) {
                        nextId = m_partitions[level][result][idx].nextId();
                        });

                    std::size_t blockSize = 0;
                    auto start = paths.begin();
                    for (int i = 0; i < paths.size(); ++i)
                    {
                        auto& path = paths[i];

                        blockSize += fileSizes[i];

                        if (blockSize >= blockSizeThreshold)
                        {
                            // here we apply the offset
                            PerPartitionWithSpecificGameLevel<std::uint32_t> nextIds;
                            forEach(nextIds, [&](auto& nextId, GameResult result, std::uint32_t idx) {
                                nextId = baseNextIds[result][idx] + idOffset;
                                });

                            // store the block of desired size
                            auto end = paths.begin() + i;
                            blocks.emplace_back(Block{ start, end, nextIds });
                            start = end;
                            idOffset += static_cast<std::uint32_t>(blockSize / (bufferSize * minPgnBytesPerMove)) + 1u;
                            blockSize = 0;
                        }
                    }

                    // if anything is left over we have to handle it here as in the
                    // loop we only handle full blocks; last one may be only partially full
                    if (start != paths.end())
                    {
                        PerPartitionWithSpecificGameLevel<std::uint32_t> nextIds;
                        forEach(nextIds, [&](auto& nextId, GameResult result, std::uint32_t idx) {
                            nextId = baseNextIds[result][idx] + idOffset;
                            });
                        blocks.emplace_back(Block{ start, paths.end(), nextIds });
                    }

                    ASSERT(blocks.size() == numBlocks);
                }

                return blocks;
            }

            ImportStats importPgnsImpl(
                std::execution::parallel_unsequenced_policy,
                AsyncStorePipeline& pipeline,
                const PgnFilePaths& paths,
                GameLevel level,
                std::size_t bufferSize,
                std::size_t numThreads
            )
            {
                auto blocks = divideIntoBlocks(paths, level, bufferSize, numThreads);

                // Here almost everything is as in the sequential algorithm.
                // Synchronization is handled in deeper layers.
                // We only have to force file ids (info kept in blocks) to
                // ensure proper order of resulting files.
                auto work = [&](const Block& block) {

                    PerPartitionWithSpecificGameLevel<std::vector<Entry>> entries;
                    forEach(entries, [&](auto& bucket, GameResult result, std::uint32_t idx) {
                        bucket = pipeline.getEmptyBuffer();
                        });

                    ImportStats stats{};
                    auto [begin, end, nextIds] = block;

                    for (; begin != end; ++begin)
                    {
                        auto& path = *begin;

                        pgn::LazyPgnFileReader fr(path, m_pgnParserMemory);
                        if (!fr.isOpen())
                        {
                            std::cerr << "Failed to open file " << path << '\n';
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

                            const std::uint32_t gameIdx = m_header.addGame(game);

                            std::size_t numPositionsInGame = 0;
                            for (auto& pos : game.positions())
                            {
                                Entry entry(pos, gameIdx);
                                const std::uint32_t partitionIdx = entry.hashMod(m_numPartitionsByHashModulo);
                                auto& bucket = entries[result][partitionIdx];
                                bucket.emplace_back(entry);
                                numPositionsInGame += 1;

                                if (bucket.size() == bufferSize)
                                {
                                    // Here we force the id and move to the next one.
                                    // This doesn't have to be atomic since we're the only
                                    // ones using this blocks and there is enough space left for
                                    // all files before the next already present id.
                                    auto& nextId = nextIds[result][partitionIdx];
                                    store(pipeline, bucket, level, result, partitionIdx, nextId++);
                                    bucket.clear();
                                }
                            }

                            ASSERT(numPositionsInGame > 0);

                            stats.numGames += 1;
                            stats.numPositions += numPositionsInGame;
                        }
                    }

                    // flush buffers and return them to the pipeline for later use
                    forEach(entries, [this, &pipeline, &nextIds = nextIds, level](auto& bucket, GameResult result, std::uint32_t idx) {
                        const std::uint32_t nextId = nextIds[result][idx];
                        store(pipeline, std::move(bucket), level, result, idx, nextId);
                        });

                    return stats;
                };

                // Schedule the work
                std::vector<std::future<ImportStats>> futureStats;
                futureStats.reserve(numThreads);
                for (int i = 0; i < numThreads - 1u; ++i)
                {
                    futureStats.emplace_back(std::async(std::launch::async, work, blocks[i]));
                }

                // and wait for completion, gather stats.
                // One worker is run in the main thread.
                ImportStats totalStats = work(blocks[numThreads - 1u]);
                for (auto& f : futureStats)
                {
                    totalStats += f.get();
                }

                return totalStats;
            }

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
                            m_partitions[level][result][partitionIdx].setPath(m_path / partitionPath);
                        }
                    }
                }
            }

            void store(
                AsyncStorePipeline& pipeline,
                std::vector<Entry>& entries,
                GameLevel level,
                GameResult result,
                std::uint32_t partitionIdx
            )
            {
                if (entries.empty())
                {
                    return;
                }

                auto newBuffer = pipeline.getEmptyBuffer();
                entries.swap(newBuffer);
                m_partitions[level][result][partitionIdx].storeUnordered(pipeline, std::move(newBuffer));
            }

            void store(
                AsyncStorePipeline& pipeline,
                std::vector<Entry>&& entries,
                GameLevel level,
                GameResult result,
                std::uint32_t partitionIdx
            )
            {
                if (entries.empty())
                {
                    return;
                }

                m_partitions[level][result][partitionIdx].storeUnordered(pipeline, std::move(entries));
            }

            void store(
                AsyncStorePipeline& pipeline,
                std::vector<Entry>& entries,
                GameLevel level,
                GameResult result,
                std::uint32_t partitionIdx,
                std::uint32_t id
            )
            {
                // Here we force the id - it's helpful when we need more control.
                // For example when access is not sequential.
                // It is required that the file with this id does not exist already.

                if (entries.empty())
                {
                    return;
                }

                auto newBuffer = pipeline.getEmptyBuffer();
                entries.swap(newBuffer);
                m_partitions[level][result][partitionIdx].storeUnordered(pipeline, std::move(newBuffer), id);
            }

            void store(
                AsyncStorePipeline& pipeline,
                std::vector<Entry>&& entries,
                GameLevel level,
                GameResult result,
                std::uint32_t partitionIdx,
                std::uint32_t id
            )
            {
                // Here we force the id - it's helpful when we need more control.
                // For example when access is not sequential.
                // It is required that the file with this id does not exist already.

                if (entries.empty())
                {
                    return;
                }

                m_partitions[level][result][partitionIdx].storeUnordered(pipeline, std::move(entries), id);
            }

            template <typename T, typename FuncT>
            static void forEach(PerPartition<T>& data, FuncT&& f)
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
            static void forEach(PerPartitionWithSpecificGameLevel<T>& data, FuncT&& f)
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
}