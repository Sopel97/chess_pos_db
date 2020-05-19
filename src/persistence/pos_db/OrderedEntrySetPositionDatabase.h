#pragma once

#include "Database.h"
#include "Query.h"

#include "algorithm/Unsort.h"

#include "chess/Bcgn.h"
#include "chess/Chess.h"
#include "chess/GameClassification.h"
#include "chess/Position.h"
#include "chess/San.h"

#include "enum/EnumArray.h"

#include "external_storage/External.h"

#include "Configuration.h"
#include "Logger.h"

#include <algorithm>
#include <cstdint>
#include <execution>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace persistence
{
    namespace pos_db
    {
        namespace detail
        {
            template <typename T>
            struct HasEloDiff
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(std::int64_t{ std::declval<const C>().eloDiff() }, Yes{});

                template<typename> static constexpr No& Test(...);

            public:
                static constexpr bool value = sizeof(Test<T>(0)) == sizeof(Yes);
            };

            template <typename T>
            struct HasFirstGameIndex
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(std::uint32_t{ std::declval<const C>().firstGameIndex() }, Yes{});

                template<typename> static constexpr No& Test(...);

            public:
                static constexpr bool value = sizeof(Test<T>(0)) == sizeof(Yes);
            };

            template <typename T>
            struct HasLastGameIndex
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(std::uint32_t{ std::declval<const C>().lastGameIndex() }, Yes{});

                template<typename> static constexpr No& Test(...);

            public:
                static constexpr bool value = sizeof(Test<T>(0)) == sizeof(Yes);
            };

            template <typename T>
            struct HasFirstGameOffset
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(std::uint64_t{ std::declval<const C>().firstGameOffset() }, Yes{});

                template<typename> static constexpr No& Test(...);

            public:
                static constexpr bool value = sizeof(Test<T>(0)) == sizeof(Yes);
            };

            template <typename T>
            struct HasLastGameOffset
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(std::uint64_t{ std::declval<const C>().lastGameOffset() }, Yes{});

                template<typename> static constexpr No& Test(...);

            public:
                static constexpr bool value = sizeof(Test<T>(0)) == sizeof(Yes);
            };

            template <typename T>
            struct HasReverseMove
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(ReverseMove{ std::declval<const C>().reverseMove(std::declval<const Position>()) }, Yes{});

                template<typename> static constexpr No& Test(...);

            public:
                static constexpr bool value = sizeof(Test<T>(0)) == sizeof(Yes);
            };
        }

        template <
            typename KeyT,
            typename EntryT,
            typename TraitsT
        >
        struct OrderedEntrySetPositionDatabase final : persistence::Database
        {
            static_assert(std::is_trivially_copyable_v<EntryT>);

            static constexpr bool hasEloDiff = detail::HasEloDiff<EntryT>::value;
            static constexpr bool hasFirstGameIndex = detail::HasFirstGameIndex<EntryT>::value;
            static constexpr bool hasLastGameIndex = detail::HasLastGameIndex<EntryT>::value;
            static constexpr bool hasFirstGameOffset = detail::HasFirstGameOffset<EntryT>::value;
            static constexpr bool hasLastGameOffset = detail::HasLastGameOffset<EntryT>::value;
            static constexpr bool hasReverseMove = detail::HasReverseMove<EntryT>::value;

            static constexpr bool usesGameIndex = hasFirstGameIndex || hasLastGameIndex;
            static constexpr bool usesGameOffset = hasFirstGameOffset || hasLastGameOffset;

            static constexpr bool hasFirstGame = hasFirstGameIndex || hasFirstGameOffset;
            static constexpr bool hasLastGame = hasLastGameIndex || hasLastGameOffset;

            static constexpr bool hasGameHeaders = usesGameIndex || usesGameOffset;

            static constexpr const char* name = TraitsT::name;

            static_assert(!(usesGameIndex && usesGameOffset), "Only one type of game reference can be used.");

            using CompareEqualWithReverseMove = typename EntryT::CompareEqualWithReverseMove;
            using CompareEqualWithoutReverseMove = typename EntryT::CompareEqualWithoutReverseMove;
            using CompareEqualFull = typename EntryT::CompareEqualFull;

            using CompareLessWithReverseMove = typename EntryT::CompareLessWithReverseMove;
            using CompareLessWithoutReverseMove = typename EntryT::CompareLessWithoutReverseMove;
            using CompareLessFull = typename EntryT::CompareLessFull;

            using KeyCompareEqualWithReverseMove = typename KeyT::CompareEqualWithReverseMove;
            using KeyCompareEqualWithoutReverseMove = typename KeyT::CompareEqualWithoutReverseMove;
            using KeyCompareEqualFull = typename KeyT::CompareEqualFull;

            using KeyCompareLessWithReverseMove = typename KeyT::CompareLessWithReverseMove;
            using KeyCompareLessWithoutReverseMove = typename KeyT::CompareLessWithoutReverseMove;
            using KeyCompareLessFull = typename KeyT::CompareLessFull;

            using PositionStats = EnumArray<query::Select, EnumArray2<GameLevel, GameResult, EntryT>>;
            using RetractionsStats = std::map<
                ReverseMove,
                EnumArray2<GameLevel, GameResult, EntryT>,
                ReverseMoveCompareLess
            >;

            using Index = ext::RangeIndex<KeyT, typename EntryT::CompareLessWithoutReverseMove>;

            [[nodiscard]] static std::filesystem::path dataFilePathToIndexPath(const std::filesystem::path& dataFilePath)
            {
                auto cpy = dataFilePath;
                cpy += "_index";
                return cpy;
            }

            [[nodiscard]] static auto readIndexOfDataFile(const std::filesystem::path& dataFilePath)
            {
                auto indexPath = dataFilePathToIndexPath(dataFilePath);
                return Index(ext::readFile<typename Index::EntryType>(indexPath));
            }

            static void writeIndexOfDataFile(const std::filesystem::path& dataFilePath, const Index& index)
            {
                auto indexPath = dataFilePathToIndexPath(dataFilePath);
                (void)ext::writeFile<typename Index::EntryType>(indexPath, index.data(), index.size());
            }

            [[nodiscard]] static std::string fileIdToName(std::uint32_t id)
            {
                return std::to_string(id);
            }

            [[nodiscard]] static std::filesystem::path pathOfDataFileWithId(const std::filesystem::path& directory, std::uint32_t id)
            {
                return directory / fileIdToName(id);
            }

            [[nodiscard]] static std::uint32_t fileNameToId(const std::string& s)
            {
                return std::stoi(s);
            }

            [[nodiscard]] static std::uint32_t dataFilePathToId(const std::filesystem::path& dataFilePath)
            {
                return fileNameToId(dataFilePath.filename().string());
            }

            [[nodiscard]] static bool isPathOfIndex(const std::filesystem::path& path)
            {
                return path.filename().string().find("index") != std::string::npos;
            }

            template <typename T>
            [[nodiscard]] static std::vector<std::vector<T>> createBuffers(std::size_t numBuffers, std::size_t size)
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

            static inline std::size_t m_indexGranularity = cfg::g_config["persistence"][name]["index_granularity"].get<std::size_t>();
            static inline MemoryAmount m_mergeWriterBufferSize = cfg::g_config["persistence"][name]["merge_writer_buffer_size"].get<MemoryAmount>();

            struct File
            {
                File(const File&) = delete;
                File(File&&) noexcept = default;

                File& operator=(const File&) = delete;
                File& operator=(File&&) noexcept = default;

                File(std::filesystem::path path) :
                    m_entries({ ext::Pooled{}, std::move(path) }),
                    m_index(readIndexOfDataFile(m_entries.path())),
                    m_id(dataFilePathToId(m_entries.path()))
                {
                }

                File(ext::ImmutableSpan<EntryT>&& entries) :
                    m_entries(std::move(entries)),
                    m_index(readIndexOfDataFile(m_entries.path())),
                    m_id(dataFilePathToId(m_entries.path()))
                {
                }

                File(std::filesystem::path path, Index&& index) :
                    m_entries({ ext::Pooled{}, std::move(path) }),
                    m_index(std::move(index)),
                    m_id(dataFilePathToId(m_entries.path()))
                {
                }

                File(ext::ImmutableSpan<EntryT>&& entries, Index&& index) :
                    m_entries(std::move(entries)),
                    m_index(std::move(index)),
                    m_id(dataFilePathToId(m_entries.path()))
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

                [[nodiscard]] std::string name() const
                {
                    return fileIdToName(m_id);
                }

                [[nodiscard]] MergableFile mergableInfo() const
                {
                    return { name(), m_entries.size_bytes() };
                }

                [[nodiscard]] const std::filesystem::path& path() const
                {
                    return m_entries.path();
                }

                [[nodiscard]] EntryT at(std::size_t idx) const
                {
                    return m_entries[idx];
                }

                [[nodiscard]] const ext::ImmutableSpan<EntryT>& entries() const
                {
                    return m_entries;
                }

                void executeQuery(
                    const query::Request& query,
                    const std::vector<KeyT>& keys,
                    const query::PositionQueries& queries,
                    std::vector<PositionStats>& stats
                )
                {
                    ASSERT(queries.size() == stats.size());
                    ASSERT(queries.size() == keys.size());

                    std::vector<EntryT> buffer;
                    for (std::size_t i = 0; i < queries.size(); ++i)
                    {
                        auto& key = keys[i];
                        auto [a, b] = m_index.equal_range(key);

                        const std::size_t count = b.it - a.it;
                        if (count == 0) continue; // the range is empty, the value certainly does not exist

                        buffer.resize(count);
                        (void)m_entries.read(buffer.data(), a.it, count);
                        accumulateStatsFromEntries(buffer, query, key, queries[i].origin, stats[i]);
                    }
                }

                void queryRetractions(
                    const query::Request& query,
                    const Position& pos,
                    RetractionsStats& retractionsStats
                )
                {
                    const auto key = KeyT(PositionWithZobrist(pos));
                    auto [a, b] = m_index.equal_range(key);

                    const std::size_t count = b.it - a.it;
                    if (count == 0) return; // the range is empty, the value certainly does not exist

                    std::vector<EntryT> buffer(count);
                    (void)m_entries.read(buffer.data(), a.it, count);
                    accumulateRetractionsStatsFromEntries(buffer, query, pos, key, retractionsStats);
                }

            private:
                ext::ImmutableSpan<EntryT> m_entries;
                Index m_index;
                std::uint32_t m_id;

                void accumulateStatsFromEntries(
                    const std::vector<EntryT>& entries,
                    const query::Request& query,
                    const KeyT& key,
                    query::PositionQueryOrigin origin,
                    PositionStats& stats
                )
                {
                    for (auto&& [select, fetch] : query.fetchingOptions)
                    {
                        auto&& statsForThisSelect = stats[select];

                        if (origin == query::PositionQueryOrigin::Child && !fetch.fetchChildren)
                        {
                            continue;
                        }

                        for (auto&& entry : entries)
                        {
                            const GameLevel level = entry.level();
                            const GameResult result = entry.result();

                            if (
                                (select == query::Select::Continuations && CompareEqualWithReverseMove{}(entry, key))
                                || (select == query::Select::Transpositions && CompareEqualWithoutReverseMove{}(entry, key) && !CompareEqualWithReverseMove{}(entry, key))
                                || (select == query::Select::All && CompareEqualWithoutReverseMove{}(entry, key))
                                )
                            {
                                statsForThisSelect[level][result].combine(entry);
                            }
                        }
                    }
                }

                void accumulateRetractionsStatsFromEntries(
                    const std::vector<EntryT>& entries,
                    const query::Request& query,
                    const Position& pos,
                    const KeyT& key,
                    RetractionsStats& retractionsStats
                )
                {
                    if constexpr (hasReverseMove)
                    {
                        for (auto&& entry : entries)
                        {
                            if (!CompareEqualWithoutReverseMove{}(entry, key))
                            {
                                continue;
                            }

                            const GameLevel level = entry.level();
                            const GameResult result = entry.result();
                            const ReverseMove rmove = entry.reverseMove(pos);

                            if (rmove.isNull())
                            {
                                continue;
                            }

                            retractionsStats[rmove][level][result].combine(entry);
                        }
                    }
                }
            };

            struct FutureFile
            {
                FutureFile(std::future<Index>&& future, std::filesystem::path path) :
                    m_future(std::move(future)),
                    m_path(std::move(path)),
                    m_id(dataFilePathToId(m_path))
                {
                }

                friend bool operator<(const FutureFile& lhs, const FutureFile& rhs) noexcept
                {
                    return lhs.m_id < rhs.m_id;
                }

                [[nodiscard]] std::uint32_t id() const
                {
                    return m_id;
                }

                [[nodiscard]] File get() &&
                {
                    Index index = m_future.get();
                    return { m_path, std::move(index) };
                }

            private:
                std::future<Index> m_future;
                std::filesystem::path m_path;
                std::uint32_t m_id;
            };

            struct AsyncStorePipeline
            {
            private:
                struct Job
                {
                    Job(std::filesystem::path path, std::vector<EntryT>&& buffer, std::promise<Index>&& promise) :
                        path(std::move(path)),
                        buffer(std::move(buffer)),
                        promise(std::move(promise))
                    {
                    }

                    std::filesystem::path path;
                    std::vector<EntryT> buffer;
                    std::promise<Index> promise;
                };

            public:
                AsyncStorePipeline(std::vector<std::vector<EntryT>>&& buffers, std::size_t numSortingThreads = 1) :
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

                [[nodiscard]] std::future<Index> scheduleUnordered(const std::filesystem::path& path, std::vector<EntryT>&& elements)
                {
                    std::unique_lock<std::mutex> lock(m_mutex);

                    std::promise<Index> promise;
                    std::future<Index> future = promise.get_future();
                    m_sortQueue.emplace(path, std::move(elements), std::move(promise));

                    lock.unlock();
                    m_sortQueueNotEmpty.notify_one();

                    return future;
                }

                [[nodiscard]] std::vector<EntryT> getEmptyBuffer()
                {
                    std::unique_lock<std::mutex> lock(m_mutex);

                    m_bufferQueueNotEmpty.wait(lock, [this]() {return !m_bufferQueue.empty(); });

                    auto buffer = std::move(m_bufferQueue.front());
                    m_bufferQueue.pop();

                    buffer.clear();

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
                std::queue<std::vector<EntryT>> m_bufferQueue;

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

                        prepareData(job.buffer);

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

                        Index index = ext::makeIndex(job.buffer, m_indexGranularity, CompareLessWithoutReverseMove{}, [](const EntryT& entry) {
                            return entry.key();
                            });
                        writeIndexOfDataFile(job.path, index);
                        job.promise.set_value(std::move(index));

                        (void)ext::writeFile(job.path, job.buffer.data(), job.buffer.size());

                        job.buffer.clear();

                        lock.lock();
                        m_bufferQueue.emplace(std::move(job.buffer));
                        lock.unlock();

                        m_bufferQueueNotEmpty.notify_one();
                    }
                }

                void sort(std::vector<EntryT>& buffer)
                {
                    auto cmp = CompareLessFull{};
                    std::sort(buffer.begin(), buffer.end(), cmp);
                }

                // works analogously to std::unique but also combines equal values
                void combine(std::vector<EntryT>& buffer)
                {
                    if (buffer.empty()) return;

                    auto read = buffer.begin();
                    auto write = buffer.begin();
                    const auto end = buffer.end();
                    auto cmp = CompareEqualFull{};

                    while (++read != end)
                    {
                        if (cmp(*write, *read))
                        {
                            write->combine(*read);
                        }
                        else if (++write != read) // we don't want to copy onto itself
                        {
                            *write = *read;
                        }
                    }

                    buffer.erase(std::next(write), buffer.end());
                }

                void prepareData(std::vector<EntryT>& buffer)
                {
                    sort(buffer);
                    combine(buffer);
                }
            };

            struct Partition
            {
                Partition() :
                    m_lastId(0)
                {
                }

                Partition(std::filesystem::path path) :
                    m_lastId(0)
                {
                    ASSERT(!path.empty());

                    setPath(std::move(path));
                }

                void executeQuery(
                    const query::Request& query,
                    const std::vector<KeyT>& keys,
                    const query::PositionQueries& queries,
                    std::vector<PositionStats>& stats)
                {
                    for (auto&& file : m_files)
                    {
                        file->executeQuery(query, keys, queries, stats);
                    }
                }

                [[nodiscard]] RetractionsStats queryRetractions(
                    const query::Request& query,
                    const Position& pos
                )
                {
                    RetractionsStats retractionsStats;

                    for (auto&& file : m_files)
                    {
                        file->queryRetractions(query, pos, retractionsStats);
                    }

                    return retractionsStats;
                }

                void mergeAll(
                    const std::vector<std::filesystem::path>& temporaryDirs,
                    std::optional<MemoryAmount> temporarySpace,
                    std::function<void(const ext::Progress&)> progressCallback
                )
                {
                    auto files = getAllFiles();
                    if (temporarySpace.has_value())
                    {
                        mergeFiles(files, temporaryDirs, progressCallback, *temporarySpace);
                    }
                    else
                    {
                        mergeFiles(files, temporaryDirs, progressCallback);
                    }
                }

                void mergeFiles(
                    const std::vector<std::filesystem::path>& temporaryDirs,
                    std::optional<MemoryAmount> temporarySpace,
                    const std::vector<std::string>& filenames,
                    std::function<void(const ext::Progress&)> progressCallback
                )
                {
                    auto files = getFilesByNames(filenames);
                    if (temporarySpace.has_value())
                    {
                        mergeFiles(files, temporaryDirs, progressCallback, *temporarySpace);
                    }
                    else
                    {
                        mergeFiles(files, temporaryDirs, progressCallback);
                    }
                }

                [[nodiscard]] std::vector<MergableFile> mergableFiles() const
                {
                    std::vector<MergableFile> files;

                    for (auto& file : m_files)
                    {
                        files.emplace_back(file->mergableInfo());
                    }

                    return files;
                }

                [[nodiscard]] std::string name() const
                {
                    return m_path.filename().string();
                }

                // Uses the passed id.
                // It is required that the file with this id doesn't exist already.
                void storeUnordered(AsyncStorePipeline& pipeline, std::vector<EntryT>&& entries)
                {
                    ASSERT(!m_path.empty());

                    addFutureFile(pipeline, std::move(entries));
                }

                void collectFutureFiles()
                {
                    while (!m_futureFiles.empty())
                    {
                        addFile(std::make_unique<File>(std::move(m_futureFiles.back()).get()));
                        m_futureFiles.pop_back();
                    }
                }

                [[nodiscard]] const std::filesystem::path path() const
                {
                    return m_path;
                }

                void clear()
                {
                    collectFutureFiles();

                    while (!m_files.empty())
                    {
                        auto path = m_files.back()->path();
                        m_files.pop_back();

                        std::filesystem::remove(path);

                        auto indexPath = dataFilePathToIndexPath(path);
                        std::filesystem::remove(indexPath);
                    }
                }

                [[nodiscard]] bool empty() const
                {
                    return m_files.empty() && m_futureFiles.empty();
                }

            private:
                std::filesystem::path m_path;
                std::vector<std::unique_ptr<File>> m_files;

                std::uint32_t m_lastId;

                std::vector<FutureFile> m_futureFiles;

                [[nodiscard]] std::uint32_t nextId() const
                {
                    return m_lastId + 1;
                }

                void setPath(std::filesystem::path path)
                {
                    ASSERT(m_futureFiles.empty());

                    m_path = std::move(path);
                    std::filesystem::create_directories(m_path);

                    discoverFiles();
                }

                [[nodiscard]] ext::MergePlan makeMergePlan(
                    const std::vector<ext::ImmutableSpan<EntryT>>& files,
                    const std::filesystem::path& outFilePath,
                    const std::vector<std::filesystem::path>& temporaryDirs
                ) const
                {
                    const auto outDir = outFilePath.parent_path();

                    if (temporaryDirs.size() == 0)
                    {
                        return ext::make_merge_plan(files, outDir, outDir);
                    }
                    else if (temporaryDirs.size() == 1)
                    {
                        ext::MergePlan plan = ext::make_merge_plan(files, outDir, temporaryDirs[0]);
                        if (plan.numPasses() == 0)
                        {
                            return plan;
                        }

                        if (plan.passes.back().writeDir != outDir)
                        {
                            plan.invert();
                        }

                        return plan;
                    }
                    else
                    {
                        return ext::make_merge_plan(files, temporaryDirs[0], temporaryDirs[1]);
                    }
                }

                [[nodiscard]] Index mergeFilesIntoFile(
                    const std::vector<File*>& files,
                    const std::filesystem::path& outFilePath,
                    const std::vector<std::filesystem::path>& temporaryDirs,
                    std::function<void(const ext::Progress&)> progressCallback,
                    bool deleteOld
                )
                {
                    ASSERT(files.size() >= 2);

                    auto extractKey = [](const EntryT& entry) {
                        return entry.key();
                    };
                    ext::IndexBuilder<EntryT, CompareLessWithoutReverseMove, decltype(extractKey)> ib(m_indexGranularity, {}, extractKey);
                    {
                        auto onWrite = [&ib](const std::byte* data, std::size_t elementSize, std::size_t count) {
                            ib.append(reinterpret_cast<const EntryT*>(data), count);
                        };

                        ext::ObservableBinaryOutputFile outFile(onWrite, outFilePath);
                        std::vector<ext::ImmutableSpan<EntryT>> spans;
                        spans.reserve(files.size());
                        for (auto&& file : files)
                        {
                            spans.emplace_back(file->entries());
                        }

                        {
                            const std::size_t outBufferSize = ext::numObjectsPerBufferUnit<EntryT>(m_mergeWriterBufferSize.bytes(), 2);
                            ext::BackInserter<EntryT> out(outFile, util::DoubleBuffer<EntryT>(outBufferSize));

                            auto cmp = CompareEqualFull{};
                            bool first = true;
                            EntryT accumulator;
                            auto append = [&](const EntryT& entry) {
                                if (first)
                                {
                                    first = false;
                                    accumulator = entry;
                                }
                                else if (cmp(accumulator, entry))
                                {
                                    accumulator.combine(entry);
                                }
                                else
                                {
                                    out.emplace(accumulator);
                                    accumulator = entry;
                                }
                            };

                            const ext::MergePlan plan = makeMergePlan(spans, outFilePath, temporaryDirs);
                            // Now we have two options.
                            // Either we have to copy the files or not.
                            const bool requiresCopyFirst = plan.passes[0].readDir != outFilePath.parent_path();
                            if (requiresCopyFirst)
                            {
                                // We have to include the copying progress.
                                std::size_t totalFileSize = 0;
                                for (auto&& file : files)
                                {
                                    totalFileSize += file->entries().size_bytes();
                                }
                                ext::Progress internalProgress{ 0, totalFileSize };

                                const std::filesystem::path copyDestinationDir = plan.passes[0].readDir;
                                std::vector<std::filesystem::path> copiedFilesPaths;
                                copiedFilesPaths.reserve(files.size());
                                for (auto&& file : files)
                                {
                                    const std::size_t size = file->entries().size_bytes();

                                    std::filesystem::path destinationPath = copyDestinationDir / file->path().filename();
                                    std::filesystem::copy_file(file->path(), destinationPath);
                                    copiedFilesPaths.emplace_back(std::move(destinationPath));

                                    internalProgress.workDone += size;
                                    progressCallback(internalProgress);
                                }

                                spans.clear();

                                if (deleteOld)
                                {
                                    removeFiles(files);
                                }

                                for (auto&& path : copiedFilesPaths)
                                {
                                    spans.emplace_back(ext::ImmutableBinaryFile(ext::Pooled{}, path));
                                }

                                auto internalProgressCallback = [&progressCallback, &internalProgress, totalFileSize](const ext::Progress& progress)
                                {
                                    internalProgress.workDone = totalFileSize + progress.workDone;
                                    internalProgress.workTotal = totalFileSize + progress.workTotal;
                                    progressCallback(internalProgress);
                                };

                                auto cleanup = [&spans, &copiedFilesPaths]() {
                                    spans.clear();

                                    for (auto&& path : copiedFilesPaths)
                                    {
                                        std::filesystem::remove(path);
                                    }
                                    copiedFilesPaths.clear();
                                };

                                ext::MergeCallbacks callbacks{
                                    internalProgressCallback,
                                    [&cleanup](int passId) {
                                        if (passId == 0)
                                        {
                                            cleanup();
                                        }
                                    }
                                };
                                ext::merge_for_each(plan, callbacks, spans, append, CompareLessFull{});

                                if (!spans.empty())
                                {
                                    cleanup();
                                }
                            }
                            else
                            {
                                ext::MergeCallbacks callbacks{
                                    progressCallback,
                                    [deleteOld, &spans, &files, this](int passId) {
                                        if (passId == 0)
                                        {
                                            if (deleteOld)
                                            {
                                                spans.clear();
                                                removeFiles(files);
                                            }
                                        }
                                    }
                                };
                                ext::merge_for_each(plan, callbacks, spans, append, CompareLessFull{});

                                if (deleteOld && !spans.empty())
                                {
                                    spans.clear();
                                    removeFiles(files);
                                }
                            }

                            if (!first) // if we did anything, ie. accumulator holds something from merge
                            {
                                out.emplace(accumulator);
                            }
                        }
                    }

                    Index index = ib.end();
                    writeIndexOfDataFile(outFilePath, index);

                    return index;
                }

                void mergeFiles(
                    const std::vector<File*>& files,
                    const std::vector<std::filesystem::path>& temporaryDirs,
                    std::function<void(const ext::Progress&)> progressCallback,
                    MemoryAmount temporarySpace
                )
                {
                    auto groups = ext::groupConsecutiveSpans(
                        files,
                        temporarySpace,
                        [](File* file) { return file->entries().size_bytes(); }
                    );

                    // assess total work
                    std::size_t totalWork = 0;
                    for (auto&& filesInGroup : groups)
                    {
                        if (filesInGroup.size() < 2)
                        {
                            continue;
                        }
                        else
                        {
                            std::vector<ext::ImmutableSpan<EntryT>> spans;
                            spans.reserve(filesInGroup.size());
                            for (auto&& file : filesInGroup)
                            {
                                spans.emplace_back(file->entries());
                            }
                            totalWork += ext::merge_assess_work(spans);
                        }
                    }

                    ext::Progress progress{ static_cast<std::size_t>(0), totalWork };
                    std::size_t totalWorkDone = 0;
                    auto internalProgressCallback = [&totalWorkDone, &progress, &progressCallback](const ext::Progress& newProgress)
                    {
                        progress.workDone = totalWorkDone + newProgress.workDone;
                        progressCallback(progress);

                        if (newProgress.workDone == newProgress.workTotal)
                        {
                            totalWorkDone += newProgress.workTotal;
                        }
                    };

                    for (auto&& filesInGroup : groups)
                    {
                        if (filesInGroup.size() < 2)
                        {
                            continue;
                        }

                        mergeFiles(
                            filesInGroup,
                            temporaryDirs,
                            internalProgressCallback
                        );
                    }
                }

                void mergeFiles(
                    const std::vector<File*>& files,
                    const std::vector<std::filesystem::path>& temporaryDirs,
                    std::function<void(const ext::Progress&)> progressCallback
                )
                {
                    if (files.size() < 2)
                    {
                        progressCallback(ext::Progress{ 1, 1 });
                        return;
                    }

                    const auto outFilePath = m_path / "merge_tmp";
                    const std::uint32_t id = files.front()->id();
                    auto index = mergeFilesIntoFile(files, outFilePath, temporaryDirs, progressCallback, true);

                    // We had to use a temporary name because we're working in the same directory.
                    // Now we can safely rename after old ones are removed.
                    auto newFilePath = outFilePath;
                    newFilePath.replace_filename(std::to_string(id));
                    std::filesystem::rename(outFilePath, newFilePath);
                    std::filesystem::rename(dataFilePathToIndexPath(outFilePath), dataFilePathToIndexPath(newFilePath));

                    addFile(std::make_unique<File>(newFilePath, std::move(index)));
                }

                void removeFiles(
                    const std::vector<File*>& files
                )
                {
                    collectFutureFiles();

                    // TODO: maybe optimize this, it's currently O(n^2)
                    // But it shouldn't be a problem.
                    for (auto&& file : files)
                    {
                        auto it = std::find_if(m_files.begin(), m_files.end(), [&file](const std::unique_ptr<File>& lhs) {
                            return lhs.get() == file;
                            });
                        if (it == m_files.end()) continue;

                        auto path = (*it)->path();
                        auto indexPath = dataFilePathToIndexPath(path);

                        m_files.erase(it);

                        std::filesystem::remove(path);
                        std::filesystem::remove(indexPath);
                    }

                    m_lastId = 0;
                    for (auto&& file : m_files)
                    {
                        m_lastId = std::max(m_lastId, file->id());
                    }
                }

                [[nodiscard]] std::vector<File*> getFilesByNames(
                    const std::vector<std::string>& names
                )
                {
                    std::vector<File*> selectedFiles;
                    selectedFiles.reserve(names.size());

                    std::set<std::string> namesSet(names.begin(), names.end());

                    for (auto&& file : m_files)
                    {
                        auto it = namesSet.find(file->name());
                        if (it == namesSet.end())
                        {
                            continue;
                        }

                        selectedFiles.push_back(file.get());
                    }

                    return selectedFiles;
                }

                [[nodiscard]] std::vector<File*> getAllFiles()
                {
                    std::vector<File*> files;

                    files.reserve(m_files.size());
                    for (auto&& f : m_files)
                    {
                        files.push_back(f.get());
                    }

                    return files;
                }

                void discoverFiles()
                {
                    // If we don't wait for future files first then we could 
                    // get some partial ones and break the app.
                    collectFutureFiles();

                    m_files.clear();
                    m_lastId = 0;

                    for (auto& entry : std::filesystem::directory_iterator(m_path))
                    {
                        if (!entry.is_regular_file())
                        {
                            continue;
                        }

                        if (isPathOfIndex(entry.path()))
                        {
                            continue;
                        }

                        if (entry.file_size() == 0)
                        {
                            continue;
                        }

                        addFile(entry.path());
                    }
                }

                void addFile(const std::filesystem::path& path)
                {
                    auto file = std::make_unique<File>(path);
                    m_lastId = std::max(m_lastId, file->id());
                    m_files.emplace_back(std::move(file));
                }

                void addFile(std::unique_ptr<File> file)
                {
                    m_lastId = std::max(m_lastId, file->id());
                    m_files.emplace_back(std::move(file));
                }

                void addFutureFile(AsyncStorePipeline& pipeline, std::vector<EntryT>&& entries)
                {
                    const std::uint32_t id = nextId();
                    auto path = pathOfDataFileWithId(m_path, id);
                    m_lastId = std::max(m_lastId, id);
                    m_futureFiles.emplace_back(pipeline.scheduleUnordered(path, std::move(entries)), path);
                }
            };

        private:
            using BaseType = persistence::Database;

            static inline const std::filesystem::path partitionDirectory = "data";

            static inline const DatabaseManifest m_manifest = { name, true };

            static constexpr std::size_t m_totalNumDirectories = 1;

            static inline const EnumArray<GameLevel, std::string> m_headerNames = {
                "_human",
                "_engine",
                "_server"
            };

            static inline const MemoryAmount m_headerBufferMemory = cfg::g_config["persistence"][name]["header_buffer_memory"].get<MemoryAmount>();
            static inline const MemoryAmount m_pgnParserMemory = cfg::g_config["persistence"][name]["pgn_parser_memory"].get<MemoryAmount>();
            static inline const MemoryAmount m_bcgnParserMemory = cfg::g_config["persistence"][name]["bcgn_parser_memory"].get<MemoryAmount>();

        public:
            OrderedEntrySetPositionDatabase(std::filesystem::path path) :
                BaseType(path, m_manifest),
                m_path(path),
                m_headers(makeHeaders(path, m_headerBufferMemory)),
                m_partition(path / partitionDirectory)
            {
            }

            [[nodiscard]] static const std::string& key()
            {
                return m_manifest.key;
            }

            [[nodiscard]] static const DatabaseSupportManifest& supportManifest()
            {
                static const DatabaseSupportManifest manifest = {
                    { 
                        ImportableFileType::Pgn, 
                        ImportableFileType::Bcgn 
                    },
                    MergeMode::Any
                };

                return manifest;
            }

            [[nodiscard]] const DatabaseManifest& manifest() const override
            {
                return m_manifest;
            }

            void clear() override
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                if constexpr (hasGameHeaders)
                {
                    for (auto& header : m_headers)
                    {
                        header->clear();
                    }
                }
                m_partition.clear();
            }

            const std::filesystem::path& path() const override
            {
                return m_path;
            }

            [[nodiscard]] query::Response executeQuery(query::Request query) override
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                disableUnsupportedQueryFeatures(query);

                query::PositionQueries posQueries = query::gatherPositionQueries(query);
                auto keys = getKeys(posQueries);
                std::vector<PositionStats> stats(posQueries.size());

                auto cmp = KeyCompareLessWithReverseMove{};
                auto unsort = reversibleZipSort(keys, posQueries, cmp);

                m_partition.executeQuery(query, keys, posQueries, stats);

                auto results = segregatePositionStats(query, posQueries, stats);

                // We have to either unsort both results and posQueries, or none.
                // unflatten only needs relative order of results and posQueries to match
                // So we don't unsort any.
                auto unflattened = query::unflatten(std::move(results), query, posQueries);

                if constexpr (hasReverseMove)
                {
                    if (query.retractionsFetchingOptions.has_value())
                    {
                        for (auto&& resultForRoot : unflattened)
                        {
                            auto queried = m_partition.queryRetractions(
                                query,
                                *resultForRoot.position.tryGet()
                            );

                            auto segregated = segregateRetractionsStats(
                                query,
                                std::move(queried)
                            );

                            resultForRoot.retractionsResults.retractions = std::move(segregated);
                        }
                    }
                }

                return { std::move(query), std::move(unflattened) };
            }

            void mergeAll(
                const std::vector<std::filesystem::path>& temporaryDirs,
                std::optional<MemoryAmount> temporarySpace,
                MergeProgressCallback progressCallback = {}
            ) override
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                Logger::instance().logInfo(": Merging files...");

                auto progressReport = [&progressCallback](const ext::Progress& report) {
                    Logger::instance().logInfo(":     ", static_cast<int>(report.ratio() * 100), "%.");

                    if (progressCallback)
                    {
                        MergeProgressReport r{
                            report.workDone,
                            report.workTotal
                        };
                        progressCallback(r);
                    }
                };

                m_partition.mergeAll(temporaryDirs, temporarySpace, progressReport);

                Logger::instance().logInfo(": Finalizing...");
                Logger::instance().logInfo(": Completed.");
            }

            void merge(
                const std::vector<std::filesystem::path>& temporaryDirs,
                std::optional<MemoryAmount> temporarySpace,
                const std::string& partitionName,
                const std::vector<std::string>& filenames,
                MergeProgressCallback progressCallback = {}
            ) override
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                if (m_partition.name() != partitionName)
                {
                    throw std::runtime_error("Parititon with name '" + partitionName + "' not found.");
                }

                Logger::instance().logInfo(": Merging files...");

                auto progressReport = [&progressCallback](const ext::Progress& report) {
                    Logger::instance().logInfo(":     ", static_cast<int>(report.ratio() * 100), "%.");

                    if (progressCallback)
                    {
                        MergeProgressReport r{
                            report.workDone,
                            report.workTotal
                        };
                        progressCallback(r);
                    }
                };

                m_partition.mergeFiles(temporaryDirs, temporarySpace, filenames, progressReport);

                Logger::instance().logInfo(": Finalizing...");
                Logger::instance().logInfo(": Completed.");
            }

            ImportStats import(
                const ImportableFiles& files,
                std::size_t memory,
                ImportProgressCallback progressCallback = {}
            ) override
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                const std::size_t numSortingThreads = std::clamp(std::thread::hardware_concurrency(), 2u, 3u) - 1u;

                if (files.empty())
                {
                    return {};
                }

                std::size_t totalSize = 0;
                std::size_t totalSizeProcessed = 0;
                for (auto&& file : files)
                {
                    totalSize += std::filesystem::file_size(file.path());
                }

                const std::size_t numBuffers = 1;

                const std::size_t numAdditionalBuffers = 1 + numSortingThreads;

                const std::size_t bucketSize =
                    ext::numObjectsPerBufferUnit<EntryT>(
                        memory,
                        numBuffers + numAdditionalBuffers
                        );

                AsyncStorePipeline pipeline(
                    createBuffers<EntryT>(numBuffers + numAdditionalBuffers, bucketSize),
                    numSortingThreads
                );

                Logger::instance().logInfo(": Importing files...");
                ImportStats statsTotal = importImpl(
                    pipeline,
                    files,
                    [&progressCallback, &totalSize, &totalSizeProcessed](auto&& file) {
                        totalSizeProcessed += std::filesystem::file_size(file);
                        Logger::instance().logInfo(
                            ":     ",
                            static_cast<int>(static_cast<double>(totalSizeProcessed) / totalSize * 100.0),
                            "% - completed ",
                            file,
                            "."
                        );

                        if (progressCallback)
                        {
                            ImportProgressReport report{
                                totalSizeProcessed,
                                totalSize,
                                file
                            };
                            progressCallback(report);
                        }
                    });
                Logger::instance().logInfo(": Finalizing...");

                pipeline.waitForCompletion();

                flush();

                Logger::instance().logInfo(": Completed.");

                Logger::instance().logInfo(": Imported ", statsTotal.totalNumGames(), " games with ", statsTotal.totalNumPositions(), " positions. Skipped ", statsTotal.totalNumSkippedGames(), " games.");

                BaseType::addStats(statsTotal);

                return statsTotal;
            }

            [[nodiscard]] std::map<std::string, std::vector<MergableFile>> mergableFiles() const override
            {
                std::map<std::string, std::vector<MergableFile>> files;

                files[m_partition.name()] = m_partition.mergableFiles();

                return files;
            }

            void flush() override
            {
                collectFutureFiles();

                if constexpr (hasGameHeaders)
                {
                    for (auto& header : m_headers)
                    {
                        header->flush();
                    }
                }
            }


        private:
            std::filesystem::path m_path;

            // TODO: don't include them when !hasGameHeaders
            EnumArray<GameLevel, std::unique_ptr<IndexedGameHeaderStorage>> m_headers;

            // We only have one partition for this format
            Partition m_partition;

            std::mutex m_mutex;
            [[nodiscard]] EnumArray<GameLevel, std::unique_ptr<IndexedGameHeaderStorage>> makeHeaders(const std::filesystem::path& path, MemoryAmount headerBufferMemory)
            {
                if constexpr (hasGameHeaders)
                {
                    return {
                        std::make_unique<IndexedGameHeaderStorage>(path, headerBufferMemory, m_headerNames[values<GameLevel>()[0]]),
                        std::make_unique<IndexedGameHeaderStorage>(path, headerBufferMemory, m_headerNames[values<GameLevel>()[1]]),
                        std::make_unique<IndexedGameHeaderStorage>(path, headerBufferMemory, m_headerNames[values<GameLevel>()[2]])
                    };
                }
                else
                {
                    return {};
                }
            }

            void collectFutureFiles()
            {
                m_partition.collectFutureFiles();
            }

            [[nodiscard]] std::vector<PackedGameHeader> queryHeadersByOffsets(const std::vector<std::uint64_t>& offsets, GameLevel level)
            {
                return m_headers[level]->queryByOffsets(offsets);
            }

            template <typename DestinationT>
            [[nodiscard]] std::vector<GameHeader> queryHeadersByOffsets(const std::vector<std::uint64_t>& offsets, const std::vector<DestinationT>& destinations)
            {
                EnumArray<GameLevel, std::vector<std::uint64_t>> offsetsByLevel;
                EnumArray<GameLevel, std::vector<std::size_t>> indices;

                for (std::size_t i = 0; i < offsets.size(); ++i)
                {
                    offsetsByLevel[destinations[i].level].emplace_back(offsets[i]);
                    indices[destinations[i].level].emplace_back(i);
                }

                EnumArray<GameLevel, std::vector<PackedGameHeader>> packedHeadersByLevel;
                for (GameLevel level : values<GameLevel>())
                {
                    packedHeadersByLevel[level] = queryHeadersByOffsets(offsetsByLevel[level], level);
                }

                std::vector<GameHeader> headers(offsets.size());

                for (GameLevel level : values<GameLevel>())
                {
                    const auto size = offsetsByLevel[level].size();
                    for (std::size_t i = 0; i < size; ++i)
                    {
                        headers[indices[level][i]] = packedHeadersByLevel[level][i];
                    }
                }

                return headers;
            }

            [[nodiscard]] std::vector<PackedGameHeader> queryHeadersByIndices(const std::vector<std::uint32_t>& indices, GameLevel level)
            {
                return m_headers[level]->queryByIndices(indices);
            }

            template <typename DestinationT>
            [[nodiscard]] std::vector<GameHeader> queryHeadersByIndices(const std::vector<std::uint32_t>& indices, const std::vector<DestinationT>& destinations)
            {
                EnumArray<GameLevel, std::vector<std::uint32_t>> indicesByLevel;
                EnumArray<GameLevel, std::vector<std::size_t>> localIndices;

                for (std::size_t i = 0; i < indices.size(); ++i)
                {
                    indicesByLevel[destinations[i].level].emplace_back(indices[i]);
                    localIndices[destinations[i].level].emplace_back(i);
                }

                EnumArray<GameLevel, std::vector<PackedGameHeader>> packedHeadersByLevel;
                for (GameLevel level : values<GameLevel>())
                {
                    packedHeadersByLevel[level] = queryHeadersByIndices(indicesByLevel[level], level);
                }

                std::vector<GameHeader> headers(indices.size());

                for (GameLevel level : values<GameLevel>())
                {
                    const auto size = indicesByLevel[level].size();
                    for (std::size_t i = 0; i < size; ++i)
                    {
                        headers[localIndices[level][i]] = packedHeadersByLevel[level][i];
                    }
                }

                return headers;
            }

            void disableUnsupportedQueryFeatures(query::Request& query) const
            {
                for (auto& [k, f] : query.fetchingOptions)
                {
                    if constexpr (!hasFirstGame)
                    {
                        f.fetchFirstGame = false;
                        f.fetchFirstGameForEachChild = false;
                    }

                    if constexpr (!hasLastGame)
                    {
                        f.fetchLastGame = false;
                        f.fetchLastGameForEachChild = false;
                    }
                }

                if constexpr (!hasReverseMove)
                {
                    query.retractionsFetchingOptions = std::nullopt;
                }
            }

            template <typename SegregatedT, typename DestinationT>
            void assignGameHeaders(
                SegregatedT& segregated,
                const std::vector<std::uint32_t>& firstGameIndices,
                const std::vector<std::uint32_t>& lastGameIndices,
                const std::vector<std::uint64_t>& firstGameOffsets,
                const std::vector<std::uint64_t>& lastGameOffsets,
                const std::vector<DestinationT>& firstGameDestinations,
                const std::vector<DestinationT>& lastGameDestinations
            )
            {
                if constexpr (hasFirstGameIndex)
                {
                    query::assignGameHeaders(segregated, firstGameDestinations, queryHeadersByIndices(firstGameIndices, firstGameDestinations));
                }

                if constexpr (hasFirstGameOffset)
                {
                    query::assignGameHeaders(segregated, firstGameDestinations, queryHeadersByOffsets(firstGameOffsets, firstGameDestinations));
                }

                if constexpr (hasLastGameIndex)
                {
                    query::assignGameHeaders(segregated, lastGameDestinations, queryHeadersByIndices(lastGameIndices, lastGameDestinations));
                }

                if constexpr (hasLastGameOffset)
                {
                    query::assignGameHeaders(segregated, lastGameDestinations, queryHeadersByOffsets(lastGameOffsets, lastGameDestinations));
                }
            }

            [[nodiscard]] query::PositionQueryResults segregatePositionStats(
                const query::Request& query,
                const query::PositionQueries& posQueries,
                std::vector<PositionStats>& stats
            )
            {
                const query::FetchLookups lookup = query::buildGameHeaderFetchLookup(query);

                query::PositionQueryResults segregated(posQueries.size());

                std::vector<std::uint32_t> firstGameIndices;
                std::vector<std::uint32_t> lastGameIndices;
                std::vector<std::uint64_t> firstGameOffsets;
                std::vector<std::uint64_t> lastGameOffsets;
                std::vector<query::GameHeaderDestination> firstGameDestinations;
                std::vector<query::GameHeaderDestination> lastGameDestinations;

                for (std::size_t i = 0; i < posQueries.size(); ++i)
                {
                    auto&& [position, reverseMove, rootId, origin] = posQueries[i];
                    auto&& stat = stats[i];

                    for (auto&& [select, fetch] : query.fetchingOptions)
                    {
                        if (origin == query::PositionQueryOrigin::Child && !fetch.fetchChildren)
                        {
                            continue;
                        }

                        for (GameLevel level : query.levels)
                        {
                            for (GameResult result : query.results)
                            {
                                auto& entry = stat[select][level][result];
                                auto& segregatedEntry = segregated[i][select].emplace(level, result, entry.count());
                                if constexpr (hasEloDiff)
                                {
                                    segregatedEntry.second.eloDiff = entry.eloDiff();
                                }

                                if (entry.count() > 0)
                                {
                                    if constexpr (hasFirstGame)
                                    {
                                        if (lookup[origin][select].fetchFirst)
                                        {
                                            if constexpr (hasFirstGameIndex)
                                            {
                                                firstGameIndices.emplace_back(entry.firstGameIndex());
                                            }

                                            if constexpr (hasFirstGameOffset)
                                            {
                                                firstGameOffsets.emplace_back(entry.firstGameOffset());
                                            }

                                            firstGameDestinations.emplace_back(i, select, level, result, &query::Entry::firstGame);
                                        }
                                    }

                                    if constexpr (hasLastGame)
                                    {
                                        if (lookup[origin][select].fetchLast)
                                        {
                                            if constexpr (hasLastGameIndex)
                                            {
                                                lastGameIndices.emplace_back(entry.lastGameIndex());
                                            }

                                            if constexpr (hasLastGameOffset)
                                            {
                                                lastGameOffsets.emplace_back(entry.lastGameOffset());
                                            }

                                            lastGameDestinations.emplace_back(i, select, level, result, &query::Entry::lastGame);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                assignGameHeaders(
                    segregated,
                    firstGameIndices,
                    lastGameIndices,
                    firstGameOffsets,
                    lastGameOffsets,
                    firstGameDestinations,
                    lastGameDestinations
                );

                return segregated;
            }

            [[nodiscard]] query::RetractionsQueryResults
                segregateRetractionsStats(
                    const query::Request& query,
                    RetractionsStats&& unsegregated
                )
            {
                const auto& fetching = *query.retractionsFetchingOptions;

                query::RetractionsQueryResults segregated;

                std::vector<std::uint32_t> firstGameIndices;
                std::vector<std::uint32_t> lastGameIndices;
                std::vector<std::uint64_t> firstGameOffsets;
                std::vector<std::uint64_t> lastGameOffsets;
                std::vector<query::GameHeaderDestinationForRetraction> firstGameDestinations;
                std::vector<query::GameHeaderDestinationForRetraction> lastGameDestinations;

                for (auto&& [reverseMove, stat] : unsegregated)
                {
                    auto segregatedEntries = query::SegregatedEntries();
                    for (GameLevel level : query.levels)
                    {
                        for (GameResult result : query.results)
                        {
                            auto& entry = stat[level][result];
                            auto& segregatedEntry = segregatedEntries.emplace(level, result, entry.count());
                            if constexpr (hasEloDiff)
                            {
                                segregatedEntry.second.eloDiff = entry.eloDiff();
                            }

                            if (entry.count() > 0)
                            {
                                if constexpr (hasFirstGame)
                                {
                                    if (fetching.fetchFirstGameForEach)
                                    {
                                        if constexpr (hasFirstGameIndex)
                                        {
                                            firstGameIndices.emplace_back(entry.firstGameIndex());
                                        }

                                        if constexpr (hasFirstGameOffset)
                                        {
                                            firstGameOffsets.emplace_back(entry.firstGameOffset());
                                        }

                                        firstGameDestinations.emplace_back(reverseMove, level, result, &query::Entry::firstGame);
                                    }
                                }

                                if constexpr (hasLastGame)
                                {
                                    if (fetching.fetchLastGameForEach)
                                    {
                                        if constexpr (hasLastGameIndex)
                                        {
                                            lastGameIndices.emplace_back(entry.lastGameIndex());
                                        }

                                        if constexpr (hasLastGameOffset)
                                        {
                                            lastGameOffsets.emplace_back(entry.lastGameOffset());
                                        }

                                        lastGameDestinations.emplace_back(reverseMove, level, result, &query::Entry::lastGame);
                                    }
                                }
                            }
                        }
                    }

                    segregated[reverseMove] = segregatedEntries;
                }

                assignGameHeaders(
                    segregated,
                    firstGameIndices,
                    lastGameIndices,
                    firstGameOffsets,
                    lastGameOffsets,
                    firstGameDestinations,
                    lastGameDestinations
                );

                return segregated;
            }

            [[nodiscard]] std::vector<KeyT> getKeys(const query::PositionQueries& queries)
            {
                std::vector<KeyT> keys;
                keys.reserve(queries.size());
                for (auto&& q : queries)
                {
                    keys.emplace_back(PositionWithZobrist(q.position), q.reverseMove);
                }
                return keys;
            }

            ImportStats importImpl(
                AsyncStorePipeline& pipeline,
                const ImportableFiles& files,
                std::function<void(const std::filesystem::path& file)> completionCallback
            )
            {
                // create buffers
                std::vector<EntryT> bucket = pipeline.getEmptyBuffer();

                auto processPosition = [this, &bucket, &pipeline](
                    const PositionWithZobrist& position,
                    const ReverseMove& reverseMove,
                    GameLevel level,
                    GameResult result,
                    auto gameIndexOrOffset,
                    std::uint64_t eloDiff
                    ) {
                        // TODO: how to make this better? currently we have a lot of combinations
                        if constexpr (hasEloDiff)
                        {
                            if constexpr (hasFirstGame + hasLastGame == 0)
                            {
                                bucket.emplace_back(position, reverseMove, level, result, eloDiff);
                            }
                            else if constexpr (hasFirstGame + hasLastGame == 1)
                            {
                                bucket.emplace_back(position, reverseMove, level, result, gameIndexOrOffset, eloDiff);
                            }
                            else if constexpr (hasFirstGame + hasLastGame == 2)
                            {
                                bucket.emplace_back(position, reverseMove, level, result, gameIndexOrOffset, gameIndexOrOffset, eloDiff);
                            }
                        }
                        else
                        {
                            if constexpr (hasFirstGame + hasLastGame == 0)
                            {
                                bucket.emplace_back(position, reverseMove, level, result);
                            }
                            else if constexpr (hasFirstGame + hasLastGame == 1)
                            {
                                bucket.emplace_back(position, reverseMove, level, result, gameIndexOrOffset);
                            }
                            else if constexpr (hasFirstGame + hasLastGame == 2)
                            {
                                bucket.emplace_back(position, reverseMove, level, result, gameIndexOrOffset, gameIndexOrOffset);
                            }
                        }

                        if (bucket.size() == bucket.capacity())
                        {
                            store(pipeline, bucket);
                        }
                };

                ImportStats stats{};
                for (auto& file : files)
                {
                    const auto& path = file.path();
                    const auto level = file.level();
                    const auto type = file.type();

                    if (type == ImportableFileType::Pgn)
                    {
                        pgn::LazyPgnFileReader fr(path, m_pgnParserMemory.bytes());
                        if (!fr.isOpen())
                        {
                            Logger::instance().logError("Failed to open file ", path);
                            completionCallback(path);
                            break;
                        }

                        for (auto& game : fr)
                        {
                            const std::optional<GameResult> result = game.result();
                            if (!result.has_value())
                            {
                                stats.statsByLevel[level].numSkippedGames += 1;
                                continue;
                            }

                            const std::int64_t eloDiff = [&game]() {
                                if constexpr (hasEloDiff) return game.eloDiff();
                                else return 0;
                            }();

                            const auto gameIndexOrOffset = [this, level]() {
                                if constexpr (usesGameIndex) return m_headers[level]->nextGameId();
                                else if constexpr (usesGameOffset) return m_headers[level]->nextGameOffset();
                                else return 0;
                            }();

                            PositionWithZobrist position = PositionWithZobrist::startPosition();
                            ReverseMove reverseMove{};

                            processPosition(position, reverseMove, level, *result, gameIndexOrOffset, eloDiff);
                            std::size_t numPositionsInGame = 1;
                            for (auto& san : game.moves())
                            {
                                const Move move = san::sanToMove(position, san);
                                if (move == Move::null())
                                {
                                    break;
                                }

                                reverseMove = position.doMove(move);
                                processPosition(position, reverseMove, level, *result, gameIndexOrOffset, eloDiff);

                                ++numPositionsInGame;
                            }

                            ASSERT(numPositionsInGame > 0);

                            if constexpr (hasGameHeaders)
                            {
                                m_headers[level]->addGame(game, static_cast<std::uint16_t>(numPositionsInGame - 1u)).index;
                            }

                            stats.statsByLevel[level].numGames += 1;
                            stats.statsByLevel[level].numPositions += numPositionsInGame;
                        }
                    }
                    else if (type == ImportableFileType::Bcgn)
                    {
                        bcgn::BcgnFileReader fr(path, m_bcgnParserMemory.bytes());
                        if (!fr.isOpen())
                        {
                            Logger::instance().logError("Failed to open file ", path);
                            completionCallback(path);
                            break;
                        }

                        for (auto& game : fr)
                        {
                            const std::optional<GameResult> result = game.result();
                            if (!result.has_value())
                            {
                                stats.statsByLevel[level].numSkippedGames += 1;
                                continue;
                            }

                            const std::int64_t eloDiff = [&game]() {
                                if constexpr (hasEloDiff)
                                {
                                    auto gameHeader = game.gameHeader();
                                    return (std::uint64_t)gameHeader.whiteElo() - (std::uint64_t)gameHeader.blackElo();
                                }
                                else return 0;
                            }();

                            const auto gameIndexOrOffset = [this, level]() {
                                if constexpr (usesGameIndex) return m_headers[level]->nextGameId();
                                else if constexpr (usesGameOffset) return m_headers[level]->nextGameOffset();
                                else return 0;
                            }();

                            PositionWithZobrist position = PositionWithZobrist::startPosition();
                            ReverseMove reverseMove{};
                            processPosition(position, reverseMove, level, *result, gameIndexOrOffset, eloDiff);
                            auto moves = game.moves();
                            while (moves.hasNext())
                            {
                                const auto move = moves.next(position);
                                reverseMove = position.doMove(move);
                                processPosition(position, reverseMove, level, *result, gameIndexOrOffset, eloDiff);
                            }

                            const auto numPositionsInGame = game.numPlies() + 1;

                            if constexpr (hasGameHeaders)
                            {
                                m_headers[level]->addGame(game, static_cast<std::uint16_t>(numPositionsInGame - 1u)).index;
                            }

                            stats.statsByLevel[level].numGames += 1;
                            stats.statsByLevel[level].numPositions += numPositionsInGame;
                        }
                    }
                    else
                    {
                        Logger::instance().logError("Importing files other than PGN or BCGN is not supported.");
                        throw std::runtime_error("Importing files other than PGN or BCGN is not supported.");
                    }

                    completionCallback(path);
                }

                // flush buffers and return them to the pipeline for later use
                store(pipeline, std::move(bucket));

                return stats;
            }

            void store(
                AsyncStorePipeline& pipeline,
                std::vector<EntryT>& entries
            )
            {
                if (entries.empty())
                {
                    return;
                }

                auto newBuffer = pipeline.getEmptyBuffer();
                entries.swap(newBuffer);
                m_partition.storeUnordered(pipeline, std::move(newBuffer));
            }

            void store(
                AsyncStorePipeline& pipeline,
                std::vector<EntryT>&& entries
            )
            {
                if (entries.empty())
                {
                    return;
                }

                m_partition.storeUnordered(pipeline, std::move(entries));
            }
        };
    }
}