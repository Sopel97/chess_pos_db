#pragma once

#include "Database.h"
#include "Query.h"

#include "algorithm/Unsort.h"

#include "chess/GameClassification.h"

#include "enum/EnumArray.h"

#include "external_storage/External.h"

#include "Logger.h"

#include <algorithm>
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
    namespace db
    {
        namespace detail
        {
            template <typename T>
            class hasEloDiff 
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(size_t{ std::declval<const C>().eloDiff() }, Yes{});

                template<typename> static constexpr No& Test(...);

            public:
                static constexpr bool value = sizeof(Test<T>(0)) == sizeof(Yes);
            };

            template <typename T>
            class hasFirstGameIndex 
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(size_t{ std::declval<const C>().firstGameIndex() }, Yes{});

                template<typename> static constexpr No& Test(...);

            public:
                static constexpr bool value = sizeof(Test<T>(0)) == sizeof(Yes);
            };

            template <typename T>
            class hasLastGameIndex
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(size_t{ std::declval<const C>().lastGameIndex() }, Yes{});

                template<typename> static constexpr No& Test(...);

            public:
                static constexpr bool value = sizeof(Test<T>(0)) == sizeof(Yes);
            };

            template <typename T>
            class hasFirstGameOffset
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(size_t{ std::declval<const C>().firstGameOffset() }, Yes{});

                template<typename> static constexpr No& Test(...);

            public:
                static constexpr bool value = sizeof(Test<T>(0)) == sizeof(Yes);
            };

            template <typename T>
            class hasLastGameOffset
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(size_t{ std::declval<const C>().lastGameOffset() }, Yes{});

                template<typename> static constexpr No& Test(...);

            public:
                static constexpr bool value = sizeof(Test<T>(0)) == sizeof(Yes);
            };

            template <typename T>
            class hasReverseMove
            {
            private:
                using Yes = char;
                using No = Yes[2];

                template<typename C> static constexpr auto Test(void*)
                    -> decltype(size_t{ std::declval<const C>().reverseMove(std::declval<const Position>()) }, Yes{});

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
        struct Database final : persistence::Database
        {
            static_assert(std::is_trivially_copyable_v<EntryT>);

            static constexpr bool hasEloDiff = detail::hasEloDiff<EntryT>::value;
            static constexpr bool hasFirstGameIndex = detail::hasFirstGameIndex<EntryT>::value;
            static constexpr bool hasLastGameIndex = detail::hasLastGameIndex<EntryT>::value;
            static constexpr bool hasFirstGameOffset = detail::hasFirstGameOffset<EntryT>::value;
            static constexpr bool hasLastGameOffset = detail::hasLastGameOffset<EntryT>::value;
            static constexpr bool hasReverseMove = detail::hasReverseMove<EntryT>::value;

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

            [[nodiscard]] static std::filesystem::path pathForIndex(const std::filesystem::path& path)
            {
                auto cpy = path;
                cpy += "_index";
                return cpy;
            }

            [[nodiscard]] static auto readIndexFor(const std::filesystem::path& path)
            {
                auto indexPath = pathForIndex(path);
                return Index(ext::readFile<typename Index::EntryType>(indexPath));
            }

            static void writeIndexFor(const std::filesystem::path& path, const Index& index)
            {
                auto indexPath = pathForIndex(path);
                (void)ext::writeFile<typename Index::EntryType>(indexPath, index.data(), index.size());
            }

            static auto extractEntryKey = [](const EntryT& entry) {
                return entry.key();
            };

            struct File
            {
                static std::filesystem::path pathForId(const std::filesystem::path& path, std::uint32_t id)
                {
                    return path / std::to_string(id);
                }

                File(const File&) = delete;
                File(File&&) noexcept = default;

                File& operator=(const File&) = delete;
                File& operator=(File&&) noexcept = default;

                File(std::filesystem::path path) :
                    m_entries({ ext::Pooled{}, std::move(path) }),
                    m_index(readIndexFor(m_entries.path())),
                    m_id(std::stoi(m_entries.path().filename().string()))
                {
                }

                File(ext::ImmutableSpan<EntryT>&& entries) :
                    m_entries(std::move(entries)),
                    m_index(readIndexFor(m_entries.path())),
                    m_id(std::stoi(m_entries.path().filename().string()))
                {
                }

                File(std::filesystem::path path, Index&& index) :
                    m_entries({ ext::Pooled{}, std::move(path) }),
                    m_index(std::move(index)),
                    m_id(std::stoi(m_entries.path().filename().string()))
                {
                }

                File(ext::ImmutableSpan<EntryT>&& entries, Index&& index) :
                    m_entries(std::move(entries)),
                    m_index(std::move(index)),
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
                    std::vector<PositionStats>& stats)
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

                void File::queryRetractions(
                    const query::Request& query,
                    const Position& pos,
                    RetractionsStats& retractionsStats
                )
                {
                    const auto key = EntryT(PositionWithZobrist(pos));
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
                    const Key& key,
                    query::PositionQueryOrigin origin,
                    PositionStats& stats)
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
                    m_id(std::stoi(m_path.filename().string()))
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

                [[nodiscard]] File get()
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

                [[nodiscard]] std::future<Index> scheduleUnordered(const std::filesystem::path& path, std::vector<Entry>&& elements)
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

                        Index index = ext::makeIndex(job.buffer, indexGranularity, CompareLessWithoutReverseMove{}, extractEntryKey);
                        writeIndexFor(job.path, index);
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

                // Uses the passed id.
                // It is required that the file with this id doesn't exist already.
                void storeUnordered(AsyncStorePipeline& pipeline, std::vector<EntryT>&& entries, std::uint32_t id)
                {
                    ASSERT(!m_path.empty());

                    std::unique_lock<std::mutex> lock(m_mutex);
                    auto path = pathForId(id);
                    m_futureFiles.emplace(pipeline.scheduleUnordered(path, std::move(entries)), path);
                }

                void storeUnordered(AsyncStorePipeline& pipeline, std::vector<EntryT>&& entries)
                {
                    storeUnordered(pipeline, std::move(entries), nextId());
                }

                void collectFutureFiles()
                {
                    while (!m_futureFiles.empty())
                        m_files.emplace_back(
                            std::make_unique<File>(
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
                        return m_files.back()->id() + 1;
                    }

                    return 0;
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

                        auto indexPath = pathForIndex(path);
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

                // We store it in a set because then we can change insertion
                // order through forcing ids. It's easier to keep it
                // ordered like that. And we need it ordered all the time
                // because of queries to nextId()
                std::set<FutureFile> m_futureFiles;

                std::mutex m_mutex;

                [[nodiscard]] std::filesystem::path pathForId(std::uint32_t id) const
                {
                    return File::pathForId(m_path, id);
                }

                [[nodiscard]] std::filesystem::path nextPath() const
                {
                    return pathForId(nextId());
                }

                [[nodiscard]] ext::MergePlan makeMergePlan(
                    const std::vector<ext::ImmutableSpan<Entry>>& files,
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

                    ext::IndexBuilder<EntryT, CompareLessWithoutReverseMove, decltype(extractEntryKey)> ib(indexGranularity, {}, extractEntryKey);
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
                            const std::size_t outBufferSize = ext::numObjectsPerBufferUnit<EntryT>(mergeMemory.bytes() / 32, 2);
                            ext::BackInserter<EntryT> out(outFile, util::DoubleBuffer<EntryT>(outBufferSize));

                            auto cmp = CompareEqualFull{};
                            bool first = true;
                            Entry accumulator;
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
                                ext::merge_for_each(plan, callbacks, spans, append, Entry::CompareLessFull{});

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
                                ext::merge_for_each(plan, callbacks, spans, append, Entry::CompareLessFull{});

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
                    writeIndexFor(outFilePath, index);

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
                            totalWorkDone = newProgress.workTotal;
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
                    std::filesystem::rename(pathForIndex(outFilePath), pathForIndex(newFilePath));

                    m_files.emplace_back(std::make_unique<File>(newFilePath, std::move(index)));
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
                        auto indexPath = pathForIndex(path);

                        m_files.erase(it);

                        std::filesystem::remove(path);
                        std::filesystem::remove(indexPath);
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
                        auto it = namesSet.find(file->path().filename().string());
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

                    for (auto& entry : std::filesystem::directory_iterator(m_path))
                    {
                        if (!entry.is_regular_file())
                        {
                            continue;
                        }

                        if (entry.path().filename().string().find("index") != std::string::npos)
                        {
                            continue;
                        }

                        if (entry.file_size() == 0)
                        {
                            continue;
                        }

                        m_files.emplace_back(std::make_unique<File>(entry.path()));
                    }

                    std::sort(m_files.begin(), m_files.end());
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

            static const MemoryAmount m_pgnParserMemory = cfg::g_config["persistence"][name]["pgn_parser_memory"].get<MemoryAmount>();

        public:
            Database(std::filesystem::path path) :
                BaseType(path, Database::manifest()),
                m_path(path),
                m_headers(makeHeaders(path)),
                m_nextGameIdx(numGamesInHeaders()),
                m_partition(path / partitionDirectory)
            {
            }

            Database(std::filesystem::path path, std::size_t headerBufferMemory) :
                BaseType(path, Database::manifest()),
                m_path(path),
                m_headers(makeHeaders(path, headerBufferMemory)),
                m_nextGameIdx(numGamesInHeaders()),
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
                    }
                };

                return manifest;
            }

            [[nodiscard]] const DatabaseManifest& manifest() const override
            {
                return m_manifest;
            }

            void clear() override
            {
                for (auto& header : *m_headers)
                {
                    header.clear();
                }
                m_partition.clear();
            }

            const std::filesystem::path& path() const override
            {
                return m_path;
            }

            [[nodiscard]] query::Response executeQuery(query::Request query) override
            {
                disableUnsupportedQueryFeatures(query);

                query::PositionQueries posQueries = query::gatherPositionQueries(query);
                auto keys = getKeys(posQueries);
                std::vector<PositionStats> stats(posQueries.size());

                auto cmp = KeyCompareLessWithReverseMove{};
                auto unsort = reversibleZipSort(keys, posQueries, cmp);

                m_partition.executeQuery(query, keys, posQueries, stats);

                auto results = commitStatsAsResults(query, posQueries, stats);

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

                            auto segregated = segregateRetractions(
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

            ImportStats import(
                const ImportableFiles& files,
                std::size_t memory,
                ImportProgressCallback progressCallback = {}
            ) override
            {
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
                collectFutureFiles();

                flush();

                Logger::instance().logInfo(": Completed.");

                Logger::instance().logInfo(": Imported ", statsTotal.totalNumGames(), " games with ", statsTotal.totalNumPositions(), " positions. Skipped ", statsTotal.totalNumSkippedGames(), " games.");

                BaseType::addStats(statsTotal);

                return statsTotal;
            }

            void flush() override
            {
                for (auto& header : *m_headers)
                {
                    header.flush();
                }
            }

        private:
            std::filesystem::path m_path;

            // TODO: don't include them when !hasGameHeaders
            std::optional<EnumArray<GameLevel, Header>> m_headers;
            std::atomic<std::uint32_t> m_nextGameIdx;

            // We only have one partition for this format
            Partition m_partition;

            [[nodiscard]] std::optional<EnumArray<GameLevel, Header>> makeHeaders(const std::filesystem::path& path)
            {
                return makeHeaders(Header::defaultMemory);
            }

            [[nodiscard]] std::optional<EnumArray<GameLevel, Header>> makeHeaders(const std::filesystem::path& path, std::size_t headerBufferMemory)
            {
                if constexpr (hasGameHeaders)
                {
                    return {
                        Header(path, headerBufferMemory, m_headerNames[values<GameLevel>()[0]]),
                        Header(path, headerBufferMemory, m_headerNames[values<GameLevel>()[1]]),
                        Header(path, headerBufferMemory, m_headerNames[values<GameLevel>()[2]])
                    };
                }
                else
                {
                    return std::nullopt;
                }
            }

            [[nodiscard]] std::uint32_t numGamesInHeaders() const
            {
                if constexpr (hasGameHeaders)
                {
                    std::uint32_t total = 0;

                    for (auto& header : *m_headers)
                    {
                        total += header.numGames();
                    }

                    return total;
                }
                else
                {
                    return 0;
                }
            }

            void collectFutureFiles()
            {
                m_partition.collectFutureFiles();
            }

            [[nodiscard]] std::vector<PackedGameHeader> Database::queryHeadersByOffsets(const std::vector<std::uint64_t>& offsets, GameLevel level)
            {
                return (*m_headers)[level].queryByOffsets(offsets);
            }

            [[nodiscard]] std::vector<GameHeader> Database::queryHeadersByOffsets(const std::vector<std::uint64_t>& offsets, const std::vector<query::GameHeaderDestination>& destinations)
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

            [[nodiscard]] std::vector<GameHeader> Database::queryHeadersByOffsets(const std::vector<std::uint64_t>& offsets, const std::vector<query::GameHeaderDestinationForRetraction>& destinations)
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
                return (*m_headers)[level].queryByIndices(indices);
            }

            [[nodiscard]] std::vector<GameHeader> queryHeadersByIndices(const std::vector<std::uint32_t>& indices, const std::vector<query::GameHeaderDestination>& destinations)
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

            [[nodiscard]] std::vector<GameHeader> queryHeadersByIndices(const std::vector<std::uint32_t>& indices, const std::vector<query::GameHeaderDestinationForRetraction>& destinations)
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
            }

            [[nodiscard]] query::PositionQueryResults commitStatsAsResults(
                const query::Request& query,
                const query::PositionQueries& posQueries,
                std::vector<PositionStats>& stats)
            {
                query::PositionQueryResults results(posQueries.size());
                std::vector<std::uint32_t> firstGameIndices;
                std::vector<std::uint32_t> lastGameIndices;
                std::vector<std::uint64_t> firstGameOffsets;
                std::vector<std::uint64_t> lastGameOffsets;
                std::vector<query::GameHeaderDestination> firstGameDestinations;
                std::vector<query::GameHeaderDestination> lastGameDestinations;
                query::FetchLookups lookup = query::buildGameHeaderFetchLookup(query);

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
                                auto& segregatedEntry = results[i][select].emplace(level, result, entry.count());
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

                if constexpr (hasFirstGameIndex)
                {
                    query::assignGameHeaders(results, firstGameDestinations, queryHeadersByIndices(firstGameIndices, firstGameDestinations));
                }

                if constexpr (hasFirstGameOffset)
                {
                    query::assignGameHeaders(results, firstGameDestinations, queryHeadersByOffsets(firstGameOffsets, firstGameDestinations));
                }

                if constexpr (hasLastGameIndex)
                {
                    query::assignGameHeaders(results, lastGameDestinations, queryHeadersByIndices(lastGameIndices, lastGameDestinations));
                }

                if constexpr (hasLastGameOffset)
                {
                    query::assignGameHeaders(results, lastGameDestinations, queryHeadersByOffsets(lastGameOffsets, lastGameDestinations));
                }

                return results;
            }

            [[nodiscard]] query::RetractionsQueryResults
                segregateRetractions(
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
                query::FetchLookups lookup = query::buildGameHeaderFetchLookup(query);

                for (auto&& [reverseMove, stat] : unsegregated)
                {
                    auto segregatedEntries = query::SegregatedEntries();
                    for (GameLevel level : query.levels)
                    {
                        for (GameResult result : query.results)
                        {
                            auto& entry = stat[level][result];
                            auto& segregatedEntry = segregatedEntries.emplace(level, result, entry.count());
                            segregatedEntry.second.eloDiff = entry.eloDiff();

                            if (entry.count() > 0)
                            {
                                if constexpr (hasFirstGame)
                                {
                                    if (lookup[origin][select].fetchFirstGameForEach)
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
                                    if (lookup[origin][select].fetchLastGameForEach)
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

                    segregated[reverseMove] = segregatedEntries;
                }

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

                            const auto gameIndexOrOffset = [level]() {
                                if constexpr (usesGameIndex) return (*m_headers)[level].nextGameId();
                                if constexpr (usesGameOffset) return (*m_headers)[level].nextGameOffset();
                                return 0;
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
                                auto gameHeader = PackedGameHeader(game, m_nextGameIdx.fetch_add(1, std::memory_order_relaxed), static_cast<std::uint16_t>(numPositionsInGame - 1u));
                                const std::uint64_t actualGameIndex = header.addHeaderNoLock(gameHeader).index;
                                ASSERT(gameIndex == actualGameIndex);
                                (void)actualGameIndex;
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
                                    auto& gameHeader = game.gameHeader();
                                    return (std::uint64_t)gameHeader.whiteElo() - (std::uint64_t)gameHeader.blackElo();
                                }
                                else return 0;
                            }();

                            const auto gameIndexOrOffset = [level]() {
                                if constexpr (usesGameIndex) return (*m_headers)[level].nextGameId();
                                if constexpr (usesGameOffset) return (*m_headers)[level].nextGameOffset();
                                return 0;
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
                                auto gameHeader = PackedGameHeader(game, m_nextGameIdx.fetch_add(1, std::memory_order_relaxed), static_cast<std::uint16_t>(numPositionsInGame - 1u));
                                const std::uint64_t actualGameIndex = header.addHeaderNoLock(gameHeader).index;
                                ASSERT(gameIndex == actualGameIndex);
                                (void)actualGameIndex;
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