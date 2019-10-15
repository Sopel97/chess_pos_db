#pragma once

#include "Configuration.h"
#include "Database.h"
#include "EnumMap.h"
#include "External.h"
#include "GameClassification.h"
#include "MemoryAmount.h"
#include "Logger.h"
#include "Pgn.h"
#include "Query.h"
#include "StorageHeader.h"
#include "Unsort.h"

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

#include "lib/infint/InfInt.h"

namespace persistence
{
    namespace db_alpha
    {
        namespace detail
        {
            static constexpr bool useIndex = true;

            // Have ranges of mixed values be at most this long
            static inline const std::size_t indexGranularity = cfg::g_config["persistence"]["db_alpha"]["index_granularity"].get<std::size_t>();

            struct Key
            {
                using StorageType = std::array<std::uint32_t, 4>;

                Key() = default;

                Key(const Position& pos, const ReverseMove& reverseMove = ReverseMove{}) :
                    m_hash(pos.hash())
                {
                    auto packedReverseMove = PackedReverseMove(reverseMove);
                    // m_hash[0] is the most significant quad, m_hash[3] is the least significant
                    // We want entries ordered with reverse move to also be ordered by just hash
                    // so we have to modify the lowest bits.
                    m_hash[3] = (m_hash[3] & ~PackedReverseMove::mask) | packedReverseMove.packed();
                }

                Key(const Key&) = default;
                Key(Key&&) = default;
                Key& operator=(const Key&) = default;
                Key& operator=(Key&&) = default;

                [[nodiscard]] const StorageType& hash() const
                {
                    return m_hash;
                }

                struct CompareLessWithReverseMove
                {
                    [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) noexcept
                    {
                        if (lhs.m_hash[0] < rhs.m_hash[0]) return true;
                        else if (lhs.m_hash[0] > rhs.m_hash[0]) return false;

                        if (lhs.m_hash[1] < rhs.m_hash[1]) return true;
                        else if (lhs.m_hash[1] > rhs.m_hash[1]) return false;

                        if (lhs.m_hash[2] < rhs.m_hash[2]) return true;
                        else if (lhs.m_hash[2] > rhs.m_hash[2]) return false;

                        return (lhs.m_hash[3] < rhs.m_hash[3]);
                    }
                };

                struct CompareLessWithoutReverseMove
                {
                    [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) noexcept
                    {
                        if (lhs.m_hash[0] < rhs.m_hash[0]) return true;
                        else if (lhs.m_hash[0] > rhs.m_hash[0]) return false;

                        if (lhs.m_hash[1] < rhs.m_hash[1]) return true;
                        else if (lhs.m_hash[1] > rhs.m_hash[1]) return false;

                        if (lhs.m_hash[2] < rhs.m_hash[2]) return true;
                        else if (lhs.m_hash[2] > rhs.m_hash[2]) return false;

                        return ((lhs.m_hash[3] & ~PackedReverseMove::mask) < (rhs.m_hash[3] & ~PackedReverseMove::mask));
                    }
                };

                struct CompareEqualWithReverseMove
                {
                    [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                    {
                        return
                            lhs.m_hash[0] == rhs.m_hash[0]
                            && lhs.m_hash[1] == rhs.m_hash[1]
                            && lhs.m_hash[2] == rhs.m_hash[2]
                            && lhs.m_hash[3] == rhs.m_hash[3];
                    }
                };

                struct CompareEqualWithoutReverseMove
                {
                    [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                    {
                        return
                            lhs.m_hash[0] == rhs.m_hash[0]
                            && lhs.m_hash[1] == rhs.m_hash[1]
                            && lhs.m_hash[2] == rhs.m_hash[2]
                            && (lhs.m_hash[3] & ~PackedReverseMove::mask) == (rhs.m_hash[3] & ~PackedReverseMove::mask);
                    }
                };

            private:
                // All bits of the hash are created equal, so we can specify some ordering.
                // Elements ordered from least significant to most significant are [3][2][1][0]
                StorageType m_hash;
            };

            static_assert(sizeof(Key) == 16);

            struct Entry
            {
                Entry() = default;

                Entry(const Position& pos, const ReverseMove& reverseMove, std::uint32_t gameIdx) :
                    m_key(pos, reverseMove),
                    m_gameIdx(gameIdx)
                {
                }

                // TODO: eventually remove this overload?
                Entry(const Position& pos, std::uint32_t gameIdx) :
                    m_key(pos),
                    m_gameIdx(gameIdx)
                {
                }

                Entry(const Entry&) = default;
                Entry(Entry&&) = default;
                Entry& operator=(const Entry&) = default;
                Entry& operator=(Entry&&) = default;

                struct CompareLessWithoutReverseMove
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        return Key::CompareLessWithoutReverseMove{}(lhs.m_key, rhs.m_key);
                    }

                    [[nodiscard]] bool operator()(const Entry& lhs, const Key& rhs) const noexcept
                    {
                        return Key::CompareLessWithoutReverseMove{}(lhs.m_key, rhs);
                    }

                    [[nodiscard]] bool operator()(const Key& lhs, const Entry& rhs) const noexcept
                    {
                        return Key::CompareLessWithoutReverseMove{}(lhs, rhs.m_key);
                    }

                    [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                    {
                        return Key::CompareLessWithoutReverseMove{}(lhs, rhs);
                    }
                };

                // This behaves like the old operator<
                struct CompareLessWithReverseMove
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        return Key::CompareLessWithReverseMove{}(lhs.m_key, rhs.m_key);
                    }

                    [[nodiscard]] bool operator()(const Entry& lhs, const Key& rhs) const noexcept
                    {
                        return Key::CompareLessWithReverseMove{}(lhs.m_key, rhs);
                    }

                    [[nodiscard]] bool operator()(const Key& lhs, const Entry& rhs) const noexcept
                    {
                        return Key::CompareLessWithReverseMove{}(lhs, rhs.m_key);
                    }

                    [[nodiscard]] bool operator()(const Key& lhs, const Key& rhs) const noexcept
                    {
                        return Key::CompareLessWithReverseMove{}(lhs, rhs);
                    }
                };

                [[nodiscard]] const Key& key() const
                {
                    return m_key;
                }

                [[nodiscard]] std::uint32_t gameIdx() const
                {
                    return m_gameIdx;
                }

            private:
                Key m_key;
                std::uint32_t m_gameIdx;
            };

            static_assert(sizeof(Entry) == 20);
            static_assert(std::is_trivially_copyable_v<Entry>);

            using IndexWithoutReverseMove = ext::RangeIndex<Key, Entry::CompareLessWithoutReverseMove>;
            using IndexWithReverseMove = ext::RangeIndex<Key, Entry::CompareLessWithReverseMove>;

            using Indexes = std::pair<IndexWithoutReverseMove, IndexWithReverseMove>;

            struct IndexWithoutReverseMoveTag 
            {
                using IndexType = IndexWithoutReverseMove;

                constexpr static const char* suffix = "_index0";
            };

            struct IndexWithReverseMoveTag
            {
                using IndexType = IndexWithReverseMove;

                constexpr static const char* suffix = "_index1";
            };

            template <typename IndexTagT>
            [[nodiscard]] std::filesystem::path pathForIndex(const std::filesystem::path& path)
            {
                auto cpy = path;
                cpy += IndexTagT::suffix;
                return cpy;
            }

            template <typename IndexTagT>
            [[nodiscard]] auto readIndexFor(const std::filesystem::path& path)
            {
                using IndexType = typename IndexTagT::IndexType;

                if constexpr (useIndex)
                {
                    auto indexPath = pathForIndex<IndexTagT>(path);
                    return IndexType(ext::readFile<typename IndexType::EntryType>(indexPath));
                }
                else
                {
                    return IndexType{};
                }
            }

            template <typename IndexTagT>
            void writeIndexFor(const std::filesystem::path& path, const typename IndexTagT::IndexType& index)
            {
                if constexpr (useIndex)
                {
                    auto indexPath = pathForIndex<IndexTagT>(path);
                    (void)ext::writeFile<typename IndexTagT::IndexType::EntryType>(indexPath, index.data(), index.size());
                }
            }

            auto extractEntryKey = [](const Entry& entry) {
                return entry.key();
            };

            auto entryKeyToArithmetic = [](const Key& sig) {
                static InfInt base(std::numeric_limits<std::uint32_t>::max());

                InfInt value = sig.hash()[0];
                value *= base;
                value += sig.hash()[1];
                value *= base;
                value += sig.hash()[2];
                value *= base;
                value += sig.hash()[3];
                return value;
            };

            auto entryKeyToArithmeticWithoutReverseMove = [](const Key& sig) {
                static InfInt base(std::numeric_limits<std::uint32_t>::max());

                InfInt value = sig.hash()[0];
                value *= base;
                value += sig.hash()[1];
                value *= base;
                value += sig.hash()[2];
                value *= base;
                value += sig.hash()[3] & ~PackedReverseMove::mask;
                return value;
            };

            auto entryKeyArithmeticToSizeT = [](const InfInt& value) {
                return value.toUnsignedLongLong();
            };

            struct File;

            struct CountAndGameIndices
            {
                std::size_t count = 0;
                const File* firstGameFile = nullptr;
                const File* lastGameFile = nullptr;
                std::uint64_t firstGameEntryIdx = 0;
                std::uint64_t lastGameEntryIdx = 0;

                void combine(const CountAndGameIndices& rhs);
            };

            using PositionStats = EnumMap<query::Select, EnumMap2<GameLevel, GameResult, CountAndGameIndices>>;

            struct File
            {
                static std::filesystem::path pathForId(const std::filesystem::path& path, std::uint32_t id)
                {
                    return path / std::to_string(id);
                }

                File(std::filesystem::path path) :
                    m_entries({ ext::Pooled{}, std::move(path) }),
                    m_indexWithoutReverseMove(readIndexFor<IndexWithoutReverseMoveTag>(m_entries.path())),
                    m_indexWithReverseMove(readIndexFor<IndexWithReverseMoveTag>(m_entries.path())),
                    m_id(std::stoi(m_entries.path().filename().string()))
                {
                }

                File(ext::ImmutableSpan<Entry>&& entries) :
                    m_entries(std::move(entries)),
                    m_indexWithoutReverseMove(readIndexFor<IndexWithoutReverseMoveTag>(m_entries.path())),
                    m_indexWithReverseMove(readIndexFor<IndexWithReverseMoveTag>(m_entries.path())),
                    m_id(std::stoi(m_entries.path().filename().string()))
                {
                }

                File(std::filesystem::path path, Indexes&& index) :
                    m_entries({ ext::Pooled{}, std::move(path) }),
                    m_indexWithoutReverseMove(std::move(index.first)),
                    m_indexWithReverseMove(std::move(index.second)),
                    m_id(std::stoi(m_entries.path().filename().string()))
                {
                }

                File(ext::ImmutableSpan<Entry>&& entries, Indexes&& index) :
                    m_entries(std::move(entries)),
                    m_indexWithoutReverseMove(std::move(index.first)),
                    m_indexWithReverseMove(std::move(index.second)),
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

                [[nodiscard]] Entry at(std::size_t idx) const
                {
                    return m_entries[idx];
                }

                [[nodiscard]] const ext::ImmutableSpan<Entry>& entries() const
                {
                    return m_entries;
                }

                template <query::Select SelectV>
                void executeQuery(
                    const std::vector<Key>& keys,
                    std::vector<PositionStats>& stats,
                    GameLevel level,
                    GameResult result);

                void executeQueryContinuations(
                    const std::vector<Key>& keys,
                    std::vector<PositionStats>& stats,
                    GameLevel level,
                    GameResult result)
                {
                    executeQuery<query::Select::Continuations>(keys, stats, level, result);
                }

                void executeQueryAll(
                    const std::vector<Key>& keys,
                    std::vector<PositionStats>& stats,
                    GameLevel level,
                    GameResult result)
                {
                    executeQuery<query::Select::All>(keys, stats, level, result);
                }

            private:
                ext::ImmutableSpan<Entry> m_entries;
                IndexWithoutReverseMove m_indexWithoutReverseMove;
                IndexWithReverseMove m_indexWithReverseMove;
                std::uint32_t m_id;
            };

            void CountAndGameIndices::combine(const CountAndGameIndices& rhs)
            {
                count += rhs.count;

                if (rhs.firstGameFile != nullptr)
                {
                    if (firstGameFile == nullptr)
                    {
                        firstGameFile = rhs.firstGameFile;
                        firstGameEntryIdx = rhs.firstGameEntryIdx;
                    }
                    else if (rhs.firstGameFile != nullptr && rhs.firstGameFile->id() < firstGameFile->id())
                    {
                        firstGameFile = rhs.firstGameFile;
                        firstGameEntryIdx = rhs.firstGameEntryIdx;
                    }
                    else if (rhs.firstGameFile->id() == firstGameFile->id() && rhs.firstGameEntryIdx < firstGameEntryIdx)
                    {
                        firstGameEntryIdx = rhs.firstGameEntryIdx;
                    }
                }

                if (rhs.lastGameFile != nullptr)
                {
                    if (lastGameFile == nullptr)
                    {
                        lastGameFile = rhs.lastGameFile;
                        lastGameEntryIdx = rhs.lastGameEntryIdx;
                    }
                    else if (rhs.lastGameFile != nullptr && rhs.lastGameFile->id() > lastGameFile->id())
                    {
                        lastGameFile = rhs.lastGameFile;
                        lastGameEntryIdx = rhs.lastGameEntryIdx;
                    }
                    else if (rhs.lastGameFile->id() == lastGameFile->id() && rhs.lastGameEntryIdx > lastGameEntryIdx)
                    {
                        lastGameEntryIdx = rhs.lastGameEntryIdx;
                    }
                }
            }

            template <query::Select SelectV>
            void File::executeQuery(
                const std::vector<Key>& keys,
                std::vector<PositionStats>& stats,
                GameLevel level,
                GameResult result)
            {
                static_assert(SelectV == query::Select::Continuations || SelectV == query::Select::All);

                using CompareT = std::conditional_t<
                    SelectV == query::Select::Continuations,
                    Entry::CompareLessWithReverseMove,
                    Entry::CompareLessWithoutReverseMove
                >;

                auto&& index = [this]() -> decltype(auto) {
                    if constexpr (SelectV == query::Select::Continuations)
                    {
                        return m_indexWithReverseMove;
                    }
                    else
                    {
                        return m_indexWithoutReverseMove;
                    }
                }();

                auto searchResults = [this, &keys, &index]() {
                    if constexpr (useIndex)
                    {
                        return ext::equal_range_multiple_interp_indexed_cross(
                            m_entries,
                            index,
                            keys,
                            CompareT{},
                            extractEntryKey,
                            entryKeyToArithmetic,
                            entryKeyArithmeticToSizeT
                        );
                    }
                    else
                    {
                        return ext::equal_range_multiple_interp_cross(
                            m_entries,
                            keys,
                            CompareT{},
                            extractEntryKey,
                            entryKeyToArithmetic,
                            entryKeyArithmeticToSizeT
                        );
                    }
                }();

                for (int i = 0; i < searchResults.size(); ++i)
                {
                    const auto& range = searchResults[i];
                    const auto count = range.second - range.first;
                    if (count == 0) continue;

                    auto& currentEntry = stats[i][SelectV][level][result];

                    CountAndGameIndices newEntry;
                    newEntry.count = count;
                    newEntry.firstGameFile = this;
                    newEntry.lastGameFile = this;
                    newEntry.firstGameEntryIdx = range.first;
                    newEntry.lastGameEntryIdx = range.second - 1;

                    currentEntry.combine(newEntry);
                }
            }

            struct FutureFile
            {
                FutureFile(std::future<Indexes>&& future, std::filesystem::path path) :
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
                    Indexes indexes = m_future.get();
                    return { m_path, std::move(indexes) };
                }

            private:
                std::future<Indexes> m_future;
                std::filesystem::path m_path;
                std::uint32_t m_id;
            };

            struct AsyncStorePipeline
            {
            private:
                struct Job
                {
                    Job(std::filesystem::path path, std::vector<Entry>&& buffer, std::promise<Indexes>&& promise, bool createIndex = useIndex) :
                        path(std::move(path)),
                        buffer(std::move(buffer)),
                        promise(std::move(promise)),
                        createIndex(createIndex)
                    {
                    }

                    std::filesystem::path path;
                    std::vector<Entry> buffer;
                    std::promise<Indexes> promise;
                    bool createIndex;
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

                [[nodiscard]] std::future<Indexes> scheduleUnordered(const std::filesystem::path& path, std::vector<Entry>&& elements, bool createIndex = useIndex)
                {
                    std::unique_lock<std::mutex> lock(m_mutex);

                    std::promise<Indexes> promise;
                    std::future<Indexes> future = promise.get_future();
                    m_sortQueue.emplace(path, std::move(elements), std::move(promise), createIndex);

                    lock.unlock();
                    m_sortQueueNotEmpty.notify_one();

                    return future;
                }

                [[nodiscard]] std::future<Indexes> scheduleOrdered(const std::filesystem::path& path, std::vector<Entry>&& elements, bool createIndex = useIndex)
                {
                    std::unique_lock<std::mutex> lock(m_mutex);

                    std::promise<Indexes> promise;
                    std::future<Indexes> future = promise.get_future();
                    m_sortQueue.emplace(path, std::move(elements), std::move(promise), createIndex);

                    lock.unlock();
                    m_writeQueueNotEmpty.notify_one();

                    return future;
                }

                [[nodiscard]] std::vector<Entry> getEmptyBuffer()
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

                        // NOTE: we don't need stable_sort here as game indices are
                        //       already ordered within one buffer.
                        // When we sort with reverse move then we have it sorted both
                        // with and without it.
                        auto cmp = Entry::CompareLessWithReverseMove{};
                        std::sort(job.buffer.begin(), job.buffer.end(), [cmp](auto&& lhs, auto&& rhs) {
                            if (cmp(lhs, rhs)) return true;
                            else if (cmp(rhs, lhs)) return false;
                            return lhs.gameIdx() < rhs.gameIdx();
                        });

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

                        if (job.createIndex)
                        {
                            IndexWithoutReverseMove index0 = ext::makeIndex(job.buffer, indexGranularity, Entry::CompareLessWithoutReverseMove{}, extractEntryKey);
                            IndexWithReverseMove index1 = ext::makeIndex(job.buffer, indexGranularity, Entry::CompareLessWithReverseMove{}, extractEntryKey);
                            writeIndexFor<IndexWithoutReverseMoveTag>(job.path, index0);
                            writeIndexFor<IndexWithReverseMoveTag>(job.path, index1);
                            job.promise.set_value(std::make_pair(std::move(index0), std::move(index1)));
                        }
                        else
                        {
                            job.promise.set_value(Indexes{});
                        }

                        (void)ext::writeFile(job.path, job.buffer.data(), job.buffer.size());

                        job.buffer.clear();

                        lock.lock();
                        m_bufferQueue.emplace(std::move(job.buffer));
                        lock.unlock();

                        m_bufferQueueNotEmpty.notify_one();
                    }
                }
            };

            struct Partition
            {
                static inline const std::size_t mergeMemory = cfg::g_config["persistence"]["db_alpha"]["max_merge_buffer_size"].get<MemoryAmount>();

                Partition() = default;

                Partition(std::filesystem::path path)
                {
                    ASSERT(!path.empty());

                    setPath(std::move(path));
                }

                void executeQueryContinuations(
                    const std::vector<Key>& keys,
                    std::vector<PositionStats>& stats,
                    GameLevel level,
                    GameResult result)
                {
                    for (auto&& file : m_files)
                    {
                        file.executeQueryContinuations(keys, stats, level, result);
                    }
                }

                void executeQueryAll(
                    const std::vector<Key>& keys,
                    std::vector<PositionStats>& stats,
                    GameLevel level,
                    GameResult result)
                {
                    for (auto&& file : m_files)
                    {
                        file.executeQueryAll(keys, stats, level, result);
                    }
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
                    (void)ext::writeFile(path, data, count);
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

                    std::unique_lock<std::mutex> lock(m_mutex);
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
                            m_futureFiles.extract(
                                m_futureFiles.begin()
                            )
                            .value()
                            .get()
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

                [[nodiscard]] const std::filesystem::path path() const
                {
                    return m_path;
                }

                void clear()
                {
                    collectFutureFiles();

                    while (!m_files.empty())
                    {
                        auto path = m_files.back().path();
                        m_files.pop_back();

                        std::filesystem::remove(path);
                        if constexpr (useIndex)
                        {
                            auto indexPath0 = pathForIndex<IndexWithoutReverseMoveTag>(path);
                            auto indexPath1 = pathForIndex<IndexWithReverseMoveTag>(path);

                            std::filesystem::remove(indexPath0);
                            std::filesystem::remove(indexPath1);
                        }
                    }
                }

                void mergeAll(std::function<void(const ext::ProgressReport&)> progressCallback)
                {
                    if (m_files.size() < 2)
                    {
                        return;
                    }

                    const auto outFilePath = m_path / "merge_tmp";
                    const std::uint32_t id = m_files.front().id();
                    auto index = mergeAllIntoFile(outFilePath, progressCallback);

                    // We haven't added the new files yet so they won't be removed.
                    clear();

                    // We had to use a temporary name because we're working in the same directory.
                    // Now we can safely rename after old ones are removed.
                    auto newFilePath = outFilePath;
                    newFilePath.replace_filename(std::to_string(id));
                    std::filesystem::rename(outFilePath, newFilePath);
                    std::filesystem::rename(pathForIndex<IndexWithoutReverseMoveTag>(outFilePath), pathForIndex<IndexWithoutReverseMoveTag>(newFilePath));
                    std::filesystem::rename(pathForIndex<IndexWithReverseMoveTag>(outFilePath), pathForIndex<IndexWithReverseMoveTag>(newFilePath));

                    m_files.emplace_back(newFilePath, std::move(index));
                }

                [[nodiscard]] bool empty() const
                {
                    return m_files.empty() && m_futureFiles.empty();
                }

                // outPath is a path of the file to output to
                void replicateMergeAll(const std::filesystem::path& outPath, std::function<void(const ext::ProgressReport&)> progressCallback)
                {
                    if (m_files.empty())
                    {
                        return;
                    }

                    ASSERT(outPath != path());

                    const auto outFilePath = outPath / "0";

                    if (m_files.size() == 1)
                    {
                        auto path = m_files.front().path();
                        std::filesystem::copy_file(path, outFilePath, std::filesystem::copy_options::overwrite_existing);

                        {
                            auto fromIndexPath0 = pathForIndex<IndexWithoutReverseMoveTag>(path);
                            auto toIndexPath0 = pathForIndex<IndexWithoutReverseMoveTag>(outFilePath);
                            std::filesystem::copy_file(fromIndexPath0, toIndexPath0, std::filesystem::copy_options::overwrite_existing);
                        }

                        {
                            auto fromIndexPath1 = pathForIndex<IndexWithReverseMoveTag>(path);
                            auto toIndexPath1 = pathForIndex<IndexWithReverseMoveTag>(outFilePath);
                            std::filesystem::copy_file(fromIndexPath1, toIndexPath1, std::filesystem::copy_options::overwrite_existing);
                        }
                    }
                    else
                    {
                        (void)mergeAllIntoFile(outFilePath, progressCallback);
                    }
                }

            private:
                std::filesystem::path m_path;
                std::vector<File> m_files;

                // We store it in a set because then we can change insertion
                // order through forcing ids. It's easier to keep it
                // ordered like that. And we need it ordered all the time
                // because of queries to nextId()
                std::set<FutureFile> m_futureFiles;

                std::mutex m_mutex;

                [[nodiscard]] Indexes mergeAllIntoFile(const std::filesystem::path& outFilePath, std::function<void(const ext::ProgressReport&)> progressCallback) const
                {
                    ASSERT(!m_files.empty());

                    ext::IndexBuilder<Entry, Entry::CompareLessWithoutReverseMove, decltype(extractEntryKey)> ib0(indexGranularity, {}, extractEntryKey);
                    ext::IndexBuilder<Entry, Entry::CompareLessWithReverseMove, decltype(extractEntryKey)> ib1(indexGranularity, {}, extractEntryKey);
                    {
                        auto onWrite = [&ib0, &ib1](const std::byte* data, std::size_t elementSize, std::size_t count) {
                            if constexpr (useIndex)
                            {
                                ib0.append(reinterpret_cast<const Entry*>(data), count);
                                ib1.append(reinterpret_cast<const Entry*>(data), count);
                            }
                        };

                        ext::ObservableBinaryOutputFile outFile(onWrite, outFilePath);
                        std::vector<ext::ImmutableSpan<Entry>> files;
                        files.reserve(m_files.size());
                        for (auto&& file : m_files)
                        {
                            files.emplace_back(file.entries());
                        }

                        ext::merge(progressCallback, { mergeMemory }, files, outFile, Entry::CompareLessWithReverseMove{});
                    }

                    IndexWithoutReverseMove index0 = ib0.end();
                    IndexWithReverseMove index1 = ib1.end();
                    if constexpr (useIndex)
                    {
                        writeIndexFor<IndexWithoutReverseMoveTag>(outFilePath, index0);
                        writeIndexFor<IndexWithReverseMoveTag>(outFilePath, index1);
                    }

                    return std::make_pair(std::move(index0), std::move(index1));
                }

                [[nodiscard]] std::filesystem::path pathForId(std::uint32_t id) const
                {
                    return File::pathForId(m_path, id);
                }

                [[nodiscard]] std::filesystem::path nextPath() const
                {
                    return pathForId(nextId());
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

                        m_files.emplace_back(entry.path());
                    }

                    std::sort(m_files.begin(), m_files.end());
                }
            };

            template <typename T>
            [[nodiscard]] std::vector<std::vector<T>> createBuffers(std::size_t numBuffers, std::size_t size)
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

            [[nodiscard]] EnumMap<GameLevel, ImportablePgnFilePaths> partitionPathsByLevel(ImportablePgnFiles files)
            {
                EnumMap<GameLevel, ImportablePgnFilePaths> partitioned;
                for (auto&& file : files)
                {
                    partitioned[file.level()].emplace_back(std::move(file).path());
                }
                return partitioned;
            }
        }

        struct Database final : persistence::Database
        {
        private:
            using BaseType = persistence::Database;

            static inline const DatabaseManifest m_manifest = { "db_alpha", true };

            template <typename T>
            using PerPartition = EnumMap2<GameLevel, GameResult, T>;

            template <typename T>
            using PerPartitionWithSpecificGameLevel = EnumMap<GameResult, T>;

            using PartitionStorageType = PerPartition<detail::Partition>;

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
                * cardinality<GameResult>();

            static inline const std::size_t m_pgnParserMemory = cfg::g_config["persistence"]["db_alpha"]["pgn_parser_memory"].get<MemoryAmount>();

        public:
            Database(std::filesystem::path path) :
                m_path(path),
                m_header(path)
            {
                // This calls virtual functions but it's fine
                // because this class is final.
                BaseType::initializeManifest();
                initializePartitions();
            }

            Database(std::filesystem::path path, std::size_t headerBufferMemory) :
                m_path(path),
                m_header(path, headerBufferMemory)
            {
                BaseType::initializeManifest();
                initializePartitions();
            }

            [[nodiscard]] const DatabaseManifest& manifest() const override
            {
                return m_manifest;
            }

            void clear() override
            {
                m_header.clear();
                forEach(m_partitions, [](auto&& partition, GameLevel level, GameResult result) {
                    partition.clear();
                    });
            }

            const std::filesystem::path& path() const override
            {
                return m_path;
            }

            [[nodiscard]] query::Response executeQuery(query::Request query) override
            {
                disableUnsupportedQueryFeatures(query);

                using KeyType = detail::Key;

                const query::SelectMask mask = query::selectMask(query);
                const query::SelectMask fetchChildrenMask = query::fetchChildrenSelectMask(query);

                // NOTE: It could be beneficial to have two different posQueries sets
                //       because we may want children for continuations but maybe not for transpositions.
                //       But it creates a lot of complications in the implementation
                //       and creates gains only when select == AllSeparate and 
                //       fetchChildren are different. So we just query all positions
                //       for all needed selects.
                query::PositionQueries posQueries = query::gatherPositionQueries(query);
                auto keys = getKeys(posQueries);
                std::vector<detail::PositionStats> stats(posQueries.size());

                auto cmp = typename KeyType::CompareLessWithReverseMove{};
                auto unsort = reversibleZipSort(keys, posQueries, cmp);

                // Select : Queries
                // Continuations : Continuations
                // Transpositions : Continuations | All
                // Continuations | Transpositions : Continuations | All
                // All | All
                for (GameLevel level : query.levels)
                {
                    for (GameResult result : query.results)
                    {
                        auto& partition = m_partitions[level][result];

                        switch (mask)
                        {
                        case query::SelectMask::OnlyContinuations:
                            partition.executeQueryContinuations(keys, stats, level, result);
                            break;
                        case query::SelectMask::OnlyTranspositions:
                        case query::SelectMask::AllSeparate:
                            partition.executeQueryContinuations(keys, stats, level, result);
                            partition.executeQueryAll(keys, stats, level, result);
                            computeTranspositions(query, stats);
                            break;
                        case query::SelectMask::AllCombined:
                            partition.executeQueryAll(keys, stats, level, result);
                            break;
                        }
                    }
                }

                auto results = commitStatsAsResults(query, posQueries, stats);

                auto unflattened = query::unflatten(std::move(results), query, posQueries);

                return { std::move(query), std::move(unflattened) };
            }

            void mergeAll() override
            {
                const std::size_t numPartitions = 9;
                std::size_t i = 0;
                Logger::instance().logInfo(": Merging files...");
                forEach(m_partitions, [numPartitions, &i](auto&& partition, GameLevel level, GameResult result) {

                    ++i;
                    Logger::instance().logInfo(": Merging files in partition ", i, '/', numPartitions, " : ", partition.path(), ".");

                    auto progressReport = [](const ext::ProgressReport& report) {
                        Logger::instance().logInfo(":     ", static_cast<int>(report.ratio() * 100), "%.");
                    };

                    partition.mergeAll(progressReport);
                    });
                Logger::instance().logInfo(": Finalizing...");
                Logger::instance().logInfo(": Completed.");
            }

            void replicateMergeAll(const std::filesystem::path& path) override
            {
                if (std::filesystem::exists(path) && !std::filesystem::is_empty(path))
                {
                    throw std::runtime_error("Destination for replicating merge must be empty.");
                }

                PerPartition<std::filesystem::path> partitionPaths = initializePartitionDirectories(path);

                m_header.replicateTo(path);

                const std::size_t numPartitions = 9;
                std::size_t i = 0;
                Logger::instance().logInfo(": Merging files...");
                forEach(m_partitions, [numPartitions, &i, &partitionPaths](auto&& partition, GameLevel level, GameResult result) {

                    ++i;
                    Logger::instance().logInfo(": Merging files in partition ", i, '/', numPartitions, " : ", partition.path(), ".");

                    auto progressReport = [](const ext::ProgressReport& report) {
                        Logger::instance().logInfo(":     ", static_cast<int>(report.ratio() * 100), "%.");
                    };

                    partition.replicateMergeAll(partitionPaths[level][result], progressReport);
                    });
                Logger::instance().logInfo(": Finalizing...");
                Logger::instance().logInfo(": Completed.");
            }

            ImportStats import(
                std::execution::parallel_unsequenced_policy,
                const ImportablePgnFiles& pgns,
                std::size_t memory,
                std::size_t numThreads = std::thread::hardware_concurrency()
            ) override
            {
                if (pgns.empty())
                {
                    return {};
                }

                if (numThreads <= 4)
                {
                    return import(std::execution::seq, pgns, memory);
                }

                const std::size_t numWorkerThreads = numThreads / 4;
                const std::size_t numSortingThreads = numThreads - numWorkerThreads;

                auto pathsByLevel = detail::partitionPathsByLevel(pgns);

                const std::size_t numBuffers = cardinality<GameResult>() * numWorkerThreads;

                const std::size_t numAdditionalBuffers = numBuffers * 2;

                const std::size_t bucketSize =
                    ext::numObjectsPerBufferUnit<detail::Entry>(
                        memory,
                        numBuffers + numAdditionalBuffers
                        );

                detail::AsyncStorePipeline pipeline(
                    detail::createBuffers<detail::Entry>(numBuffers + numAdditionalBuffers, bucketSize),
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
                collectFutureFiles();

                flush();

                return stats;
            }

            ImportStats import(
                std::execution::sequenced_policy,
                const ImportablePgnFiles& pgns,
                std::size_t memory
            ) override
            {
                const std::size_t numSortingThreads = std::clamp(std::thread::hardware_concurrency(), 1u, 3u) - 1u;

                if (pgns.empty())
                {
                    return {};
                }

                std::size_t totalSize = 0;
                std::size_t totalSizeProcessed = 0;
                for (auto&& pgn : pgns)
                {
                    totalSize += std::filesystem::file_size(pgn.path());
                }

                auto pathsByLevel = detail::partitionPathsByLevel(pgns);

                const std::size_t numBuffers = cardinality<GameResult>();

                const std::size_t numAdditionalBuffers = numBuffers * 2;

                const std::size_t bucketSize =
                    ext::numObjectsPerBufferUnit<detail::Entry>(
                        memory,
                        numBuffers + numAdditionalBuffers
                        );

                detail::AsyncStorePipeline pipeline(
                    detail::createBuffers<detail::Entry>(numBuffers + numAdditionalBuffers, bucketSize),
                    numSortingThreads
                );

                ImportStats statsTotal{};
                Logger::instance().logInfo(": Importing pgns...");
                for (auto level : values<GameLevel>())
                {
                    if (pathsByLevel[level].empty())
                    {
                        continue;
                    }

                    statsTotal += importPgnsImpl(std::execution::seq, pipeline, pathsByLevel[level], level, [&totalSize, &totalSizeProcessed](auto&& pgn) {
                        totalSizeProcessed += std::filesystem::file_size(pgn);
                        Logger::instance().logInfo(":     ", static_cast<int>(static_cast<double>(totalSizeProcessed) / totalSize * 100.0), "% - completed ", pgn, ".");
                        });
                }
                Logger::instance().logInfo(": Finalizing...");

                pipeline.waitForCompletion();
                collectFutureFiles();

                flush();

                Logger::instance().logInfo(": Completed.");

                Logger::instance().logInfo(": Imported ", statsTotal.numGames, " games with ", statsTotal.numPositions, " positions. Skipped ", statsTotal.numSkippedGames, " games.");

                return statsTotal;
            }

            ImportStats import(const ImportablePgnFiles& pgns, std::size_t memory) override
            {
                return import(std::execution::seq, pgns, memory);
            }

            void flush() override
            {
                m_header.flush();
            }

        private:
            std::filesystem::path m_path;

            Header m_header;
            PartitionStorageType m_partitions;

            void collectFutureFiles()
            {
                forEach(m_partitions, [this](auto& bucket, GameLevel level, GameResult result) {
                    m_partitions[level][result].collectFutureFiles();
                    });
            }

            [[nodiscard]] std::vector<PackedGameHeader> queryHeadersByIndices(const std::vector<std::uint32_t>& indices)
            {
                return m_header.queryByIndices(indices);
            }

            void disableUnsupportedQueryFeatures(query::Request& query) const
            {
                for (auto&& [select, fetch] : query.fetchingOptions)
                {
                    if (select == query::Select::Transpositions || select == query::Select::All)
                    {
                        fetch.fetchFirstGame = false;
                        fetch.fetchFirstGameForEachChild = false;
                        fetch.fetchLastGame = false;
                        fetch.fetchLastGameForEachChild = false;
                    }
                }
            }

            [[nodiscard]] std::vector<detail::Key> getKeys(const query::PositionQueries& queries)
            {
                std::vector<detail::Key> keys;
                keys.reserve(queries.size());
                for (auto&& q : queries)
                {
                    keys.emplace_back(q.position, q.reverseMove);
                }
                return keys;
            }

            [[nodiscard]] query::PositionQueryResults commitStatsAsResults(
                const query::Request& query,
                const query::PositionQueries& posQueries,
                std::vector<detail::PositionStats>& stats)
            {
                query::PositionQueryResults results(posQueries.size());
                std::vector<std::uint32_t> indices;
                std::vector<query::GameHeaderDestination> destinations;
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
                                results[i][select].emplace(level, result, entry.count);

                                if (lookup[origin][select].fetchFirst && entry.firstGameFile != nullptr)
                                {
                                    auto e = entry.firstGameFile->at(entry.firstGameEntryIdx);
                                    indices.emplace_back(e.gameIdx());
                                    destinations.emplace_back(i, select, level, result, &query::Entry::firstGame);
                                }
                                if (lookup[origin][select].fetchLast && entry.lastGameFile != nullptr)
                                {
                                    auto e = entry.lastGameFile->at(entry.lastGameEntryIdx);
                                    indices.emplace_back(e.gameIdx());
                                    destinations.emplace_back(i, select, level, result, &query::Entry::lastGame);
                                }
                            }
                        }
                    }
                }

                query::assignGameHeaders(results, destinations, queryHeadersByIndices(indices));

                return results;
            }

            void computeTranspositions(const query::Request& query, std::vector<detail::PositionStats>& stats)
            {
                for (GameLevel level : query.levels)
                {
                    for (GameResult result : query.results)
                    {
                        for (auto&& stat : stats)
                        {
                            stat[query::Select::Transpositions][level][result].count =
                                stat[query::Select::All][level][result].count
                                - stat[query::Select::Continuations][level][result].count;
                        }
                    }
                }
            }
            
            ImportStats importPgnsImpl(
                std::execution::sequenced_policy,
                detail::AsyncStorePipeline& pipeline,
                const ImportablePgnFilePaths& paths,
                GameLevel level,
                std::function<void(const std::filesystem::path& file)> completionCallback
            )
            {
                // create buffers
                PerPartitionWithSpecificGameLevel<std::vector<detail::Entry>> buckets;
                forEach(buckets, [&](auto& bucket, GameResult result) {
                    bucket = pipeline.getEmptyBuffer();
                    });

                ImportStats stats{};
                for (auto& path : paths)
                {
                    pgn::LazyPgnFileReader fr(path, m_pgnParserMemory);
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
                            stats.numSkippedGames += 1;
                            continue;
                        }

                        const std::uint32_t gameIdx = m_header.nextGameId();

                        std::size_t numPositionsInGame = 0;
                        auto processPosition = [&](const Position& position, const ReverseMove& reverseMove) {
                            auto& bucket = buckets[*result];
                            bucket.emplace_back(position, reverseMove, gameIdx);
                            numPositionsInGame += 1;

                            if (bucket.size() == bucket.capacity())
                            {
                                store(pipeline, bucket, level, *result);
                            }
                        };

                        Position position = Position::startPosition();
                        ReverseMove reverseMove{};
                        processPosition(position, reverseMove);
                        for (auto& san : game.moves())
                        {
                            const Move move = san::sanToMove(position, san);
                            if (move == Move::null())
                            {
                                break;
                            }

                            reverseMove = position.doMove(move);
                            processPosition(position, reverseMove);
                        }

                        ASSERT(numPositionsInGame > 0);

                        const std::uint32_t actualGameIdx = m_header.addGameNoLock(game, static_cast<std::uint16_t>(numPositionsInGame - 1u)).index;
                        ASSERT(actualGameIdx == gameIdx);
                        (void)actualGameIdx;

                        stats.numGames += 1;
                        stats.numPositions += numPositionsInGame;
                    }

                    completionCallback(path);
                }

                // flush buffers and return them to the pipeline for later use
                forEach(buckets, [this, &pipeline, level](auto& bucket, GameResult result) {
                    store(pipeline, std::move(bucket), level, result);
                    });

                return stats;
            }

            struct Block
            {
                typename std::vector<std::filesystem::path>::const_iterator begin;
                typename std::vector<std::filesystem::path>::const_iterator end;
                PerPartitionWithSpecificGameLevel<std::uint32_t> nextIds;
            };

            [[nodiscard]] std::vector<Block> divideIntoBlocks(
                const ImportablePgnFilePaths& paths,
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
                    // we prepare the next free file id for each file
                    // and store just one global offset because we 
                    // don't know the distribution of the games
                    // and have to assume that all positions could go
                    // into one file
                    std::uint32_t idOffset = 0;
                    PerPartitionWithSpecificGameLevel<std::uint32_t> baseNextIds{};
                    forEach(baseNextIds, [&](auto& nextId, GameResult result) {
                        nextId = m_partitions[level][result].nextId();
                        });

                    std::size_t blockSize = 0;
                    auto start = paths.begin();
                    for (int i = 0; i < paths.size(); ++i)
                    {
                        blockSize += fileSizes[i];

                        if (blockSize >= blockSizeThreshold)
                        {
                            // here we apply the offset
                            PerPartitionWithSpecificGameLevel<std::uint32_t> nextIds;
                            forEach(nextIds, [&](auto& nextId, GameResult result) {
                                nextId = baseNextIds[result] + idOffset;
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
                        forEach(nextIds, [&](auto& nextId, GameResult result) {
                            nextId = baseNextIds[result] + idOffset;
                            });
                        blocks.emplace_back(Block{ start, paths.end(), nextIds });
                    }

                    ASSERT(blocks.size() <= numBlocks);

                    blocks.resize(numBlocks);

                    ASSERT(blocks.size() == numBlocks);
                }

                return blocks;
            }

            ImportStats importPgnsImpl(
                std::execution::parallel_unsequenced_policy,
                detail::AsyncStorePipeline& pipeline,
                const ImportablePgnFilePaths& paths,
                GameLevel level,
                std::size_t bufferSize,
                std::size_t numThreads
            )
            {
                const auto blocks = divideIntoBlocks(paths, level, bufferSize, numThreads);

                // Here almost everything is as in the sequential algorithm.
                // Synchronization is handled in deeper layers.
                // We only have to force file ids (info kept in blocks) to
                // ensure proper order of resulting files.
                auto work = [&](const Block& block) {

                    PerPartitionWithSpecificGameLevel<std::vector<detail::Entry>> entries;
                    forEach(entries, [&](auto& bucket, GameResult result) {
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
                            Logger::instance().logError("Failed to open file ", path);
                            break;
                        }

                        for (auto& game : fr)
                        {
                            const std::optional<GameResult> result = game.result();
                            if (!result.has_value())
                            {
                                stats.numSkippedGames += 1;
                                continue;
                            }

                            const std::uint32_t gameIdx = m_header.addGame(game).index;

                            std::size_t numPositionsInGame = 0;
                            auto processPosition = [&, &nextIds = nextIds](const Position& position, const ReverseMove& reverseMove) {
                                auto& bucket = entries[*result];
                                bucket.emplace_back(position, gameIdx);
                                numPositionsInGame += 1;

                                if (bucket.size() == bufferSize)
                                {
                                    // Here we force the id and move to the next one.
                                    // This doesn't have to be atomic since we're the only
                                    // ones using this blocks and there is enough space left for
                                    // all files before the next already present id.
                                    auto& nextId = nextIds[*result];
                                    store(pipeline, bucket, level, *result, nextId++);
                                }
                            };

                            Position position = Position::startPosition();
                            ReverseMove reverseMove{};
                            processPosition(position, reverseMove);
                            for (auto& san : game.moves())
                            {
                                const Move move = san::sanToMove(position, san);
                                if (move == Move::null())
                                {
                                    break;
                                }

                                reverseMove = position.doMove(move);
                                processPosition(position, reverseMove);
                            }

                            ASSERT(numPositionsInGame > 0);

                            stats.numGames += 1;
                            stats.numPositions += numPositionsInGame;
                        }
                    }

                    // flush buffers and return them to the pipeline for later use
                    forEach(entries, [this, &pipeline, &nextIds = nextIds, level](auto& bucket, GameResult result) {
                        const std::uint32_t nextId = nextIds[result];
                        store(pipeline, std::move(bucket), level, result, nextId);
                        });

                    return stats;
                };

                // Schedule the work
                std::vector<std::future<ImportStats>> futureStats;
                futureStats.reserve(blocks.size());
                for (int i = 1; i < blocks.size(); ++i)
                {
                    const auto& block = blocks[i];
                    if (block.begin == block.end)
                    {
                        continue;
                    }
                    futureStats.emplace_back(std::async(std::launch::async, work, block));
                }

                ImportStats totalStats{};
                // and wait for completion, gather stats.
                // One worker is run in the main thread.
                if (!blocks.empty())
                {
                    totalStats += work(blocks.front());
                }

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
                        m_partitions[level][result].setPath(m_path / resultPath);
                    }
                }
            }

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>& entries,
                GameLevel level,
                GameResult result
            )
            {
                if (entries.empty())
                {
                    return;
                }

                auto newBuffer = pipeline.getEmptyBuffer();
                entries.swap(newBuffer);
                m_partitions[level][result].storeUnordered(pipeline, std::move(newBuffer));
            }

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>&& entries,
                GameLevel level,
                GameResult result
            )
            {
                if (entries.empty())
                {
                    return;
                }

                m_partitions[level][result].storeUnordered(pipeline, std::move(entries));
            }

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>& entries,
                GameLevel level,
                GameResult result,
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
                m_partitions[level][result].storeUnordered(pipeline, std::move(newBuffer), id);
            }

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>&& entries,
                GameLevel level,
                GameResult result,
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

                m_partitions[level][result].storeUnordered(pipeline, std::move(entries), id);
            }

            static PerPartition<std::filesystem::path> initializePartitionDirectories(const std::filesystem::path& path)
            {
                PerPartition<std::filesystem::path> paths;

                for (const auto& level : values<GameLevel>())
                {
                    const std::filesystem::path levelPath = path / m_pathByGameLevel[level];
                    for (const auto& result : values<GameResult>())
                    {
                        const std::filesystem::path resultPath = levelPath / m_pathByGameResult[result];
                        paths[level][result] = resultPath;
                        std::filesystem::create_directories(resultPath);
                    }
                }

                return paths;
            }

            template <typename T, typename FuncT>
            static void forEach(PerPartition<T>& data, FuncT&& f)
            {
                for (const auto& level : values<GameLevel>())
                {
                    for (const auto& result : values<GameResult>())
                    {
                        f(data[level][result], level, result);
                    }
                }
            }

            template <typename T, typename FuncT>
            static void forEach(const PerPartition<T>& data, FuncT&& f)
            {
                for (const auto& level : values<GameLevel>())
                {
                    for (const auto& result : values<GameResult>())
                    {
                        f(data[level][result], level, result);
                    }
                }
            }

            template <typename T, typename FuncT>
            static void forEach(PerPartitionWithSpecificGameLevel<T>& data, FuncT&& f)
            {
                for (const auto& result : values<GameResult>())
                {
                    f(data[result], result);
                }
            }
        };
    }
}