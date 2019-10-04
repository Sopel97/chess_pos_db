#pragma once

#include "Configuration.h"
#include "EnumMap.h"
#include "External.h"
#include "GameClassification.h"
#include "MemoryAmount.h"
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

#include "lib/infint/InfInt.h"

namespace persistence
{
    namespace local
    {
        namespace detail
        {
            decltype(auto) timestamp()
            {
                auto time_now = std::time(nullptr);
                return std::put_time(std::localtime(&time_now), "%Y-%m-%d %OH:%OM:%OS");
            }

            template <typename... ArgsTs>
            void log(ArgsTs&& ... args)
            {
                std::cerr << timestamp();
                (std::cerr << ... << args);
                std::cerr << '\n';
            }

            static constexpr bool useIndex = true;

            // Have ranges of mixed values be at most this long
            static inline const std::size_t indexGranularity = cfg::g_config["persistence"]["local"]["index_granularity"].get<std::size_t>();

            struct Entry
            {
                Entry() = default;

                Entry(const Position& pos, const ReverseMove& reverseMove, std::uint32_t gameIdx) :
                    m_positionSignature(pos, reverseMove),
                    m_gameIdx(gameIdx)
                {
                }

                // TODO: eventually remove this overload?
                Entry(const Position& pos, std::uint32_t gameIdx) :
                    m_positionSignature(pos),
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
                        return PositionSignatureWithReverseMove::CompareLessWithoutReverseMove{}(lhs.m_positionSignature, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Entry& lhs, const PositionSignatureWithReverseMove& rhs) const noexcept
                    {
                        return PositionSignatureWithReverseMove::CompareLessWithoutReverseMove{}(lhs.m_positionSignature, rhs);
                    }

                    [[nodiscard]] bool operator()(const PositionSignatureWithReverseMove& lhs, const Entry& rhs) const noexcept
                    {
                        return PositionSignatureWithReverseMove::CompareLessWithoutReverseMove{}(lhs, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const PositionSignatureWithReverseMove& lhs, const PositionSignatureWithReverseMove& rhs) const noexcept
                    {
                        return PositionSignatureWithReverseMove::CompareLessWithoutReverseMove{}(lhs, rhs);
                    }
                };

                // This behaves like the old operator<
                struct CompareLessWithReverseMove
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        return PositionSignatureWithReverseMove::CompareLessWithReverseMove{}(lhs.m_positionSignature, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Entry& lhs, const PositionSignatureWithReverseMove& rhs) const noexcept
                    {
                        return PositionSignatureWithReverseMove::CompareLessWithReverseMove{}(lhs.m_positionSignature, rhs);
                    }

                    [[nodiscard]] bool operator()(const PositionSignatureWithReverseMove& lhs, const Entry& rhs) const noexcept
                    {
                        return PositionSignatureWithReverseMove::CompareLessWithReverseMove{}(lhs, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const PositionSignatureWithReverseMove& lhs, const PositionSignatureWithReverseMove& rhs) const noexcept
                    {
                        return PositionSignatureWithReverseMove::CompareLessWithReverseMove{}(lhs, rhs);
                    }
                };

                [[nodiscard]] const PositionSignatureWithReverseMove& positionSignature() const
                {
                    return m_positionSignature;
                }

                [[nodiscard]] std::uint32_t gameIdx() const
                {
                    return m_gameIdx;
                }

            private:
                PositionSignatureWithReverseMove m_positionSignature;
                std::uint32_t m_gameIdx;
            };

            static_assert(sizeof(Entry) == 20);
            static_assert(std::is_trivially_copyable_v<Entry>);

            using IndexWithoutReverseMove = ext::RangeIndex<PositionSignatureWithReverseMove, detail::Entry::CompareLessWithoutReverseMove>;
            using IndexWithReverseMove = ext::RangeIndex<PositionSignatureWithReverseMove, detail::Entry::CompareLessWithReverseMove>;

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
                return entry.positionSignature();
            };

            auto entryKeyToArithmetic = [](const PositionSignatureWithReverseMove& sig) {
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

            auto entryKeyToArithmeticWithoutReverseMove = [](const PositionSignatureWithReverseMove& sig) {
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
        }

        struct QueryResult;

        struct File
        {
            static std::filesystem::path pathForId(const std::filesystem::path& path, std::uint32_t id)
            {
                return path / std::to_string(id);
            }

            File(std::filesystem::path path) :
                m_entries({ ext::Pooled{}, std::move(path) }),
                m_indexWithoutReverseMove(detail::readIndexFor<detail::IndexWithoutReverseMoveTag>(m_entries.path())),
                m_indexWithReverseMove(detail::readIndexFor<detail::IndexWithReverseMoveTag>(m_entries.path())),
                m_id(std::stoi(m_entries.path().filename().string()))
            {
            }

            File(ext::ImmutableSpan<detail::Entry>&& entries) :
                m_entries(std::move(entries)),
                m_indexWithoutReverseMove(detail::readIndexFor<detail::IndexWithoutReverseMoveTag>(m_entries.path())),
                m_indexWithReverseMove(detail::readIndexFor<detail::IndexWithReverseMoveTag>(m_entries.path())),
                m_id(std::stoi(m_entries.path().filename().string()))
            {
            }

            File(std::filesystem::path path, detail::Indexes&& index) :
                m_entries({ ext::Pooled{}, std::move(path) }),
                m_indexWithoutReverseMove(std::move(index.first)),
                m_indexWithReverseMove(std::move(index.second)),
                m_id(std::stoi(m_entries.path().filename().string()))
            {
            }

            File(ext::ImmutableSpan<detail::Entry>&& entries, detail::Indexes&& index) :
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

            [[nodiscard]] detail::Entry at(std::size_t idx) const
            {
                return m_entries[idx];
            }

            [[nodiscard]] const ext::ImmutableSpan<detail::Entry>& entries() const
            {
                return m_entries;
            }

            void printInfo(std::ostream& out) const
            {
                std::cout << "Location: " << m_entries.path() << "\n";
                std::cout << "Entry count: " << m_entries.size() << "\n";
                std::cout << "Index size: " << m_indexWithoutReverseMove.size() << "\n";
                std::cout << "Direct Index size: " << m_indexWithReverseMove.size() << "\n";
            }

            void queryDirectRanges(std::vector<QueryResult>& results, const std::vector<PositionSignatureWithReverseMove>& keys) const;
            void queryRanges(std::vector<QueryResult>& results, const std::vector<PositionSignatureWithReverseMove>& keys) const;

        private:
            ext::ImmutableSpan<detail::Entry> m_entries;
            detail::IndexWithoutReverseMove m_indexWithoutReverseMove;
            detail::IndexWithReverseMove m_indexWithReverseMove;
            std::uint32_t m_id;
        };

        struct QueryTarget
        {
            GameLevel level;
            GameResult result;
        };

        struct QueryResultRange
        {
            QueryResultRange(const File& file, std::size_t begin, std::size_t end) :
                m_file(&file),
                m_begin(begin),
                m_end(end)
            {
            }

            [[nodiscard]] const File& file() const
            {
                return *m_file;
            }

            [[nodiscard]] std::size_t begin() const
            {
                return m_begin;
            }

            [[nodiscard]] std::size_t end() const
            {
                return m_end;
            }

            void print() const
            {
                std::cout << m_file->path() << ' ' << m_begin << ' ' << m_end << '\n';
            }

            [[nodiscard]] std::size_t count() const
            {
                return m_end - m_begin;
            }

            [[nodiscard]] std::uint32_t firstGameIndex() const
            {
                ASSERT(m_begin != m_end);

                return m_file->at(m_begin).gameIdx();
            }

            [[nodiscard]] std::uint32_t lastGameIndex() const
            {
                ASSERT(m_begin != m_end);

                return m_file->at(m_end - 1u).gameIdx();
            }

        private:
            const File* m_file;
            std::size_t m_begin;
            std::size_t m_end;
        };

        struct QueryResult
        {
            template <typename... ArgsTs>
            void emplaceRange(ArgsTs&& ... args)
            {
                m_ranges.emplace_back(std::forward<ArgsTs>(args)...);
            }

            template <typename... ArgsTs>
            void emplaceDirectRange(ArgsTs&& ... args)
            {
                m_directRanges.emplace_back(std::forward<ArgsTs>(args)...);
            }

            void print() const
            {
                for (auto&& range : m_ranges)
                {
                    range.print();
                }
                std::cout << '\n';
            }

            [[nodiscard]] std::size_t count() const
            {
                std::size_t c = 0;
                for (auto&& range : m_ranges)
                {
                    c += range.count();
                }
                return c;
            }

            [[nodiscard]] std::size_t directCount()
            {
                std::size_t c = 0;
                for (auto&& range : m_directRanges)
                {
                    c += range.count();
                }
                return c;
            }

            [[nodiscard]] std::uint32_t firstGameIndex() const
            {
                ASSERT(!m_ranges.empty());

                return m_ranges.front().firstGameIndex();
            }

            [[nodiscard]] std::uint32_t lastGameIndex() const
            {
                ASSERT(!m_ranges.empty());

                return m_ranges.back().lastGameIndex();
            }

            [[nodiscard]] std::uint32_t firstDirectGameIndex() const
            {
                ASSERT(!m_directRanges.empty());

                return m_directRanges.front().firstGameIndex();
            }

            [[nodiscard]] std::uint32_t lastDirectGameIndex() const
            {
                ASSERT(!m_directRanges.empty());

                return m_directRanges.back().lastGameIndex();
            }

        private:
            // Where the position hash matches
            std::vector<QueryResultRange> m_ranges;

            // Where both position hash and reverse move match
            std::vector<QueryResultRange> m_directRanges;
        };

        void File::queryDirectRanges(std::vector<QueryResult>& results, const std::vector<PositionSignatureWithReverseMove>& keys) const
        {
            auto searchResults = [this, &keys]() {
                if constexpr (detail::useIndex)
                {
                    return ext::equal_range_multiple_interp_indexed_cross(
                        m_entries,
                        m_indexWithReverseMove,
                        keys,
                        detail::Entry::CompareLessWithReverseMove{},
                        detail::extractEntryKey,
                        detail::entryKeyToArithmetic,
                        detail::entryKeyArithmeticToSizeT
                    );
                }
                else
                {
                    return ext::equal_range_multiple_interp_cross(
                        m_entries,
                        keys,
                        detail::Entry::CompareLessWithReverseMove{},
                        detail::extractEntryKey,
                        detail::entryKeyToArithmetic,
                        detail::entryKeyArithmeticToSizeT
                    );
                }
            }();
            for (int i = 0; i < searchResults.size(); ++i)
            {
                const auto& result = searchResults[i];
                const auto count = result.second - result.first;
                if (count == 0) continue;

                results[i].emplaceDirectRange(*this, result.first, result.second);
            }
        }
        
        void File::queryRanges(std::vector<QueryResult>& results, const std::vector<PositionSignatureWithReverseMove>& keys) const
        {
            auto searchResults = [this, &keys]() {
                if constexpr (detail::useIndex)
                {
                    return ext::equal_range_multiple_interp_indexed_cross(
                        m_entries, 
                        m_indexWithoutReverseMove, 
                        keys, 
                        detail::Entry::CompareLessWithoutReverseMove{},
                        detail::extractEntryKey,
                        detail::entryKeyToArithmeticWithoutReverseMove,
                        detail::entryKeyArithmeticToSizeT
                    );
                }
                else
                {
                    return ext::equal_range_multiple_interp_cross(
                        m_entries, 
                        keys, 
                        detail::Entry::CompareLessWithoutReverseMove{},
                        detail::extractEntryKey,
                        detail::entryKeyToArithmeticWithoutReverseMove,
                        detail::entryKeyArithmeticToSizeT
                    );
                }
            }();
            for (int i = 0; i < searchResults.size(); ++i)
            {
                const auto& result = searchResults[i];
                const auto count = result.second - result.first;
                if (count == 0) continue;

                results[i].emplaceRange(*this, result.first, result.second);
            }
        }

        struct FutureFile
        {
            FutureFile(std::future<detail::Indexes>&& future, std::filesystem::path path) :
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
                detail::Indexes indexes = m_future.get();
                return { m_path, std::move(indexes) };
            }

        private:
            std::future<detail::Indexes> m_future;
            std::filesystem::path m_path;
            std::uint32_t m_id;
        };

        struct AsyncStorePipeline
        {
        private:
            struct Job
            {
                Job(std::filesystem::path path, std::vector<detail::Entry>&& buffer, std::promise<detail::Indexes>&& promise, bool createIndex = detail::useIndex) :
                    path(std::move(path)),
                    buffer(std::move(buffer)),
                    promise(std::move(promise)),
                    createIndex(createIndex)
                {
                }

                std::filesystem::path path;
                std::vector<detail::Entry> buffer;
                std::promise<detail::Indexes> promise;
                bool createIndex;
            };

        public:
            AsyncStorePipeline(std::vector<std::vector<detail::Entry>>&& buffers, std::size_t numSortingThreads = 1) :
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

            [[nodiscard]] std::future<detail::Indexes> scheduleUnordered(const std::filesystem::path& path, std::vector<detail::Entry>&& elements, bool createIndex = detail::useIndex)
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                std::promise<detail::Indexes> promise;
                std::future<detail::Indexes> future = promise.get_future();
                m_sortQueue.emplace(path, std::move(elements), std::move(promise), createIndex);

                lock.unlock();
                m_sortQueueNotEmpty.notify_one();

                return future;
            }

            [[nodiscard]] std::future<detail::Indexes> scheduleOrdered(const std::filesystem::path& path, std::vector<detail::Entry>&& elements, bool createIndex = detail::useIndex)
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                std::promise<detail::Indexes> promise;
                std::future<detail::Indexes> future = promise.get_future();
                m_sortQueue.emplace(path, std::move(elements), std::move(promise), createIndex);

                lock.unlock();
                m_writeQueueNotEmpty.notify_one();

                return future;
            }

            [[nodiscard]] std::vector<detail::Entry> getEmptyBuffer()
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
            std::queue<std::vector<detail::Entry>> m_bufferQueue;

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
                    auto cmp = detail::Entry::CompareLessWithReverseMove{};
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
                        detail::IndexWithoutReverseMove index0 = ext::makeIndex(job.buffer, detail::indexGranularity, detail::Entry::CompareLessWithoutReverseMove{}, detail::extractEntryKey);
                        detail::IndexWithReverseMove index1 = ext::makeIndex(job.buffer, detail::indexGranularity, detail::Entry::CompareLessWithReverseMove{}, detail::extractEntryKey);
                        detail::writeIndexFor<detail::IndexWithoutReverseMoveTag>(job.path, index0);
                        detail::writeIndexFor<detail::IndexWithReverseMoveTag>(job.path, index1);
                        job.promise.set_value(std::make_pair(std::move(index0), std::move(index1)));
                    }
                    else
                    {
                        job.promise.set_value(detail::Indexes{});
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
            static inline const std::size_t mergeMemory = cfg::g_config["persistence"]["local"]["max_merge_buffer_size"].get<MemoryAmount>();

            Partition() = default;

            Partition(std::filesystem::path path)
            {
                ASSERT(!path.empty());

                setPath(std::move(path));
            }

            void queryRanges(std::vector<QueryResult>& results, const std::vector<PositionSignatureWithReverseMove>& keys) const
            {
                for (auto&& file : m_files)
                {
                    file.queryRanges(results, keys);
                }
            }

            void queryDirectRanges(std::vector<QueryResult>& results, const std::vector<PositionSignatureWithReverseMove>& keys) const
            {
                for (auto&& file : m_files)
                {
                    file.queryDirectRanges(results, keys);
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
            void storeOrdered(const detail::Entry* data, std::size_t count)
            {
                ASSERT(!m_path.empty());

                auto path = nextPath();
                (void)ext::writeFile(path, data, count);
                m_files.emplace_back(path);
            }

            // entries have to be sorted in ascending order
            void storeOrdered(const std::vector<detail::Entry>& entries)
            {
                storeOrdered(entries.data(), entries.size());
            }

            // Uses the passed id.
            // It is required that the file with this id doesn't exist already.
            void storeUnordered(AsyncStorePipeline& pipeline, std::vector<detail::Entry>&& entries, std::uint32_t id)
            {
                ASSERT(!m_path.empty());

                std::unique_lock<std::mutex> lock(m_mutex);
                auto path = pathForId(id);
                m_futureFiles.emplace(pipeline.scheduleUnordered(path, std::move(entries)), path);
            }

            void storeUnordered(AsyncStorePipeline& pipeline, std::vector<detail::Entry>&& entries)
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

            void printInfo(std::ostream& out) const
            {
                std::cout << "Location: " << m_path << "\n";
                std::cout << "Files: \n";
                for (auto&& file : m_files)
                {
                    file.printInfo(out);
                    std::cout << "\n";
                }
            }

            void clear()
            {
                collectFutureFiles();

                while (!m_files.empty())
                {
                    auto path = m_files.back().path();
                    m_files.pop_back();

                    std::filesystem::remove(path);
                    if constexpr (detail::useIndex)
                    {
                        auto indexPath0 = detail::pathForIndex<detail::IndexWithoutReverseMoveTag>(path);
                        auto indexPath1 = detail::pathForIndex<detail::IndexWithReverseMoveTag>(path);

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
                        auto fromIndexPath0 = detail::pathForIndex<detail::IndexWithoutReverseMoveTag>(path);
                        auto toIndexPath0 = detail::pathForIndex<detail::IndexWithoutReverseMoveTag>(outFilePath);
                        std::filesystem::copy_file(fromIndexPath0, toIndexPath0, std::filesystem::copy_options::overwrite_existing);
                    }

                    {
                        auto fromIndexPath1 = detail::pathForIndex<detail::IndexWithReverseMoveTag>(path);
                        auto toIndexPath1 = detail::pathForIndex<detail::IndexWithReverseMoveTag>(outFilePath);
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

            [[nodiscard]] detail::Indexes mergeAllIntoFile(const std::filesystem::path& outFilePath, std::function<void(const ext::ProgressReport&)> progressCallback) const
            {
                ASSERT(!m_files.empty());

                ext::IndexBuilder<detail::Entry, detail::Entry::CompareLessWithoutReverseMove, decltype(detail::extractEntryKey)> ib0(detail::indexGranularity, {}, detail::extractEntryKey);
                ext::IndexBuilder<detail::Entry, detail::Entry::CompareLessWithReverseMove, decltype(detail::extractEntryKey)> ib1(detail::indexGranularity, {}, detail::extractEntryKey);
                {
                    auto onWrite = [&ib0, &ib1](const std::byte* data, std::size_t elementSize, std::size_t count) {
                        if constexpr (detail::useIndex)
                        {
                            ib0.append(reinterpret_cast<const detail::Entry*>(data), count);
                            ib1.append(reinterpret_cast<const detail::Entry*>(data), count);
                        }
                    };

                    ext::ObservableBinaryOutputFile outFile(onWrite, outFilePath);
                    std::vector<ext::ImmutableSpan<detail::Entry>> files;
                    files.reserve(m_files.size());
                    for (auto&& file : m_files)
                    {
                        files.emplace_back(file.entries());
                    }

                    ext::merge(progressCallback, { mergeMemory }, files, outFile, detail::Entry::CompareLessWithReverseMove{});
                }

                detail::IndexWithoutReverseMove index0 = ib0.end();
                detail::IndexWithReverseMove index1 = ib1.end();
                if constexpr (detail::useIndex)
                {
                    detail::writeIndexFor<detail::IndexWithoutReverseMoveTag>(outFilePath, index0);
                    detail::writeIndexFor<detail::IndexWithReverseMoveTag>(outFilePath, index1);
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

            [[nodiscard]] const auto& path() const &
            {
                return m_path;
            }

            [[nodiscard]] PgnFilePath path() &&
            {
                return std::move(m_path);
            }

            [[nodiscard]] GameLevel level() const
            {
                return m_level;
            }

        private:
            PgnFilePath m_path;
            GameLevel m_level;
        };

        using PgnFiles = std::vector<PgnFile>;

        namespace detail
        {
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

            [[nodiscard]] EnumMap<GameLevel, PgnFilePaths> partitionPathsByLevel(PgnFiles files)
            {
                EnumMap<GameLevel, PgnFilePaths> partitioned;
                for (auto&& file : files)
                {
                    partitioned[file.level()].emplace_back(std::move(file).path());
                }
                return partitioned;
            }
        }

        struct Database
        {
        private:
            static inline const std::string m_name = "local";

            template <typename T>
            using PerPartition = EnumMap2<GameLevel, GameResult, T>;

            template <typename T>
            using PerPartitionWithSpecificGameLevel = EnumMap<GameResult, T>;

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
                * cardinality<GameResult>();

            static inline const std::size_t m_pgnParserMemory = cfg::g_config["persistence"]["local"]["pgn_parser_memory"].get<MemoryAmount>();

        public:

            static const std::vector<QueryTarget>& allQueryTargets()
            {
                static const std::vector<QueryTarget> s_allQueryTargets = []() {
                    std::vector<QueryTarget> s_allQueryTargets;

                    for (auto level : values<GameLevel>())
                    {
                        for (auto result : values<GameResult>())
                        {
                            s_allQueryTargets.emplace_back(QueryTarget{ level, result });
                        }
                    }

                    return s_allQueryTargets;
                }();

                return s_allQueryTargets;
            }

            Database(std::filesystem::path path) :
                m_path(path),
                m_header(path)
            {
                initializePartitions();
            }

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

            void printInfo(std::ostream& out) const
            {
                std::cout << "Location: " << m_path << "\n";
                forEach(m_partitions, [&out](auto&& partition, GameLevel level, GameResult result) {
                    std::cout << "Partition " << static_cast<unsigned>(level) << ' ' << static_cast<unsigned>(result) << ":\n";
                    partition.printInfo(out);
                    std::cout << "\n";
                    });
            }

            void clear()
            {
                m_header.clear();
                forEach(m_partitions, [](auto&& partition, GameLevel level, GameResult result) {
                    partition.clear();
                    });
            }

            const std::filesystem::path& path() const
            {
                return m_path;
            }

            [[nodiscard]] std::vector<PackedGameHeader> queryHeadersByIndices(const std::vector<std::uint32_t>& indices)
            {
                return m_header.query(indices);
            }

            // TODO: refactor
            [[nodiscard]] EnumMap2<GameLevel, GameResult, std::vector<QueryResult>> queryRanges(
                const std::vector<QueryTarget>& targets, 
                const std::vector<Position>& positions,
                const std::vector<ReverseMove>& reverseMoves = std::vector<ReverseMove>{}
            ) const
            {
                const std::size_t numPositions = positions.size();

                std::vector<PositionSignatureWithReverseMove> orderedKeys;
                std::vector<std::size_t> originalIds;
                std::vector<std::pair<PositionSignatureWithReverseMove, std::size_t>> compound;
                orderedKeys.reserve(numPositions);
                originalIds.reserve(numPositions);
                compound.reserve(numPositions);
                for (std::size_t i = 0; i < numPositions; ++i)
                {
                    if (reverseMoves.size() > i)
                    {
                        compound.emplace_back(PositionSignatureWithReverseMove(positions[i], reverseMoves[i]), i);
                    }
                    else
                    {
                        compound.emplace_back(PositionSignatureWithReverseMove(positions[i]), i);
                    }
                }

                auto cmp = detail::Entry::CompareLessWithReverseMove{};
                std::sort(compound.begin(), compound.end(), [cmp](auto&& lhs, auto&& rhs) { return cmp(lhs.first, rhs.first); });
                for (auto&& [key, id] : compound)
                {
                    orderedKeys.emplace_back(key);
                    originalIds.emplace_back(id);
                }

                EnumMap2<GameLevel, GameResult, std::vector<QueryResult>> results;
                for (auto&& target : targets)
                {
                    const GameLevel level = target.level;
                    const GameResult result = target.result;

                    std::vector<QueryResult> orderedResults;
                    orderedResults.resize(numPositions);
                    m_partitions[level][result].queryRanges(orderedResults, orderedKeys);
                    if (!reverseMoves.empty())
                    {
                        m_partitions[level][result].queryDirectRanges(orderedResults, orderedKeys);
                    }

                    results[level][result].resize(numPositions);
                    for (std::size_t i = 0; i < numPositions; ++i)
                    {
                        results[level][result][originalIds[i]] = orderedResults[i];
                    }
                }
                return results;
            }

            [[nodiscard]] std::vector<QueryResult> queryRanges(
                QueryTarget target, 
                const std::vector<Position>& positions,
                const std::vector<ReverseMove>& reverseMoves = std::vector<ReverseMove>{}
            ) const
            {
                return queryRanges(std::vector<QueryTarget>{ { target } }, positions, reverseMoves)[target.level][target.result];
            }

            [[nodiscard]] EnumMap2<GameLevel, GameResult, std::vector<QueryResult>> queryRanges(
                const std::vector<Position>& positions,
                const std::vector<ReverseMove>& reverseMoves = std::vector<ReverseMove>{}
            ) const
            {
                return queryRanges(allQueryTargets(), positions, reverseMoves);
            }

            void mergeAll()
            {
                const std::size_t numPartitions = 9;
                std::size_t i = 0;
                detail::log(": Merging files...");
                forEach(m_partitions, [numPartitions, &i](auto&& partition, GameLevel level, GameResult result) {

                    ++i;
                    detail::log(": Merging files in partition ", i, '/', numPartitions, " : ", partition.path(), ".");

                    auto progressReport = [](const ext::ProgressReport& report) {
                        detail::log(":     ", static_cast<int>(report.ratio() * 100), "%.");
                    };

                    partition.mergeAll(progressReport);
                    });
                detail::log(": Finalizing...");
                detail::log(": Completed.");
            }

            void replicateMergeAll(const std::filesystem::path& path)
            {
                if (std::filesystem::exists(path) && !std::filesystem::is_empty(path))
                {
                    throw std::runtime_error("Destination for replicating merge must be empty.");
                }

                PerPartition<std::filesystem::path> partitionPaths = initializePartitionDirectories(path);

                m_header.replicateTo(path);

                const std::size_t numPartitions = 9;
                std::size_t i = 0;
                detail::log(": Merging files...");
                forEach(m_partitions, [numPartitions, &i, &partitionPaths](auto&& partition, GameLevel level, GameResult result) {

                    ++i;
                    detail::log(": Merging files in partition ", i, '/', numPartitions, " : ", partition.path(), ".");

                    auto progressReport = [](const ext::ProgressReport& report) {
                        detail::log(":     ", static_cast<int>(report.ratio() * 100), "%.");
                    };

                    partition.replicateMergeAll(partitionPaths[level][result], progressReport);
                    });
                detail::log(": Finalizing...");
                detail::log(": Completed.");
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

                if (numThreads <= 4)
                {
                    return importPgns(std::execution::seq, pgns, memory);
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

                AsyncStorePipeline pipeline(
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

            ImportStats importPgns(
                std::execution::sequenced_policy,
                const PgnFiles& pgns,
                std::size_t memory
            )
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

                AsyncStorePipeline pipeline(
                    detail::createBuffers<detail::Entry>(numBuffers + numAdditionalBuffers, bucketSize),
                    numSortingThreads
                );

                ImportStats statsTotal{};
                detail::log(": Importing pgns...");
                for (auto level : values<GameLevel>())
                {
                    if (pathsByLevel[level].empty())
                    {
                        continue;
                    }

                    statsTotal += importPgnsImpl(std::execution::seq, pipeline, pathsByLevel[level], level, [&totalSize, &totalSizeProcessed](auto&& pgn) {
                        totalSizeProcessed += std::filesystem::file_size(pgn);
                        detail::log(":     ", static_cast<int>(static_cast<double>(totalSizeProcessed) / totalSize * 100.0), "% - completed ", pgn, ".");
                        });
                }
                detail::log(": Finalizing...");

                pipeline.waitForCompletion();
                collectFutureFiles();

                flush();

                detail::log(": Completed.");

                detail::log(": Imported ", statsTotal.numGames, " games with ", statsTotal.numPositions, " positions. Skipped ", statsTotal.numSkippedGames, " games.");

                return statsTotal;
            }

            ImportStats importPgns(const PgnFiles& pgns, std::size_t memory)
            {
                return importPgns(std::execution::seq, pgns, memory);
            }

            void flush()
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
            
            ImportStats importPgnsImpl(
                std::execution::sequenced_policy,
                AsyncStorePipeline& pipeline,
                const PgnFilePaths& paths,
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
                        detail::log("Failed to open file ", path);
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

                        const std::uint32_t actualGameIdx = m_header.addGameNoLock(game, static_cast<std::uint16_t>(numPositionsInGame - 1u));
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
                AsyncStorePipeline& pipeline,
                const PgnFilePaths& paths,
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
                            detail::log("Failed to open file ", path);
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

                            const std::uint32_t gameIdx = m_header.addGame(game);

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
                AsyncStorePipeline& pipeline,
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
                AsyncStorePipeline& pipeline,
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
                AsyncStorePipeline& pipeline,
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
                AsyncStorePipeline& pipeline,
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