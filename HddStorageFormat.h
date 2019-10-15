#pragma once

#include "Configuration.h"
#include "Database.h"
#include "EnumMap.h"
#include "External.h"
#include "GameClassification.h"
#include "MemoryAmount.h"
#include "Pgn.h"
#include "PositionSignature.h"
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

namespace persistence
{
    namespace hdd
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

            static constexpr bool usePacked = true;

            // Have ranges of mixed values be at most this long
            static inline const std::size_t indexGranularity = cfg::g_config["persistence"]["hdd"]["index_granularity"].get<std::size_t>();

            static constexpr std::uint64_t invalidGameOffset = std::numeric_limits<std::uint64_t>::max();

            struct PackedCountAndGameOffset;

            struct SingleGame {};

            struct CountAndGameOffset
            {
                CountAndGameOffset() :
                    m_count(0),
                    m_gameOffset(invalidGameOffset)
                {
                }

                CountAndGameOffset(std::uint64_t count, std::uint64_t gameOffset) :
                    m_count(count),
                    m_gameOffset(gameOffset)
                {
                }

                CountAndGameOffset(SingleGame, std::uint64_t gameOffset) :
                    m_count(1),
                    m_gameOffset(gameOffset)
                {
                }

                CountAndGameOffset& operator+=(std::uint64_t rhs)
                {
                    m_count += rhs;
                    return *this;
                }

                CountAndGameOffset operator+(std::uint64_t rhs)
                {
                    return { m_count + rhs, m_gameOffset };
                }

                void combine(const CountAndGameOffset& rhs)
                {
                    m_count += rhs.m_count;
                    m_gameOffset = std::min(m_gameOffset, rhs.m_gameOffset);
                }

                void combine(const PackedCountAndGameOffset& rhs);

                [[nodiscard]] std::uint64_t count() const
                {
                    return m_count;
                }

                [[nodiscard]] std::uint64_t gameOffset() const
                {
                    return m_gameOffset;
                }

            private:
                std::uint64_t m_count;
                std::uint64_t m_gameOffset;
            };

            static_assert(sizeof(CountAndGameOffset) == 16);

            struct PackedCountAndGameOffset
            {
                // game offset is invalid if we don't have enough bits to store it
                // ie. count takes all the bits
                static constexpr std::uint64_t numSizeBits = 6;

                // numCountBits should always be at least 1 to avoid shifting by 64
                static constexpr std::uint64_t numDataBits = 64 - numSizeBits;

                static constexpr std::uint64_t mask = std::numeric_limits<std::uint64_t>::max();
                static constexpr std::uint64_t sizeMask = 0b111111;

                PackedCountAndGameOffset()
                {
                    setNone();
                }

                PackedCountAndGameOffset(const CountAndGameOffset& unpacked)
                {
                    pack(unpacked);
                }

                PackedCountAndGameOffset(std::uint64_t count, std::uint64_t gameOffset)
                {
                    pack(count, gameOffset);
                }

                PackedCountAndGameOffset(SingleGame, std::uint64_t gameOffset)
                {
                    pack(SingleGame{}, gameOffset);
                }

                [[nodiscard]] CountAndGameOffset unpack() const
                {
                    const std::uint64_t s = countLength();
                    const std::uint64_t countMask = mask >> (64 - s);

                    const std::uint64_t data = m_packed >> numSizeBits;

                    const std::uint64_t count = data & countMask;
                    const std::uint64_t gameOffset =
                        (s == numDataBits)
                        ? invalidGameOffset 
                        : (data >> s);

                    return { count, gameOffset };
                }

                PackedCountAndGameOffset& operator+=(std::uint64_t rhs)
                {
                    pack(unpack() + rhs);
                    return *this;
                }

                void combine(const PackedCountAndGameOffset& rhs)
                {
                    auto unpacked = unpack();

                    unpacked.combine(rhs.unpack());

                    pack(unpacked);
                }

                void combine(const CountAndGameOffset& rhs)
                {
                    auto unpacked = unpack();

                    unpacked.combine(rhs);

                    pack(unpacked);
                }

                [[nodiscard]] std::uint64_t count() const
                {
                    const std::uint64_t countMask = mask >> (64 - countLength());
                    return (m_packed >> numSizeBits) & countMask;
                }

                [[nodiscard]] std::uint64_t gameOffset() const
                {
                    const std::uint64_t s = countLength();
                    if (s == numDataBits) return invalidGameOffset;
                    return (m_packed >> (numSizeBits + s));
                }

            private:
                // from least significant:
                // 6 bits for number N of count bits, at most 58
                // N bits for count
                // 58-N bits for game offset

                std::uint64_t m_packed;

                void setNone()
                {
                    m_packed = numDataBits;
                }

                void pack(std::uint64_t count, std::uint64_t gameOffset)
                {
                    const std::uint64_t countSize = count ? intrin::msb(count) + 1 : 1;
                    const std::uint64_t gameOffsetSize = gameOffset ? intrin::msb(gameOffset) + 1 : 1;
                    if (countSize + gameOffsetSize > numDataBits)
                    {
                        // We cannot fit both so we just store count
                        m_packed = (count << numSizeBits) | numDataBits;
                    }
                    else
                    {
                        // We can fit both
                        m_packed = gameOffset;
                        m_packed <<= countSize;
                        m_packed |= count;
                        m_packed <<= numSizeBits;
                        m_packed |= countSize;
                    }
                }

                void pack(SingleGame, std::uint64_t gameOffset)
                {
                    // We assume that we can fit both.
                    // For otherwise to happen gameOffset would be too big anyway.
                    m_packed = gameOffset;
                    m_packed <<= (numSizeBits + 1);
                    m_packed |= ((1 << numSizeBits) | 1);
                }

                void pack(const CountAndGameOffset& rhs)
                {
                    pack(rhs.count(), rhs.gameOffset());
                }

                [[nodiscard]] std::uint64_t countLength() const
                {
                    return m_packed & sizeMask;
                }
            };

            static_assert(sizeof(PackedCountAndGameOffset) == 8);

            inline void CountAndGameOffset::combine(const PackedCountAndGameOffset& rhs)
            {
                combine(rhs.unpack());
            }

            using CountAndGameOffsetType = std::conditional_t<usePacked, PackedCountAndGameOffset, CountAndGameOffset>;

            struct Entry
            {
                using Signature = PositionSignatureWithReverseMoveAndGameClassification;

                Entry() = default;

                Entry(const Position& pos, const ReverseMove& reverseMove, GameLevel level, GameResult result, std::uint64_t gameOffset) :
                    m_positionSignature(pos, reverseMove, level, result),
                    m_countAndGameOffset(SingleGame{}, gameOffset)
                {
                }

                // TODO: eventually remove this overload?
                Entry(const Position& pos, GameLevel level, GameResult result, std::uint64_t gameOffset) :
                    m_positionSignature(pos, {}, level, result),
                    m_countAndGameOffset(SingleGame{}, gameOffset)
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
                        return Signature::CompareLessWithoutReverseMove{}(lhs.m_positionSignature, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Entry& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareLessWithoutReverseMove{}(lhs.m_positionSignature, rhs);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Entry& rhs) const noexcept
                    {
                        return Signature::CompareLessWithoutReverseMove{}(lhs, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareLessWithoutReverseMove{}(lhs, rhs);
                    }
                };

                struct CompareEqualWithoutReverseMove
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        return Signature::CompareEqualWithoutReverseMove{}(lhs.m_positionSignature, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Entry& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareEqualWithoutReverseMove{}(lhs.m_positionSignature, rhs);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Entry& rhs) const noexcept
                    {
                        return Signature::CompareEqualWithoutReverseMove{}(lhs, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareEqualWithoutReverseMove{}(lhs, rhs);
                    }
                };

                struct CompareLessWithReverseMove
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        return Signature::CompareLessWithReverseMove{}(lhs.m_positionSignature, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Entry& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareLessWithReverseMove{}(lhs.m_positionSignature, rhs);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Entry& rhs) const noexcept
                    {
                        return Signature::CompareLessWithReverseMove{}(lhs, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareLessWithReverseMove{}(lhs, rhs);
                    }
                };

                struct CompareEqualWithReverseMove
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        return Signature::CompareEqualWithReverseMove{}(lhs.m_positionSignature, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Entry& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareEqualWithReverseMove{}(lhs.m_positionSignature, rhs);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Entry& rhs) const noexcept
                    {
                        return Signature::CompareEqualWithReverseMove{}(lhs, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareEqualWithReverseMove{}(lhs, rhs);
                    }
                };

                // This behaves like the old operator<
                struct CompareLessFull
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        return Signature::CompareLessFull{}(lhs.m_positionSignature, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Entry& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareLessFull{}(lhs.m_positionSignature, rhs);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Entry& rhs) const noexcept
                    {
                        return Signature::CompareLessFull{}(lhs, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareLessFull{}(lhs, rhs);
                    }
                };

                struct CompareEqualFull
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        return Signature::CompareEqualFull{}(lhs.m_positionSignature, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Entry& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareEqualFull{}(lhs.m_positionSignature, rhs);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Entry& rhs) const noexcept
                    {
                        return Signature::CompareEqualFull{}(lhs, rhs.m_positionSignature);
                    }

                    [[nodiscard]] bool operator()(const Signature& lhs, const Signature& rhs) const noexcept
                    {
                        return Signature::CompareEqualFull{}(lhs, rhs);
                    }
                };

                [[nodiscard]] const Signature& positionSignature() const
                {
                    return m_positionSignature;
                }

                [[nodiscard]] std::uint64_t count() const
                {
                    return m_countAndGameOffset.count();
                }

                [[nodiscard]] std::uint64_t gameOffset() const
                {
                    return m_countAndGameOffset.gameOffset();
                }

                [[nodiscard]] GameLevel level() const
                {
                    return m_positionSignature.level();
                }

                [[nodiscard]] GameResult result() const
                {
                    return m_positionSignature.result();
                }

                [[nodiscard]] const CountAndGameOffsetType& countAndGameOffset() const
                {
                    return m_countAndGameOffset;
                }

                void combine(const Entry& rhs)
                {
                    m_countAndGameOffset.combine(rhs.m_countAndGameOffset);
                }

            private:
                Signature m_positionSignature;
                CountAndGameOffsetType m_countAndGameOffset;
            };

            static_assert(sizeof(Entry) == 16 + sizeof(CountAndGameOffsetType));
            static_assert(std::is_trivially_copyable_v<Entry>);

            using PositionStats = EnumMap<query::Select, EnumMap2<GameLevel, GameResult, CountAndGameOffset>>;

            using Index = ext::RangeIndex<typename Entry::Signature, Entry::CompareLessWithoutReverseMove>;

            [[nodiscard]] std::filesystem::path pathForIndex(const std::filesystem::path& path)
            {
                auto cpy = path;
                cpy += "_index";
                return cpy;
            }

            [[nodiscard]] auto readIndexFor(const std::filesystem::path& path)
            {
                auto indexPath = pathForIndex(path);
                return Index(ext::readFile<typename Index::EntryType>(indexPath));
            }

            void writeIndexFor(const std::filesystem::path& path, const Index& index)
            {
                auto indexPath = pathForIndex(path);
                (void)ext::writeFile<typename Index::EntryType>(indexPath, index.data(), index.size());
            }

            auto extractEntryKey = [](const Entry& entry) {
                return entry.positionSignature();
            };

            struct File
            {
                static std::filesystem::path pathForId(const std::filesystem::path& path, std::uint32_t id)
                {
                    return path / std::to_string(id);
                }

                File(std::filesystem::path path) :
                    m_entries({ ext::Pooled{}, std::move(path) }),
                    m_index(readIndexFor(m_entries.path())),
                    m_id(std::stoi(m_entries.path().filename().string()))
                {
                }

                File(ext::ImmutableSpan<Entry>&& entries) :
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

                File(ext::ImmutableSpan<Entry>&& entries, Index&& index) :
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

                [[nodiscard]] Entry at(std::size_t idx) const
                {
                    return m_entries[idx];
                }

                [[nodiscard]] const ext::ImmutableSpan<Entry>& entries() const
                {
                    return m_entries;
                }

                void printInfo(std::ostream& out) const
                {
                    std::cout << "Location: " << m_entries.path() << "\n";
                    std::cout << "Entry count: " << m_entries.size() << "\n";
                    std::cout << "Index size: " << m_index.size() << "\n";
                }

                void accumulateStatsFromEntries(
                    const std::vector<Entry>& entries,
                    const query::Request& query,
                    const PositionSignatureWithReverseMoveAndGameClassification& key,
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

                        for(auto&& entry : entries)
                        {
                            const GameLevel level = entry.level();
                            const GameResult result = entry.result();

                            if (
                                   (select == query::Select::Continuations && Entry::CompareEqualWithReverseMove{}(entry, key))
                                || (select == query::Select::Transpositions && Entry::CompareEqualWithoutReverseMove{}(entry, key) && !Entry::CompareEqualWithReverseMove{}(entry, key))
                                || (select == query::Select::All && Entry::CompareEqualWithoutReverseMove{}(entry, key))
                                )
                            {
                                statsForThisSelect[level][result].combine(entry.countAndGameOffset());
                            }
                        }
                    }
                }

                void executeQuery(
                    const query::Request& query,
                    const std::vector<PositionSignatureWithReverseMoveAndGameClassification>& keys,
                    const query::PositionQueries& queries,
                    std::vector<PositionStats>& stats)
                {
                    ASSERT(queries.size() == stats.size());
                    ASSERT(queries.size() == keys.size());

                    std::vector<Entry> buffer;
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

            private:
                ext::ImmutableSpan<Entry> m_entries;
                Index m_index;
                std::uint32_t m_id;
            };

            struct FutureFile
            {
                FutureFile(std::future<Index>&& future, std::filesystem::path path) :
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
                    Job(std::filesystem::path path, std::vector<Entry>&& buffer, std::promise<Index>&& promise) :
                        path(std::move(path)),
                        buffer(std::move(buffer)),
                        promise(std::move(promise))
                    {
                    }

                    std::filesystem::path path;
                    std::vector<Entry> buffer;
                    std::promise<Index> promise;
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

                [[nodiscard]] std::future<Index> scheduleOrdered(const std::filesystem::path& path, std::vector<Entry>&& elements)
                {
                    std::unique_lock<std::mutex> lock(m_mutex);

                    std::promise<Index> promise;
                    std::future<Index> future = promise.get_future();
                    m_sortQueue.emplace(path, std::move(elements), std::move(promise));

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

                        Index index = ext::makeIndex(job.buffer, indexGranularity, Entry::CompareLessWithoutReverseMove{}, extractEntryKey);
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

                void sort(std::vector<Entry>& buffer)
                {
                    auto cmp = Entry::CompareLessFull{};
                    std::sort(buffer.begin(), buffer.end(), cmp);
                }

                // works analogously to std::unique but also combines equal values
                void combine(std::vector<Entry>& buffer)
                {
                    if (buffer.empty()) return;

                    auto read = buffer.begin();
                    auto write = buffer.begin();
                    const auto end = buffer.end();
                    auto cmp = Entry::CompareEqualFull{};

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

                void prepareData(std::vector<Entry>& buffer)
                {
                    sort(buffer);
                    combine(buffer);
                }
            };

            struct Partition
            {
                static inline const std::size_t mergeMemory = cfg::g_config["persistence"]["hdd"]["max_merge_buffer_size"].get<MemoryAmount>();

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
                    const std::vector<PositionSignatureWithReverseMoveAndGameClassification>& keys,
                    const query::PositionQueries& queries, 
                    std::vector<PositionStats>& stats)
                {
                    for (auto&& file : m_files)
                    {
                        file.executeQuery(query, keys, queries, stats);
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
                    std::filesystem::rename(pathForIndex(outFilePath), pathForIndex(newFilePath));

                    m_files.emplace_back(newFilePath, std::move(index));
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
                        std::filesystem::copy_file(pathForIndex(path), pathForIndex(outFilePath), std::filesystem::copy_options::overwrite_existing);
                    }
                    else
                    {
                        (void)mergeAllIntoFile(outFilePath, progressCallback);
                    }
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
                std::vector<File> m_files;

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

                [[nodiscard]] Index mergeAllIntoFile(const std::filesystem::path& outFilePath, std::function<void(const ext::ProgressReport&)> progressCallback) const
                {
                    ASSERT(!m_files.empty());

                    ext::IndexBuilder<Entry, Entry::CompareLessWithoutReverseMove, decltype(extractEntryKey)> ib(indexGranularity, {}, extractEntryKey);
                    {
                        auto onWrite = [&ib](const std::byte* data, std::size_t elementSize, std::size_t count) {
                            ib.append(reinterpret_cast<const Entry*>(data), count);
                        };

                        ext::ObservableBinaryOutputFile outFile(onWrite, outFilePath);
                        std::vector<ext::ImmutableSpan<Entry>> files;
                        files.reserve(m_files.size());
                        for (auto&& file : m_files)
                        {
                            files.emplace_back(file.entries());
                        }

                        {
                            const std::size_t outBufferSize = ext::numObjectsPerBufferUnit<Entry>(mergeMemory / 32, 2);
                            ext::BackInserter<Entry> out(outFile, ext::DoubleBuffer<Entry>(outBufferSize));

                            auto cmp = Entry::CompareEqualFull{};
                            bool first = true;
                            Entry accumulator;
                            auto append = [&](const Entry& entry) {
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

                            ext::merge_for_each(progressCallback, { mergeMemory }, files, append, Entry::CompareLessFull{});

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
        }

        struct Database final : persistence::Database
        {
        private:
            using BaseType = persistence::Database;

            static inline const std::filesystem::path partitionDirectory = "data";

            static inline const DatabaseManifest m_manifest = { "hdd", true };

            static constexpr std::size_t m_totalNumDirectories = 1;

            static inline const EnumMap<GameLevel, std::string> m_headerNames = {
                "_human",
                "_engine",
                "_server"
            };

            static inline const std::size_t m_pgnParserMemory = cfg::g_config["persistence"]["hdd"]["pgn_parser_memory"].get<MemoryAmount>();

        public:
            Database(std::filesystem::path path) :
                m_path(path),
                m_headers(makeHeaders(path)),
                m_nextGameIdx(numGamesInHeaders()),
                m_partition(path / partitionDirectory)
            {
                // This calls virtual functions but it's fine
                // because this class is final.
                BaseType::initializeManifest();
            }

            Database(std::filesystem::path path, std::size_t headerBufferMemory) :
                m_path(path),
                m_headers(makeHeaders(path, headerBufferMemory)),
                m_nextGameIdx(numGamesInHeaders()),
                m_partition(path / partitionDirectory)
            {
                BaseType::initializeManifest();
            }

            [[nodiscard]] const DatabaseManifest& manifest() const override
            {
                return m_manifest;
            }

            void printInfo(std::ostream& out) const
            {
                std::cout << "Location: " << m_path << "\n";
                m_partition.printInfo(out);
                std::cout << '\n';
            }

            void clear() override
            {
                for (auto& header : m_headers)
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

                using KeyType = PositionSignatureWithReverseMoveAndGameClassification;

                query::PositionQueries posQueries = query::gatherPositionQueries(query);
                auto keys = getKeys(posQueries);
                std::vector<detail::PositionStats> stats(posQueries.size());

                auto cmp = typename KeyType::CompareLessWithReverseMove{};
                auto unsort = reversibleZipSort(keys, posQueries, cmp);

                m_partition.executeQuery(query, keys, posQueries, stats);

                auto results = commitStatsAsResults(query, posQueries, stats);
                
                // We have to either unsort both results and posQueries, or none.
                // unflatten only needs relative order of results and posQueries to match
                // So we don't unsort any.
                auto unflattened = query::unflatten(std::move(results), query, posQueries);

                return { std::move(query), std::move(unflattened) };
            }

            void mergeAll() override
            {
                detail::log(": Merging files...");

                auto progressReport = [](const ext::ProgressReport& report) {
                    detail::log(":     ", static_cast<int>(report.ratio() * 100), "%.");
                };

                m_partition.mergeAll(progressReport);

                detail::log(": Finalizing...");
                detail::log(": Completed.");
            }

            void replicateMergeAll(const std::filesystem::path& path) override
            {
                if (std::filesystem::exists(path) && !std::filesystem::is_empty(path))
                {
                    throw std::runtime_error("Destination for replicating merge must be empty.");
                }
                std::filesystem::create_directories(path / partitionDirectory);

                for (auto& header : m_headers)
                {
                    header.replicateTo(path);
                }

                detail::log(": Merging files...");

                auto progressReport = [](const ext::ProgressReport& report) {
                    detail::log(":     ", static_cast<int>(report.ratio() * 100), "%.");
                };

                m_partition.replicateMergeAll(path / partitionDirectory, progressReport);

                detail::log(": Finalizing...");
                detail::log(": Completed.");
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

                const std::size_t numBuffers = numWorkerThreads;

                const std::size_t numAdditionalBuffers = numBuffers * 4;

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
                ImportStats stats = importPgnsImpl(std::execution::par_unseq, pipeline, pgns, bucketSize, numWorkerThreads);

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

                const std::size_t numBuffers = 1;

                const std::size_t numAdditionalBuffers = numBuffers * 4;

                const std::size_t bucketSize =
                    ext::numObjectsPerBufferUnit<detail::Entry>(
                        memory,
                        numBuffers + numAdditionalBuffers
                        );

                detail::AsyncStorePipeline pipeline(
                    detail::createBuffers<detail::Entry>(numBuffers + numAdditionalBuffers, bucketSize),
                    numSortingThreads
                );

                detail::log(": Importing pgns...");
                ImportStats statsTotal = importPgnsImpl(std::execution::seq, pipeline, pgns, [&totalSize, &totalSizeProcessed](auto&& pgn) {
                    totalSizeProcessed += std::filesystem::file_size(pgn);
                    detail::log(":     ", static_cast<int>(static_cast<double>(totalSizeProcessed) / totalSize * 100.0), "% - completed ", pgn, ".");
                    });
                detail::log(": Finalizing...");

                pipeline.waitForCompletion();
                collectFutureFiles();

                flush();

                detail::log(": Completed.");

                detail::log(": Imported ", statsTotal.numGames, " games with ", statsTotal.numPositions, " positions. Skipped ", statsTotal.numSkippedGames, " games.");

                return statsTotal;
            }

            ImportStats import(const ImportablePgnFiles& pgns, std::size_t memory) override
            {
                return import(std::execution::seq, pgns, memory);
            }

            void flush() override
            {
                for (auto& header : m_headers)
                {
                    header.flush();
                }
            }

        private:
            std::filesystem::path m_path;

            EnumMap<GameLevel, Header> m_headers;
            std::atomic<std::uint32_t> m_nextGameIdx;

            // We only have one partition for this format
            detail::Partition m_partition;

            [[nodiscard]] EnumMap<GameLevel, Header> makeHeaders(const std::filesystem::path& path)
            {
                return { 
                    Header(path, Header::defaultMemory, m_headerNames[values<GameLevel>()[0]]),
                    Header(path, Header::defaultMemory, m_headerNames[values<GameLevel>()[1]]),
                    Header(path, Header::defaultMemory, m_headerNames[values<GameLevel>()[2]])
                };
            }

            [[nodiscard]] EnumMap<GameLevel, Header> makeHeaders(const std::filesystem::path& path, std::size_t headerBufferMemory)
            {
                return {
                    Header(path, headerBufferMemory, m_headerNames[values<GameLevel>()[0]]),
                    Header(path, headerBufferMemory, m_headerNames[values<GameLevel>()[1]]),
                    Header(path, headerBufferMemory, m_headerNames[values<GameLevel>()[2]])
                };
            }

            [[nodiscard]] std::uint32_t numGamesInHeaders() const
            {
                std::uint32_t total = 0;

                for (auto& header : m_headers)
                {
                    total += header.numGames();
                }

                return total;
            }

            void collectFutureFiles()
            {
                m_partition.collectFutureFiles();
            }

            [[nodiscard]] std::vector<PackedGameHeader> queryHeadersByOffsets(const std::vector<std::uint64_t>& offsets, GameLevel level)
            {
                return m_headers[level].queryByOffsets(offsets);
            }

            [[nodiscard]] std::vector<GameHeader> queryHeadersByOffsets(const std::vector<std::uint64_t>& offsets, const std::vector<query::GameHeaderDestination>& destinations)
            {
                EnumMap<GameLevel, std::vector<std::uint64_t>> offsetsByLevel;
                EnumMap<GameLevel, std::vector<std::size_t>> indices;

                for (std::size_t i = 0; i < offsets.size(); ++i)
                {
                    offsetsByLevel[destinations[i].level].emplace_back(offsets[i]);
                    indices[destinations[i].level].emplace_back(i);
                }

                EnumMap<GameLevel, std::vector<PackedGameHeader>> packedHeadersByLevel;
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

            void disableUnsupportedQueryFeatures(query::Request& query) const
            {
                for (auto&& [select, fetch] : query.fetchingOptions)
                {
                    fetch.fetchLastGame = false;
                    fetch.fetchLastGameForEachChild = false;
                }
            }

            [[nodiscard]] query::PositionQueryResults commitStatsAsResults(
                const query::Request& query,
                const query::PositionQueries& posQueries,
                std::vector<detail::PositionStats>& stats)
            {
                query::PositionQueryResults results(posQueries.size());
                std::vector<std::uint64_t> offsets;
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
                                results[i][select].emplace(level, result, entry.count());

                                if (lookup[origin][select].fetchFirst && entry.gameOffset() != detail::invalidGameOffset)
                                {
                                    offsets.emplace_back(entry.gameOffset());
                                    destinations.emplace_back(i, select, level, result, &query::Entry::firstGame);
                                }
                            }
                        }
                    }
                }

                query::assignGameHeaders(results, destinations, queryHeadersByOffsets(offsets, destinations));

                return results;
            }

            [[nodiscard]] std::vector<PositionSignatureWithReverseMoveAndGameClassification> getKeys(const query::PositionQueries& queries)
            {
                std::vector<PositionSignatureWithReverseMoveAndGameClassification> keys;
                keys.reserve(queries.size());
                for (auto&& q : queries)
                {
                    keys.emplace_back(q.position, q.reverseMove);
                }
                return keys;
            }

            ImportStats importPgnsImpl(
                std::execution::sequenced_policy,
                detail::AsyncStorePipeline& pipeline,
                const ImportablePgnFiles& pgns,
                std::function<void(const std::filesystem::path& file)> completionCallback
            )
            {
                // create buffers
                std::vector<detail::Entry> bucket = pipeline.getEmptyBuffer();

                ImportStats stats{};
                for (auto& pgn : pgns)
                {
                    const auto& path = pgn.path();
                    const auto level = pgn.level();

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

                        auto& header = m_headers[level];

                        const std::uint64_t gameOffset = header.nextGameOffset();

                        std::size_t numPositionsInGame = 0;
                        auto processPosition = [&](const Position& position, const ReverseMove& reverseMove) {
                            bucket.emplace_back(position, reverseMove, level, *result, gameOffset);
                            numPositionsInGame += 1;

                            if (bucket.size() == bucket.capacity())
                            {
                                store(pipeline, bucket);
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

                        auto gameHeader = PackedGameHeader(game, m_nextGameIdx.fetch_add(1, std::memory_order_relaxed), static_cast<std::uint16_t>(numPositionsInGame - 1u));
                        const std::uint64_t actualGameOffset = header.addHeaderNoLock(gameHeader).offset;
                        ASSERT(gameOffset == actualGameOffset);
                        (void)actualGameOffset;

                        stats.numGames += 1;
                        stats.numPositions += numPositionsInGame;
                    }

                    completionCallback(path);
                }

                // flush buffers and return them to the pipeline for later use
                store(pipeline, std::move(bucket));

                return stats;
            }

            struct Block
            {
                typename ImportablePgnFiles::const_iterator begin;
                typename ImportablePgnFiles::const_iterator end;
                std::uint32_t nextId;
            };

            [[nodiscard]] std::vector<Block> divideIntoBlocks(
                const ImportablePgnFiles& pgns,
                std::size_t bufferSize,
                std::size_t numBlocks
            )
            {
                constexpr std::size_t minPgnBytesPerMove = 4;

                // We compute the total size of the files
                std::vector<std::size_t> fileSizes;
                fileSizes.reserve(pgns.size());
                std::size_t totalFileSize = 0;
                for (auto& pgn : pgns)
                {
                    const std::size_t size = std::filesystem::file_size(pgn.path());
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
                    std::uint32_t baseNextId = m_partition.nextId();
                    
                    std::size_t blockSize = 0;
                    auto start = pgns.begin();
                    for (int i = 0; i < pgns.size(); ++i)
                    {
                        blockSize += fileSizes[i];

                        if (blockSize >= blockSizeThreshold)
                        {
                            // here we apply the offset
                            std::uint32_t nextIds = baseNextId + idOffset;

                            // store the block of desired size
                            auto end = pgns.begin() + i;
                            blocks.emplace_back(Block{ start, end, nextIds });
                            start = end;
                            idOffset += static_cast<std::uint32_t>(blockSize / (bufferSize * minPgnBytesPerMove)) + 1u;
                            blockSize = 0;
                        }
                    }

                    // if anything is left over we have to handle it here as in the
                    // loop we only handle full blocks; last one may be only partially full
                    if (start != pgns.end())
                    {
                        std::uint32_t nextId = baseNextId + idOffset;
                        blocks.emplace_back(Block{ start, pgns.end(), nextId });
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
                const ImportablePgnFiles& paths,
                std::size_t bufferSize,
                std::size_t numThreads
            )
            {
                const auto blocks = divideIntoBlocks(paths, bufferSize, numThreads);

                // Here almost everything is as in the sequential algorithm.
                // Synchronization is handled in deeper layers.
                // We only have to force file ids (info kept in blocks) to
                // ensure proper order of resulting files.
                auto work = [&](const Block& block) {

                    std::vector<detail::Entry> entries = pipeline.getEmptyBuffer();

                    ImportStats stats{};
                    auto [begin, end, nextId] = block;

                    for (; begin != end; ++begin)
                    {
                        auto& pgn = *begin;
                        const auto& path = pgn.path();
                        const auto level = pgn.level();

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

                            auto& header = m_headers[level];

                            auto gameHeader = PackedGameHeader(game, m_nextGameIdx.fetch_add(1));
                            const std::uint64_t gameOffset = header.addHeader(gameHeader).offset;

                            std::size_t numPositionsInGame = 0;
                            auto processPosition = [&, &nextId = nextId](const Position& position, const ReverseMove& reverseMove) {
                                entries.emplace_back(position, level, *result, gameOffset);
                                numPositionsInGame += 1;

                                if (entries.size() == bufferSize)
                                {
                                    // Here we force the id and move to the next one.
                                    // This doesn't have to be atomic since we're the only
                                    // ones using this blocks and there is enough space left for
                                    // all files before the next already present id.
                                    store(pipeline, entries, nextId++);
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
                    store(pipeline, std::move(entries), nextId);

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

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>& entries
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
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>&& entries
            )
            {
                if (entries.empty())
                {
                    return;
                }

                m_partition.storeUnordered(pipeline, std::move(entries));
            }

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>& entries,
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
                m_partition.storeUnordered(pipeline, std::move(newBuffer), id);
            }

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>&& entries,
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

                m_partition.storeUnordered(pipeline, std::move(entries), id);
            }
        };
    }
}
