#include "DatabaseFormatBeta.h"

#include "algorithm/Unsort.h"

#include "chess/GameClassification.h"
#include "chess/Pgn.h"
#include "chess/Position.h"
#include "chess/San.h"

#include "enum/EnumArray.h"

#include "external_storage/External.h"

#include "persistence/pos_db/Database.h"
#include "persistence/pos_db/Query.h"
#include "persistence/pos_db/StorageHeader.h"

#include "util/Buffer.h"
#include "util/MemoryAmount.h"

#include "Configuration.h"
#include "Logger.h"

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
    namespace db_beta
    {
        namespace detail
        {
            const std::size_t indexGranularity = cfg::g_config["persistence"]["db_beta"]["index_granularity"].get<std::size_t>();

            Key::Key(const Position& pos, const ReverseMove& reverseMove) :
                m_hash(pos.hash())
            {
                auto packedReverseMove = PackedReverseMove(reverseMove);
                // m_hash[0] is the most significant quad, m_hash[3] is the least significant
                // We want entries ordered with reverse move to also be ordered by just hash
                // so we have to modify the lowest bits.
                m_hash[3] = (packedReverseMove.packed() << reverseMoveShift);
            }

            Key::Key(const Position& pos, const ReverseMove& reverseMove, GameLevel level, GameResult result) :
                Key(pos, reverseMove)
            {
                m_hash[3] |=
                    ((ordinal(level) & levelMask) << levelShift)
                    | ((ordinal(result) & resultMask) << resultShift);
            }

            [[nodiscard]] const Key::StorageType& Key::hash() const
            {
                return m_hash;
            }

            [[nodiscard]] GameLevel Key::level() const
            {
                return fromOrdinal<GameLevel>((m_hash[3] >> levelShift) & levelMask);
            }

            [[nodiscard]] GameResult Key::result() const
            {
                return fromOrdinal<GameResult>((m_hash[3] >> resultShift) & resultMask);
            }

            CountAndGameOffset::CountAndGameOffset() :
                m_count(0),
                m_gameOffset(invalidGameOffset)
            {
            }

            CountAndGameOffset::CountAndGameOffset(std::uint64_t count, std::uint64_t gameOffset) :
                m_count(count),
                m_gameOffset(gameOffset)
            {
            }

            CountAndGameOffset::CountAndGameOffset(SingleGame, std::uint64_t gameOffset) :
                m_count(1),
                m_gameOffset(gameOffset)
            {
            }

            CountAndGameOffset& CountAndGameOffset::operator+=(std::uint64_t rhs)
            {
                m_count += rhs;
                return *this;
            }

            CountAndGameOffset CountAndGameOffset::operator+(std::uint64_t rhs)
            {
                return { m_count + rhs, m_gameOffset };
            }

            void CountAndGameOffset::combine(const CountAndGameOffset& rhs)
            {
                m_count += rhs.m_count;
                m_gameOffset = std::min(m_gameOffset, rhs.m_gameOffset);
            }

            [[nodiscard]] std::uint64_t CountAndGameOffset::count() const
            {
                return m_count;
            }

            [[nodiscard]] std::uint64_t CountAndGameOffset::gameOffset() const
            {
                return m_gameOffset;
            }

            PackedCountAndGameOffset::PackedCountAndGameOffset()
            {
                setNone();
            }

            PackedCountAndGameOffset::PackedCountAndGameOffset(const CountAndGameOffset& unpacked)
            {
                pack(unpacked);
            }

            PackedCountAndGameOffset::PackedCountAndGameOffset(std::uint64_t count, std::uint64_t gameOffset)
            {
                pack(count, gameOffset);
            }

            PackedCountAndGameOffset::PackedCountAndGameOffset(SingleGame, std::uint64_t gameOffset)
            {
                pack(SingleGame{}, gameOffset);
            }

            [[nodiscard]] CountAndGameOffset PackedCountAndGameOffset::unpack() const
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

            PackedCountAndGameOffset& PackedCountAndGameOffset::operator+=(std::uint64_t rhs)
            {
                pack(unpack() + rhs);
                return *this;
            }

            void PackedCountAndGameOffset::combine(const PackedCountAndGameOffset& rhs)
            {
                auto unpacked = unpack();

                unpacked.combine(rhs.unpack());

                pack(unpacked);
            }

            void PackedCountAndGameOffset::combine(const CountAndGameOffset& rhs)
            {
                auto unpacked = unpack();

                unpacked.combine(rhs);

                pack(unpacked);
            }

            [[nodiscard]] std::uint64_t PackedCountAndGameOffset::count() const
            {
                const std::uint64_t countMask = mask >> (64 - countLength());
                return (m_packed >> numSizeBits) & countMask;
            }

            [[nodiscard]] std::uint64_t PackedCountAndGameOffset::gameOffset() const
            {
                const std::uint64_t s = countLength();
                if (s == numDataBits) return invalidGameOffset;
                return (m_packed >> (numSizeBits + s));
            }

            void PackedCountAndGameOffset::setNone()
            {
                m_packed = numDataBits;
            }

            void PackedCountAndGameOffset::pack(std::uint64_t count, std::uint64_t gameOffset)
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

            void PackedCountAndGameOffset::pack(SingleGame, std::uint64_t gameOffset)
            {
                // We assume that we can fit both.
                // For otherwise to happen gameOffset would be too big anyway.
                m_packed = gameOffset;
                m_packed <<= (numSizeBits + 1);
                m_packed |= ((1 << numSizeBits) | 1);
            }

            void PackedCountAndGameOffset::pack(const CountAndGameOffset& rhs)
            {
                pack(rhs.count(), rhs.gameOffset());
            }

            [[nodiscard]] std::uint64_t PackedCountAndGameOffset::countLength() const
            {
                return m_packed & sizeMask;
            }

            void CountAndGameOffset::combine(const PackedCountAndGameOffset& rhs)
            {
                combine(rhs.unpack());
            }

            using CountAndGameOffsetType = std::conditional_t<usePacked, PackedCountAndGameOffset, CountAndGameOffset>;

            Entry::Entry(const Position& pos, const ReverseMove& reverseMove, GameLevel level, GameResult result, std::uint64_t gameOffset) :
                m_key(pos, reverseMove, level, result),
                m_countAndGameOffset(SingleGame{}, gameOffset)
            {
            }

            [[nodiscard]] const Key& Entry::key() const
            {
                return m_key;
            }

            [[nodiscard]] std::uint64_t Entry::count() const
            {
                return m_countAndGameOffset.count();
            }

            [[nodiscard]] std::uint64_t Entry::gameOffset() const
            {
                return m_countAndGameOffset.gameOffset();
            }

            [[nodiscard]] GameLevel Entry::level() const
            {
                return m_key.level();
            }

            [[nodiscard]] GameResult Entry::result() const
            {
                return m_key.result();
            }

            [[nodiscard]] const CountAndGameOffsetType& Entry::countAndGameOffset() const
            {
                return m_countAndGameOffset;
            }

            void Entry::combine(const Entry& rhs)
            {
                m_countAndGameOffset.combine(rhs.m_countAndGameOffset);
            }

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

            static auto extractEntryKey = [](const Entry& entry) {
                return entry.key();
            };

            std::filesystem::path File::pathForId(const std::filesystem::path& path, std::uint32_t id)
            {
                return path / std::to_string(id);
            }

            File::File(std::filesystem::path path) :
                m_entries({ ext::Pooled{}, std::move(path) }),
                m_index(readIndexFor(m_entries.path())),
                m_id(std::stoi(m_entries.path().filename().string()))
            {
            }

            File::File(ext::ImmutableSpan<Entry>&& entries) :
                m_entries(std::move(entries)),
                m_index(readIndexFor(m_entries.path())),
                m_id(std::stoi(m_entries.path().filename().string()))
            {
            }

            File::File(std::filesystem::path path, Index&& index) :
                m_entries({ ext::Pooled{}, std::move(path) }),
                m_index(std::move(index)),
                m_id(std::stoi(m_entries.path().filename().string()))
            {
            }

            File::File(ext::ImmutableSpan<Entry>&& entries, Index&& index) :
                m_entries(std::move(entries)),
                m_index(std::move(index)),
                m_id(std::stoi(m_entries.path().filename().string()))
            {
            }

            [[nodiscard]] bool operator<(const File& lhs, const File& rhs) noexcept
            {
                return lhs.m_id < rhs.m_id;
            }

            [[nodiscard]] std::uint32_t File::id() const
            {
                return m_id;
            }

            [[nodiscard]] const std::filesystem::path& File::path() const
            {
                return m_entries.path();
            }

            [[nodiscard]] Entry File::at(std::size_t idx) const
            {
                return m_entries[idx];
            }

            [[nodiscard]] const ext::ImmutableSpan<Entry>& File::entries() const
            {
                return m_entries;
            }

            void File::accumulateStatsFromEntries(
                const std::vector<Entry>& entries,
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

            void File::executeQuery(
                const query::Request& query,
                const std::vector<Key>& keys,
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

            FutureFile::FutureFile(std::future<Index>&& future, std::filesystem::path path) :
                m_future(std::move(future)),
                m_path(std::move(path)),
                m_id(std::stoi(m_path.filename().string()))
            {
            }

            [[nodiscard]] bool operator<(const FutureFile& lhs, const FutureFile& rhs) noexcept
            {
                return lhs.m_id < rhs.m_id;
            }

            [[nodiscard]] std::uint32_t FutureFile::id() const
            {
                return m_id;
            }

            [[nodiscard]] File FutureFile::get()
            {
                Index index = m_future.get();
                return { m_path, std::move(index) };
            }

            AsyncStorePipeline::Job::Job(std::filesystem::path path, std::vector<Entry>&& buffer, std::promise<Index>&& promise) :
                path(std::move(path)),
                buffer(std::move(buffer)),
                promise(std::move(promise))
            {
            }

            AsyncStorePipeline::AsyncStorePipeline(std::vector<std::vector<Entry>>&& buffers, std::size_t numSortingThreads) :
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

            AsyncStorePipeline::~AsyncStorePipeline()
            {
                waitForCompletion();
            }

            [[nodiscard]] std::future<Index> AsyncStorePipeline::scheduleUnordered(const std::filesystem::path& path, std::vector<Entry>&& elements)
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                std::promise<Index> promise;
                std::future<Index> future = promise.get_future();
                m_sortQueue.emplace(path, std::move(elements), std::move(promise));

                lock.unlock();
                m_sortQueueNotEmpty.notify_one();

                return future;
            }

            [[nodiscard]] std::future<Index> AsyncStorePipeline::scheduleOrdered(const std::filesystem::path& path, std::vector<Entry>&& elements)
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                std::promise<Index> promise;
                std::future<Index> future = promise.get_future();
                m_sortQueue.emplace(path, std::move(elements), std::move(promise));

                lock.unlock();
                m_writeQueueNotEmpty.notify_one();

                return future;
            }

            [[nodiscard]] std::vector<Entry> AsyncStorePipeline::getEmptyBuffer()
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                m_bufferQueueNotEmpty.wait(lock, [this]() {return !m_bufferQueue.empty(); });

                auto buffer = std::move(m_bufferQueue.front());
                m_bufferQueue.pop();

                buffer.clear();

                return buffer;
            }

            void AsyncStorePipeline::waitForCompletion()
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

            void AsyncStorePipeline::runSortingThread()
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

            void AsyncStorePipeline::runWritingThread()
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

            void AsyncStorePipeline::sort(std::vector<Entry>& buffer)
            {
                auto cmp = Entry::CompareLessFull{};
                std::sort(buffer.begin(), buffer.end(), cmp);
            }

            // works analogously to std::unique but also combines equal values
            void AsyncStorePipeline::combine(std::vector<Entry>& buffer)
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

            void AsyncStorePipeline::prepareData(std::vector<Entry>& buffer)
            {
                sort(buffer);
                combine(buffer);
            }

            const std::size_t Partition::mergeMemory = cfg::g_config["persistence"]["db_beta"]["max_merge_buffer_size"].get<MemoryAmount>();

            Partition::Partition(std::filesystem::path path)
            {
                ASSERT(!path.empty());

                setPath(std::move(path));
            }

            void Partition::setPath(std::filesystem::path path)
            {
                ASSERT(m_futureFiles.empty());

                m_path = std::move(path);
                std::filesystem::create_directories(m_path);

                discoverFiles();
            }

            void Partition::executeQuery(
                const query::Request& query,
                const std::vector<Key>& keys,
                const query::PositionQueries& queries,
                std::vector<PositionStats>& stats)
            {
                for (auto&& file : m_files)
                {
                    file.executeQuery(query, keys, queries, stats);
                }
            }

            void Partition::mergeAll(std::function<void(const ext::ProgressReport&)> progressCallback)
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
            void Partition::replicateMergeAll(const std::filesystem::path& outPath, std::function<void(const ext::ProgressReport&)> progressCallback)
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
            void Partition::storeOrdered(const Entry* data, std::size_t count)
            {
                ASSERT(!m_path.empty());

                auto path = nextPath();
                (void)ext::writeFile(path, data, count);
                m_files.emplace_back(path);
            }

            // entries have to be sorted in ascending order
            void Partition::storeOrdered(const std::vector<Entry>& entries)
            {
                storeOrdered(entries.data(), entries.size());
            }

            // Uses the passed id.
            // It is required that the file with this id doesn't exist already.
            void Partition::storeUnordered(AsyncStorePipeline& pipeline, std::vector<Entry>&& entries, std::uint32_t id)
            {
                ASSERT(!m_path.empty());

                std::unique_lock<std::mutex> lock(m_mutex);
                auto path = pathForId(id);
                m_futureFiles.emplace(pipeline.scheduleUnordered(path, std::move(entries)), path);
            }

            void Partition::storeUnordered(AsyncStorePipeline& pipeline, std::vector<Entry>&& entries)
            {
                storeUnordered(pipeline, std::move(entries), nextId());
            }

            void Partition::collectFutureFiles()
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

            [[nodiscard]] std::uint32_t Partition::nextId() const
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

            [[nodiscard]] const std::filesystem::path Partition::path() const
            {
                return m_path;
            }

            void Partition::clear()
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

            [[nodiscard]] bool Partition::empty() const
            {
                return m_files.empty() && m_futureFiles.empty();
            }

            [[nodiscard]] std::filesystem::path Partition::pathForId(std::uint32_t id) const
            {
                return File::pathForId(m_path, id);
            }

            [[nodiscard]] std::filesystem::path Partition::nextPath() const
            {
                return pathForId(nextId());
            }

            [[nodiscard]] Index Partition::mergeAllIntoFile(const std::filesystem::path& outFilePath, std::function<void(const ext::ProgressReport&)> progressCallback) const
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
                        ext::BackInserter<Entry> out(outFile, util::DoubleBuffer<Entry>(outBufferSize));

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

            void Partition::discoverFiles()
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
        }

        const std::size_t Database::m_pgnParserMemory = cfg::g_config["persistence"]["db_beta"]["pgn_parser_memory"].get<MemoryAmount>();

        Database::Database(std::filesystem::path path) :
            BaseType(path, Database::manifest()),
            m_path(path),
            m_headers(makeHeaders(path)),
            m_nextGameIdx(numGamesInHeaders()),
            m_partition(path / partitionDirectory)
        {
        }

        Database::Database(std::filesystem::path path, std::size_t headerBufferMemory) :
            BaseType(path, Database::manifest()),
            m_path(path),
            m_headers(makeHeaders(path, headerBufferMemory)),
            m_nextGameIdx(numGamesInHeaders()),
            m_partition(path / partitionDirectory)
        {
        }

        [[nodiscard]] const std::string& Database::key()
        {
            return m_manifest.key;
        }

        [[nodiscard]] const DatabaseManifest& Database::manifest() const
        {
            return m_manifest;
        }

        void Database::clear()
        {
            for (auto& header : m_headers)
            {
                header.clear();
            }
            m_partition.clear();
        }

        const std::filesystem::path& Database::path() const
        {
            return m_path;
        }

        [[nodiscard]] query::Response Database::executeQuery(query::Request query)
        {
            disableUnsupportedQueryFeatures(query);

            query::PositionQueries posQueries = query::gatherPositionQueries(query);
            auto keys = getKeys(posQueries);
            std::vector<detail::PositionStats> stats(posQueries.size());

            auto cmp = detail::Key::CompareLessWithReverseMove{};
            auto unsort = reversibleZipSort(keys, posQueries, cmp);

            m_partition.executeQuery(query, keys, posQueries, stats);

            auto results = commitStatsAsResults(query, posQueries, stats);

            // We have to either unsort both results and posQueries, or none.
            // unflatten only needs relative order of results and posQueries to match
            // So we don't unsort any.
            auto unflattened = query::unflatten(std::move(results), query, posQueries);

            return { std::move(query), std::move(unflattened) };
        }

        void Database::mergeAll(Database::MergeProgressCallback progressCallback)
        {
            Logger::instance().logInfo(": Merging files...");

            auto progressReport = [&progressCallback](const ext::ProgressReport& report) {
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

            m_partition.mergeAll(progressReport);

            Logger::instance().logInfo(": Finalizing...");
            Logger::instance().logInfo(": Completed.");
        }

        void Database::replicateMergeAll(const std::filesystem::path& path, Database::MergeProgressCallback progressCallback)
        {
            if (std::filesystem::exists(path) && !std::filesystem::is_empty(path))
            {
                throw std::runtime_error("Destination for replicating merge must be empty.");
            }
            std::filesystem::create_directories(path / partitionDirectory);

            BaseType::replicateMergeAll(path);

            for (auto& header : m_headers)
            {
                header.replicateTo(path);
            }

            Logger::instance().logInfo(": Merging files...");

            auto progressReport = [&progressCallback](const ext::ProgressReport& report) {
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

            m_partition.replicateMergeAll(path / partitionDirectory, progressReport);

            Logger::instance().logInfo(": Finalizing...");
            Logger::instance().logInfo(": Completed.");
        }

        ImportStats Database::import(
            std::execution::parallel_unsequenced_policy,
            const ImportableFiles& pgns,
            std::size_t memory,
            std::size_t numThreads,
            Database::ImportProgressCallback progressCallback
            )
        {
            // TODO: progress reporting

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

            BaseType::addStats(stats);

            return stats;
        }

        ImportStats Database::import(
            std::execution::sequenced_policy,
            const ImportableFiles& files,
            std::size_t memory,
            Database::ImportProgressCallback progressCallback
            )
        {
            const std::size_t numSortingThreads = std::clamp(std::thread::hardware_concurrency(), 1u, 3u) - 1u;

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

            Logger::instance().logInfo(": Importing files...");
            ImportStats statsTotal = importPgnsImpl(
                std::execution::seq, 
                pipeline, 
                files, 
                [&progressCallback, &totalSize, &totalSizeProcessed](auto&& pgn) {
                    totalSizeProcessed += std::filesystem::file_size(pgn);
                    Logger::instance().logInfo(
                        ":     ", 
                        static_cast<int>(static_cast<double>(totalSizeProcessed) / totalSize * 100.0), 
                        "% - completed ", 
                        pgn, 
                        "."
                    );

                    if (progressCallback)
                    {
                        ImportProgressReport report{
                            totalSizeProcessed,
                            totalSize,
                            pgn
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

        ImportStats Database::import(const ImportableFiles& files, std::size_t memory, Database::ImportProgressCallback progressCallback)
        {
            return import(std::execution::seq, files, memory, progressCallback);
        }

        void Database::flush()
        {
            for (auto& header : m_headers)
            {
                header.flush();
            }
        }

        [[nodiscard]] EnumArray<GameLevel, Header> Database::makeHeaders(const std::filesystem::path& path)
        {
            return {
                Header(path, Header::defaultMemory, m_headerNames[values<GameLevel>()[0]]),
                Header(path, Header::defaultMemory, m_headerNames[values<GameLevel>()[1]]),
                Header(path, Header::defaultMemory, m_headerNames[values<GameLevel>()[2]])
            };
        }

        [[nodiscard]] EnumArray<GameLevel, Header> Database::makeHeaders(const std::filesystem::path& path, std::size_t headerBufferMemory)
        {
            return {
                Header(path, headerBufferMemory, m_headerNames[values<GameLevel>()[0]]),
                Header(path, headerBufferMemory, m_headerNames[values<GameLevel>()[1]]),
                Header(path, headerBufferMemory, m_headerNames[values<GameLevel>()[2]])
            };
        }

        [[nodiscard]] std::uint32_t Database::numGamesInHeaders() const
        {
            std::uint32_t total = 0;

            for (auto& header : m_headers)
            {
                total += header.numGames();
            }

            return total;
        }

        void Database::collectFutureFiles()
        {
            m_partition.collectFutureFiles();
        }

        [[nodiscard]] std::vector<PackedGameHeader> Database::queryHeadersByOffsets(const std::vector<std::uint64_t>& offsets, GameLevel level)
        {
            return m_headers[level].queryByOffsets(offsets);
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

        void Database::disableUnsupportedQueryFeatures(query::Request& query) const
        {
            for (auto&& [select, fetch] : query.fetchingOptions)
            {
                fetch.fetchLastGame = false;
                fetch.fetchLastGameForEachChild = false;
            }
        }

        [[nodiscard]] query::PositionQueryResults Database::commitStatsAsResults(
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

        [[nodiscard]] std::vector<detail::Key> Database::getKeys(const query::PositionQueries& queries)
        {
            std::vector<detail::Key> keys;
            keys.reserve(queries.size());
            for (auto&& q : queries)
            {
                keys.emplace_back(q.position, q.reverseMove);
            }
            return keys;
        }

        ImportStats Database::importPgnsImpl(
            std::execution::sequenced_policy,
            detail::AsyncStorePipeline& pipeline,
            const ImportableFiles& files,
            std::function<void(const std::filesystem::path& file)> completionCallback
        )
        {
            // create buffers
            std::vector<detail::Entry> bucket = pipeline.getEmptyBuffer();

            ImportStats stats{};
            for (auto& file : files)
            {
                const auto& path = file.path();
                const auto level = file.level();

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
                        stats.statsByLevel[level].numSkippedGames += 1;
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

                    stats.statsByLevel[level].numGames += 1;
                    stats.statsByLevel[level].numPositions += numPositionsInGame;
                }

                completionCallback(path);
            }

            // flush buffers and return them to the pipeline for later use
            store(pipeline, std::move(bucket));

            return stats;
        }

        [[nodiscard]] std::vector<Database::Block> Database::divideIntoBlocks(
            const ImportableFiles& files,
            std::size_t bufferSize,
            std::size_t numBlocks
        )
        {
            constexpr std::size_t minPgnBytesPerMove = 4;

            // We compute the total size of the files
            std::vector<std::size_t> fileSizes;
            fileSizes.reserve(files.size());
            std::size_t totalFileSize = 0;
            for (auto& file : files)
            {
                const std::size_t size = std::filesystem::file_size(file.path());
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
                auto start = files.begin();
                for (int i = 0; i < files.size(); ++i)
                {
                    blockSize += fileSizes[i];

                    if (blockSize >= blockSizeThreshold)
                    {
                        // here we apply the offset
                        std::uint32_t nextIds = baseNextId + idOffset;

                        // store the block of desired size
                        auto end = files.begin() + i;
                        blocks.emplace_back(Block{ start, end, nextIds });
                        start = end;
                        idOffset += static_cast<std::uint32_t>(blockSize / (bufferSize * minPgnBytesPerMove)) + 1u;
                        blockSize = 0;
                    }
                }

                // if anything is left over we have to handle it here as in the
                // loop we only handle full blocks; last one may be only partially full
                if (start != files.end())
                {
                    std::uint32_t nextId = baseNextId + idOffset;
                    blocks.emplace_back(Block{ start, files.end(), nextId });
                }

                ASSERT(blocks.size() <= numBlocks);

                blocks.resize(numBlocks);

                ASSERT(blocks.size() == numBlocks);
            }

            return blocks;
        }

        ImportStats Database::importPgnsImpl(
            std::execution::parallel_unsequenced_policy,
            detail::AsyncStorePipeline& pipeline,
            const ImportableFiles& files,
            std::size_t bufferSize,
            std::size_t numThreads
        )
        {
            const auto blocks = divideIntoBlocks(files, bufferSize, numThreads);

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
                    auto& file = *begin;
                    const auto& path = file.path();
                    const auto level = file.level();

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
                            stats.statsByLevel[level].numSkippedGames += 1;
                            continue;
                        }

                        auto& header = m_headers[level];

                        auto gameHeader = PackedGameHeader(game, m_nextGameIdx.fetch_add(1));
                        const std::uint64_t gameOffset = header.addHeader(gameHeader).offset;

                        std::size_t numPositionsInGame = 0;
                        auto processPosition = [&, &nextId = nextId](const Position& position, const ReverseMove& reverseMove) {
                            entries.emplace_back(position, reverseMove, level, *result, gameOffset);
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

                        stats.statsByLevel[level].numGames += 1;
                        stats.statsByLevel[level].numPositions += numPositionsInGame;
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

        void Database::store(
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

        void Database::store(
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

        void Database::store(
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

        void Database::store(
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
    }
}
