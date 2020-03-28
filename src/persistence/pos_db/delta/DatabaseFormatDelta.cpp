#include "DatabaseFormatDelta.h"

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
    namespace db_delta
    {
        namespace detail
        {
            const std::size_t indexGranularity = cfg::g_config["persistence"]["db_delta"]["index_granularity"].get<std::size_t>();

            Entry::Entry() :
                m_hashPart1{},
                m_eloDiffAndHashPart2(0),
                m_packedInfo{},
                m_count(0),
                m_firstGameIndex(std::numeric_limits<std::uint32_t>::max()),
                m_lastGameIndex(0)
            {
            }

            Entry::Entry(const Position& pos, const ReverseMove& reverseMove) :
                m_count(1),
                m_firstGameIndex(std::numeric_limits<std::uint32_t>::max()),
                m_lastGameIndex(0)
            {
                const auto hash = pos.hash();
                m_hashPart1 = static_cast<std::uint64_t>(hash[0]) << 32 | hash[1];
                m_eloDiffAndHashPart2 = (hash[2] & nbitmask<std::uint64_t>[additionalHashBits]);

                auto packedReverseMove = PackedReverseMove(reverseMove);
                // m_hash[0] is the most significant quad, m_hash[3] is the least significant
                // We want entries ordered with reverse move to also be ordered by just hash
                // so we have to modify the lowest bits.
                m_packedInfo = (packedReverseMove.packed() << reverseMoveShift);
            }

            Entry::Entry(const Position& pos, const ReverseMove& reverseMove, GameLevel level, GameResult result, std::uint32_t firstGameIndex, std::uint32_t lastGameIndex, std::int64_t eloDiff) :
                m_count(1),
                m_firstGameIndex(firstGameIndex),
                m_lastGameIndex(lastGameIndex)
            {
                const auto hash = pos.hash();
                m_hashPart1 = static_cast<std::uint64_t>(hash[0]) << 32 | hash[1];
                m_eloDiffAndHashPart2 = (static_cast<std::uint64_t>(eloDiff) << additionalHashBits) | (hash[2] & nbitmask<std::uint64_t>[additionalHashBits]);

                auto packedReverseMove = PackedReverseMove(reverseMove);
                // m_hash[0] is the most significant quad, m_hash[3] is the least significant
                // We want entries ordered with reverse move to also be ordered by just hash
                // so we have to modify the lowest bits.
                m_packedInfo = 
                    (packedReverseMove.packed() << reverseMoveShift)
                    | ((ordinal(level) & levelMask) << levelShift)
                    | ((ordinal(result) & resultMask) << resultShift);
            }

            [[nodiscard]] std::int64_t Entry::eloDiff() const
            {
                return signExtend<64 - additionalHashBits>(m_eloDiffAndHashPart2 >> additionalHashBits);
            }

            [[nodiscard]] std::array<std::uint64_t, 2> Entry::hash() const
            {
                return std::array<std::uint64_t, 2>{ m_hashPart1, ((m_eloDiffAndHashPart2& nbitmask<std::uint64_t>[additionalHashBits]) << 32) | m_packedInfo };
            }

            [[nodiscard]] std::uint32_t Entry::count() const
            {
                return m_count;
            }

            [[nodiscard]] std::uint32_t Entry::firstGameIndex() const
            {
                return m_firstGameIndex;
            }

            [[nodiscard]] std::uint32_t Entry::lastGameIndex() const
            {
                return m_lastGameIndex;
            }

            void Entry::combine(const Entry& other)
            {
                m_eloDiffAndHashPart2 += other.m_eloDiffAndHashPart2 & ~nbitmask<std::uint64_t>[additionalHashBits];
                m_count += other.m_count;
                const auto newFirstGame = std::min(m_firstGameIndex, other.m_firstGameIndex);
                const auto newLastGame = std::max(m_lastGameIndex, other.m_lastGameIndex);
                m_firstGameIndex = newFirstGame;
                m_lastGameIndex = newLastGame;
            }

            [[nodiscard]] GameLevel Entry::level() const
            {
                return fromOrdinal<GameLevel>((m_packedInfo >> levelShift) & levelMask);
            }

            [[nodiscard]] GameResult Entry::result() const
            {
                return fromOrdinal<GameResult>((m_packedInfo >> resultShift) & resultMask);
            }

            [[nodiscard]] std::uint32_t Entry::additionalHash() const
            {
                return m_hashPart1 & nbitmask<std::uint32_t>[additionalHashBits];
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
                return entry;
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
                            statsForThisSelect[level][result].combine(entry);
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

            const std::size_t Partition::mergeMemory = cfg::g_config["persistence"]["db_delta"]["max_merge_buffer_size"].get<MemoryAmount>();

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

        const std::size_t Database::m_pgnParserMemory = cfg::g_config["persistence"]["db_delta"]["pgn_parser_memory"].get<MemoryAmount>();

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
            const ImportablePgnFiles& pgns,
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
            const ImportablePgnFiles& pgns,
            std::size_t memory,
            Database::ImportProgressCallback progressCallback
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

            Logger::instance().logInfo(": Importing pgns...");
            ImportStats statsTotal = importPgnsImpl(
                std::execution::seq, 
                pipeline, 
                pgns, 
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

        ImportStats Database::import(const ImportablePgnFiles& pgns, std::size_t memory, Database::ImportProgressCallback progressCallback)
        {
            return import(std::execution::seq, pgns, memory, progressCallback);
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

        [[nodiscard]] std::vector<PackedGameHeader> Database::queryHeadersByIndices(const std::vector<std::uint32_t>& indices, GameLevel level)
        {
            return m_headers[level].queryByIndices(indices);
        }

        [[nodiscard]] std::vector<GameHeader> Database::queryHeadersByIndices(const std::vector<std::uint32_t>& indices, const std::vector<query::GameHeaderDestination>& destinations)
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
            std::vector<std::uint32_t> firstGameIndices;
            std::vector<std::uint32_t> lastGameIndices;
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
                            segregatedEntry.second.eloDiff = entry.eloDiff();

                            if (entry.count() > 0)
                            {
                                if (lookup[origin][select].fetchFirst)
                                {
                                    firstGameIndices.emplace_back(entry.firstGameIndex());
                                    firstGameDestinations.emplace_back(i, select, level, result, &query::Entry::firstGame);
                                }
                                if (lookup[origin][select].fetchLast)
                                {
                                    lastGameIndices.emplace_back(entry.firstGameIndex());
                                    lastGameDestinations.emplace_back(i, select, level, result, &query::Entry::lastGame);
                                }
                            }
                        }
                    }
                }
            }

            query::assignGameHeaders(results, firstGameDestinations, queryHeadersByIndices(firstGameIndices, firstGameDestinations));
            query::assignGameHeaders(results, lastGameDestinations, queryHeadersByIndices(lastGameIndices, lastGameDestinations));

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

                    const std::int64_t eloDiff = game.eloDiff();

                    auto& header = m_headers[level];

                    const std::uint32_t gameIndex = header.nextGameId();

                    std::size_t numPositionsInGame = 0;
                    auto processPosition = [&](const Position& position, const ReverseMove& reverseMove) {
                        bucket.emplace_back(position, reverseMove, level, *result, gameIndex, gameIndex, eloDiff);
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
                    const std::uint64_t actualGameIndex = header.addHeaderNoLock(gameHeader).index;
                    ASSERT(gameIndex == actualGameIndex);
                    (void)actualGameIndex;

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

        ImportStats Database::importPgnsImpl(
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

                        const std::int64_t eloDiff = game.eloDiff();

                        auto& header = m_headers[level];

                        auto gameHeader = PackedGameHeader(game, m_nextGameIdx.fetch_add(1));
                        const std::uint32_t gameIndex = header.addHeader(gameHeader).index;

                        std::size_t numPositionsInGame = 0;
                        auto processPosition = [&, &nextId = nextId](const Position& position, const ReverseMove& reverseMove) {
                            entries.emplace_back(position, reverseMove, level, *result, gameIndex, gameIndex, eloDiff);
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
