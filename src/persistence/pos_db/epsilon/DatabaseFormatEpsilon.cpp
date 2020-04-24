#include "DatabaseFormatEpsilon.h"

#include "algorithm/Unsort.h"

#include "chess/Bcgn.h"
#include "chess/GameClassification.h"
#include "chess/MoveIndex.h"
#include "chess/Pgn.h"
#include "chess/Position.h"
#include "chess/San.h"

#include "enum/EnumArray.h"

#include "external_storage/External.h"

#include "persistence/pos_db/Database.h"
#include "persistence/pos_db/Query.h"

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
    namespace db_epsilon
    {
        namespace detail
        {
            const std::size_t indexGranularity = cfg::g_config["persistence"]["db_epsilon"]["index_granularity"].get<std::size_t>();

            static uint32_t packReverseMove(const Position& pos, const ReverseMove& rm)
            {
                uint32_t toSquareIndex;
                uint32_t destinationIndex;
                if (rm.move.type == MoveType::Castle)
                {
                    toSquareIndex = 0; // we can set this to zero because destinationIndex is unique

                    const bool isKingSide = rm.move.to.file() == fileH;
                    destinationIndex = isKingSide ? 30 : 31;
                }
                else if (rm.move.type == MoveType::Promotion)
                {
                    toSquareIndex = (bb::before(rm.move.to) & pos.piecesBB(pos.sideToMove())).count();
                    destinationIndex = std::abs(ordinal(rm.move.to) - ordinal(rm.move.from)) - 7 + 27; // verify
                }
                else
                {
                    toSquareIndex = (bb::before(rm.move.to) & pos.piecesBB(pos.sideToMove())).count();
                    const PieceType pt = pos.pieceAt(rm.move.to).type();
                    if (pt == PieceType::Pawn)
                    {
                        destinationIndex = move_index::pawnDestinationIndex(rm.move.from, rm.move.to, pos.sideToMove(), PieceType::None);
                    }
                    else
                    {
                        destinationIndex = move_index::destinationIndex(pt, rm.move.from, rm.move.to);
                    }
                }

                const uint32_t capturedPieceType = ordinal(rm.capturedPiece.type());
                const uint32_t oldCastlingRights = ordinal(rm.oldCastlingRights);
                const uint32_t hadEpSquare = pos.epSquare() != Square::none();
                const uint32_t oldEpSquareFile = ordinal(pos.epSquare().file());

                return
                    (toSquareIndex << (20 - 4))
                    | (destinationIndex << (20 - 4 - 5))
                    | (capturedPieceType << (20 - 4 - 5 - 3))
                    | (oldCastlingRights << (20 - 4 - 5 - 3 - 4))
                    | (hadEpSquare << (20 - 4 - 5 - 3 - 4 - 1))
                    | oldEpSquareFile;
            }

            Key::Key(const PositionWithZobrist& pos, const ReverseMove& reverseMove)
            {
                const auto zobrist = pos.zobrist();
                m_hash[0] = zobrist.high >> 32;
                m_hash[1] = zobrist.high & 0xFFFFFFFFull;
                m_hash[2] = zobrist.low & lastHashPartMask;
                m_hash[2] |= packReverseMove(pos, reverseMove) << reverseMoveShift;
            }

            Key::Key(const PositionWithZobrist& pos, const ReverseMove& reverseMove, GameLevel level, GameResult result) :
                Key(pos, reverseMove)
            {
                m_hash[2] |=
                    ((ordinal(level) & levelMask) << levelShift)
                    | ((ordinal(result) & resultMask));
            }

            [[nodiscard]] const Key::StorageType& Key::hash() const
            {
                return m_hash;
            }

            [[nodiscard]] GameLevel Key::level() const
            {
                return fromOrdinal<GameLevel>((m_hash[2] >> levelShift) & levelMask);
            }

            [[nodiscard]] GameResult Key::result() const
            {
                return fromOrdinal<GameResult>(m_hash[2] & resultMask);
            }

            Entry::Entry(const PositionWithZobrist& pos, const ReverseMove& reverseMove, GameLevel level, GameResult result) :
                m_key(pos, reverseMove, level, result),
                m_count(1)
            {
            }

            [[nodiscard]] const Key& Entry::key() const
            {
                return m_key;
            }

            [[nodiscard]] std::uint32_t Entry::count() const
            {
                return m_count;
            }

            [[nodiscard]] GameLevel Entry::level() const
            {
                return m_key.level();
            }

            [[nodiscard]] GameResult Entry::result() const
            {
                return m_key.result();
            }

            void Entry::combine(const Entry& rhs)
            {
                m_count += rhs.m_count;
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
                            statsForThisSelect[level][result] += entry.count();
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

        const std::size_t Database::m_pgnParserMemory = cfg::g_config["persistence"]["db_epsilon"]["pgn_parser_memory"].get<MemoryAmount>();
        const std::size_t Database::m_bcgnParserMemory = cfg::g_config["persistence"]["db_epsilon"]["bcgn_parser_memory"].get<MemoryAmount>();

        Database::Database(std::filesystem::path path) :
            BaseType(path, Database::manifest()),
            m_path(path),
            m_partition(path / partitionDirectory)
        {
        }

        Database::Database(std::filesystem::path path, std::size_t headerBufferMemory) :
            BaseType(path, Database::manifest()),
            m_path(path),
            m_partition(path / partitionDirectory)
        {
        }

        [[nodiscard]] const std::string& Database::key()
        {
            return m_manifest.key;
        }

        [[nodiscard]] const DatabaseSupportManifest& Database::supportManifest()
        {
            static const DatabaseSupportManifest manifest = {
                { ImportableFileType::Pgn, ImportableFileType::Bcgn }
            };

            return manifest;
        }

        [[nodiscard]] const DatabaseManifest& Database::manifest() const
        {
            return m_manifest;
        }

        void Database::clear()
        {
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
            const ImportableFiles& files,
            std::size_t memory,
            std::size_t numThreads,
            Database::ImportProgressCallback progressCallback
            )
        {
            // TODO: progress reporting

            if (files.empty())
            {
                return {};
            }

            if (numThreads <= 4)
            {
                return import(std::execution::seq, files, memory);
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
            ImportStats stats = importImpl(std::execution::par_unseq, pipeline, files, bucketSize, numWorkerThreads);

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
                ext::numObjectsPerBufferUnit<detail::Entry>(
                    memory,
                    numBuffers + numAdditionalBuffers
                    );

            detail::AsyncStorePipeline pipeline(
                detail::createBuffers<detail::Entry>(numBuffers + numAdditionalBuffers, bucketSize),
                numSortingThreads
            );

            Logger::instance().logInfo(": Importing files...");
            ImportStats statsTotal = importImpl(
                std::execution::seq,
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

        ImportStats Database::import(const ImportableFiles& files, std::size_t memory, Database::ImportProgressCallback progressCallback)
        {
            return import(std::execution::seq, files, memory, progressCallback);
        }

        void Database::flush()
        {
        }

        void Database::collectFutureFiles()
        {
            m_partition.collectFutureFiles();
        }

        void Database::disableUnsupportedQueryFeatures(query::Request& query) const
        {
            for (auto&& [select, fetch] : query.fetchingOptions)
            {
                fetch.fetchFirstGame = false;
                fetch.fetchFirstGameForEachChild = false;
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
                            auto& count = stat[select][level][result];
                            results[i][select].emplace(level, result, count);
                        }
                    }
                }
            }

            return results;
        }

        [[nodiscard]] std::vector<detail::Key> Database::getKeys(const query::PositionQueries& queries)
        {
            std::vector<detail::Key> keys;
            keys.reserve(queries.size());
            for (auto&& q : queries)
            {
                keys.emplace_back(PositionWithZobrist(q.position), q.reverseMove);
            }
            return keys;
        }

        ImportStats Database::importImpl(
            std::execution::sequenced_policy,
            detail::AsyncStorePipeline& pipeline,
            const ImportableFiles& files,
            std::function<void(const std::filesystem::path& file)> completionCallback
        )
        {
            // create buffers
            std::vector<detail::Entry> bucket = pipeline.getEmptyBuffer();

            auto processPosition = [this, &bucket, &pipeline](
                const PositionWithZobrist& position,
                const ReverseMove& reverseMove,
                GameLevel level,
                GameResult result
                ) {
                    bucket.emplace_back(position, reverseMove, level, result);

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

                        PositionWithZobrist position = PositionWithZobrist::startPosition();
                        ReverseMove reverseMove{};
                        processPosition(position, reverseMove, level, *result);
                        std::size_t numPositionsInGame = 1;
                        for (auto& san : game.moves())
                        {
                            const Move move = san::sanToMove(position, san);
                            if (move == Move::null())
                            {
                                break;
                            }

                            reverseMove = position.doMove(move);
                            processPosition(position, reverseMove, level, *result);

                            ++numPositionsInGame;
                        }

                        ASSERT(numPositionsInGame > 0);

                        stats.statsByLevel[level].numGames += 1;
                        stats.statsByLevel[level].numPositions += numPositionsInGame;
                    }
                }
                else if (type == ImportableFileType::Bcgn)
                {
                    bcgn::BcgnFileReader fr(path, m_bcgnParserMemory);
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

                        PositionWithZobrist position = PositionWithZobrist::startPosition();
                        ReverseMove reverseMove{};
                        processPosition(position, reverseMove, level, *result);
                        auto moves = game.moves();
                        while(moves.hasNext())
                        {
                            const auto move = moves.next(position);
                            reverseMove = position.doMove(move);
                            processPosition(position, reverseMove, level, *result);
                        }

                        stats.statsByLevel[level].numGames += 1;
                        stats.statsByLevel[level].numPositions += game.numPlies() + 1;
                    }
                }
                else
                {
                    Logger::instance().logError("Importing files other than PGN or BCGN is not supported by db_epsilon.");
                    throw std::runtime_error("Importing files other than PGN or BCGN is not supported by db_epsilon.");
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
            constexpr std::size_t minBcgnBytesPerMove = 1;

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
                std::size_t maxNumberOfMovesInBlock = 0;
                auto start = files.begin();
                for (int i = 0; i < files.size(); ++i)
                {
                    blockSize += fileSizes[i];

                    const auto minBytesPerMove =
                        files[i].type() == ImportableFileType::Pgn
                        ? minPgnBytesPerMove
                        : minBcgnBytesPerMove;
                    maxNumberOfMovesInBlock += (fileSizes[i] / minBytesPerMove) + 1u;

                    if (blockSize >= blockSizeThreshold)
                    {
                        // here we apply the offset
                        std::uint32_t nextIds = baseNextId + idOffset;

                        // store the block of desired size
                        auto end = files.begin() + i;
                        blocks.emplace_back(Block{ start, end, nextIds });
                        start = end;
                        idOffset += static_cast<std::uint32_t>(maxNumberOfMovesInBlock / bufferSize) + 1u;
                        blockSize = 0;
                        maxNumberOfMovesInBlock = 0;
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

        ImportStats Database::importImpl(
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

                auto processPosition = [this, &pipeline, &entries](
                    const PositionWithZobrist& position,
                    const ReverseMove& reverseMove,
                    GameLevel level,
                    GameResult result,
                    std::uint32_t& nextId
                    ) {
                    entries.emplace_back(position, reverseMove, level, result);

                    if (entries.size() == entries.capacity())
                    {
                        // Here we force the id and move to the next one.
                        // This doesn't have to be atomic since we're the only
                        // ones using this blocks and there is enough space left for
                        // all files before the next already present id.
                        store(pipeline, entries, nextId++);
                    }
                };

                ImportStats stats{};
                auto [begin, end, nextId] = block;

                for (; begin != end; ++begin)
                {
                    auto& file = *begin;
                    const auto& path = file.path();
                    const auto level = file.level();
                    const auto type = file.type();

                    if (type == ImportableFileType::Pgn)
                    {
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

                            std::size_t numPositionsInGame = 0;

                            PositionWithZobrist position = PositionWithZobrist::startPosition();
                            ReverseMove reverseMove{};
                            processPosition(position, reverseMove, level, *result, nextId);
                            for (auto& san : game.moves())
                            {
                                const Move move = san::sanToMove(position, san);
                                if (move == Move::null())
                                {
                                    break;
                                }

                                reverseMove = position.doMove(move);
                                processPosition(position, reverseMove, level, *result, nextId);
                            }

                            ASSERT(numPositionsInGame > 0);

                            stats.statsByLevel[level].numGames += 1;
                            stats.statsByLevel[level].numPositions += numPositionsInGame;
                        }
                    }
                    else if (type == ImportableFileType::Bcgn)
                    {
                        bcgn::BcgnFileReader fr(path, m_bcgnParserMemory);
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

                            PositionWithZobrist position = PositionWithZobrist::startPosition();
                            ReverseMove reverseMove{};
                            processPosition(position, reverseMove, level, *result, nextId);
                            auto moves = game.moves();
                            while(moves.hasNext())
                            {
                                const Move move = moves.next(position);
                                reverseMove = position.doMove(move);
                                processPosition(position, reverseMove, level, *result, nextId);
                            }

                            stats.statsByLevel[level].numGames += 1;
                            stats.statsByLevel[level].numPositions += game.numPlies() + 1;
                        }
                    }
                    else
                    {
                        Logger::instance().logError("Importing files other than PGN or BCGN is not supported by db_epsilon.");
                        throw std::runtime_error("Importing files other than PGN or BCGN is not supported by db_epsilon.");
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
