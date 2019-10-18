#pragma once

#include "persistence/pos_db/Database.h"
#include "persistence/pos_db/Query.h"
#include "persistence/pos_db/StorageHeader.h"

#include "chess/GameClassification.h"

#include "data_structure/EnumMap.h"

#include "external_storage/External.h"

#include "util/MemoryAmount.h"

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

struct Position;
struct ReverseMove;
struct Move;

namespace persistence
{
    namespace db_alpha
    {
        namespace detail
        {
            static constexpr bool useIndex = true;

            // Have ranges of mixed values be at most this long
            extern const std::size_t indexGranularity;

            struct Key
            {
                using StorageType = std::array<std::uint32_t, 4>;

                Key() = default;

                Key(const Position& pos, const ReverseMove& reverseMove = ReverseMove{});

                Key(const Key&) = default;
                Key(Key&&) = default;
                Key& operator=(const Key&) = default;
                Key& operator=(Key&&) = default;

                [[nodiscard]] const StorageType& hash() const;

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

                Entry(const Position& pos, const ReverseMove& reverseMove, std::uint32_t gameIdx);

                // TODO: eventually remove this overload?
                Entry(const Position& pos, std::uint32_t gameIdx);

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

                [[nodiscard]] const Key& key() const;

                [[nodiscard]] std::uint32_t gameIdx() const;

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
                static std::filesystem::path pathForId(const std::filesystem::path& path, std::uint32_t id);

                File(std::filesystem::path path);

                File(ext::ImmutableSpan<Entry>&& entries);

                File(std::filesystem::path path, Indexes&& index);

                File(ext::ImmutableSpan<Entry>&& entries, Indexes&& index);

                [[nodiscard]] friend bool operator<(const File& lhs, const File& rhs) noexcept;

                [[nodiscard]] std::uint32_t id() const;

                [[nodiscard]] const std::filesystem::path& path() const;

                [[nodiscard]] Entry at(std::size_t idx) const;

                [[nodiscard]] const ext::ImmutableSpan<Entry>& entries() const;

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
                    GameResult result);

                void executeQueryAll(
                    const std::vector<Key>& keys,
                    std::vector<PositionStats>& stats,
                    GameLevel level,
                    GameResult result);

            private:
                ext::ImmutableSpan<Entry> m_entries;
                IndexWithoutReverseMove m_indexWithoutReverseMove;
                IndexWithReverseMove m_indexWithReverseMove;
                std::uint32_t m_id;
            };

            struct FutureFile
            {
                FutureFile(std::future<Indexes>&& future, std::filesystem::path path);

                [[nodiscard]] friend bool operator<(const FutureFile& lhs, const FutureFile& rhs) noexcept;

                [[nodiscard]] std::uint32_t id() const;

                [[nodiscard]] File get();

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
                    Job(std::filesystem::path path, std::vector<Entry>&& buffer, std::promise<Indexes>&& promise, bool createIndex = useIndex);

                    std::filesystem::path path;
                    std::vector<Entry> buffer;
                    std::promise<Indexes> promise;
                    bool createIndex;
                };

            public:
                AsyncStorePipeline(std::vector<std::vector<Entry>>&& buffers, std::size_t numSortingThreads = 1);

                AsyncStorePipeline(const AsyncStorePipeline&) = delete;

                ~AsyncStorePipeline();

                [[nodiscard]] std::future<Indexes> scheduleUnordered(const std::filesystem::path& path, std::vector<Entry>&& elements, bool createIndex = useIndex);

                [[nodiscard]] std::future<Indexes> scheduleOrdered(const std::filesystem::path& path, std::vector<Entry>&& elements, bool createIndex = useIndex);

                [[nodiscard]] std::vector<Entry> getEmptyBuffer();

                void waitForCompletion();

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

                void runSortingThread();

                void runWritingThread();
            };

            struct Partition
            {
                static const std::size_t mergeMemory;

                Partition() = default;

                Partition(std::filesystem::path path);

                void executeQueryContinuations(
                    const std::vector<Key>& keys,
                    std::vector<PositionStats>& stats,
                    GameLevel level,
                    GameResult result);

                void executeQueryAll(
                    const std::vector<Key>& keys,
                    std::vector<PositionStats>& stats,
                    GameLevel level,
                    GameResult result);

                void setPath(std::filesystem::path path);

                // data has to be sorted in ascending order
                void storeOrdered(const Entry* data, std::size_t count);

                // entries have to be sorted in ascending order
                void storeOrdered(const std::vector<Entry>& entries);

                // Uses the passed id.
                // It is required that the file with this id doesn't exist already.
                void storeUnordered(AsyncStorePipeline& pipeline, std::vector<Entry>&& entries, std::uint32_t id);

                void storeUnordered(AsyncStorePipeline& pipeline, std::vector<Entry>&& entries);

                void collectFutureFiles();

                [[nodiscard]] std::uint32_t nextId() const;

                [[nodiscard]] const std::filesystem::path path() const;

                void clear();

                void mergeAll(std::function<void(const ext::ProgressReport&)> progressCallback);

                [[nodiscard]] bool empty() const;

                // outPath is a path of the file to output to
                void replicateMergeAll(const std::filesystem::path& outPath, std::function<void(const ext::ProgressReport&)> progressCallback);

            private:
                std::filesystem::path m_path;
                std::vector<File> m_files;

                // We store it in a set because then we can change insertion
                // order through forcing ids. It's easier to keep it
                // ordered like that. And we need it ordered all the time
                // because of queries to nextId()
                std::set<FutureFile> m_futureFiles;

                std::mutex m_mutex;

                [[nodiscard]] Indexes mergeAllIntoFile(const std::filesystem::path& outFilePath, std::function<void(const ext::ProgressReport&)> progressCallback) const;

                [[nodiscard]] std::filesystem::path pathForId(std::uint32_t id) const;

                [[nodiscard]] std::filesystem::path nextPath() const;

                void discoverFiles();
            };
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

            static const std::size_t m_pgnParserMemory;

        public:
            Database(std::filesystem::path path);

            Database(std::filesystem::path path, std::size_t headerBufferMemory);

            [[nodiscard]] const DatabaseManifest& manifest() const override;

            void clear() override;

            const std::filesystem::path& path() const override;

            [[nodiscard]] query::Response executeQuery(query::Request query) override;

            void mergeAll() override;

            void replicateMergeAll(const std::filesystem::path& path) override;

            ImportStats import(
                std::execution::parallel_unsequenced_policy,
                const ImportablePgnFiles& pgns,
                std::size_t memory,
                std::size_t numThreads = std::thread::hardware_concurrency()
                ) override;

            ImportStats import(
                std::execution::sequenced_policy,
                const ImportablePgnFiles& pgns,
                std::size_t memory
                ) override;

            ImportStats import(const ImportablePgnFiles& pgns, std::size_t memory) override;

            void flush() override;

        private:
            std::filesystem::path m_path;

            Header m_header;
            PartitionStorageType m_partitions;

            struct Block
            {
                typename std::vector<std::filesystem::path>::const_iterator begin;
                typename std::vector<std::filesystem::path>::const_iterator end;
                PerPartitionWithSpecificGameLevel<std::uint32_t> nextIds;
            };

            void collectFutureFiles();

            [[nodiscard]] std::vector<PackedGameHeader> queryHeadersByIndices(const std::vector<std::uint32_t>& indices);

            void disableUnsupportedQueryFeatures(query::Request& query) const;

            [[nodiscard]] std::vector<detail::Key> getKeys(const query::PositionQueries& queries);

            [[nodiscard]] query::PositionQueryResults commitStatsAsResults(
                const query::Request& query,
                const query::PositionQueries& posQueries,
                std::vector<detail::PositionStats>& stats);

            void computeTranspositions(const query::Request& query, std::vector<detail::PositionStats>& stats);
            
            ImportStats importPgnsImpl(
                std::execution::sequenced_policy,
                detail::AsyncStorePipeline& pipeline,
                const ImportablePgnFilePaths& paths,
                GameLevel level,
                std::function<void(const std::filesystem::path& file)> completionCallback
            );

            [[nodiscard]] std::vector<Block> divideIntoBlocks(
                const ImportablePgnFilePaths& paths,
                GameLevel level,
                std::size_t bufferSize,
                std::size_t numBlocks
            );

            ImportStats importPgnsImpl(
                std::execution::parallel_unsequenced_policy,
                detail::AsyncStorePipeline& pipeline,
                const ImportablePgnFilePaths& paths,
                GameLevel level,
                std::size_t bufferSize,
                std::size_t numThreads
            );

            // this is nontrivial to do in the constructor initializer list
            void initializePartitions();

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>& entries,
                GameLevel level,
                GameResult result
            );

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>&& entries,
                GameLevel level,
                GameResult result
            );

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>& entries,
                GameLevel level,
                GameResult result,
                std::uint32_t id
            );

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>&& entries,
                GameLevel level,
                GameResult result,
                std::uint32_t id
            );

            static PerPartition<std::filesystem::path> initializePartitionDirectories(const std::filesystem::path& path);
        };
    }
}