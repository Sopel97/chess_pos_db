#pragma once

#include "persistence/pos_db/Database.h"
#include "persistence/pos_db/Query.h"
#include "persistence/pos_db/StorageHeader.h"

#include "chess/GameClassification.h"

#include "enum/EnumArray.h"

#include "external_storage/External.h"

#include "util/MemoryAmount.h"
#include "util/ArithmeticUtility.h"

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

struct Position;
struct ReverseMove;
struct Move;

namespace persistence
{
    namespace db_delta
    {
        namespace detail
        {
            static constexpr bool usePacked = true;

            // Have ranges of mixed values be at most this long
            extern const std::size_t indexGranularity;

            static constexpr std::uint32_t invalidGameIndex = std::numeric_limits<std::uint32_t>::max();

            struct alignas(32) Entry
            {
                // Hash              : 64
                // Elo diff + Hash   : 40 + 24
                // PackedReverseMove : 27, GameLevel : 2, GameResult : 2, padding : 1
                // Count             : 32
                // First game index  : 32
                // Last game index   : 32
                // Total             : 64b * 4 = 256b = 32B

                // Hash:96, 

                static constexpr std::size_t additionalHashBits = 24;

                static constexpr std::size_t levelBits = 2;
                static constexpr std::size_t resultBits = 2;

                static constexpr std::uint32_t reverseMoveShift = 32 - PackedReverseMove::numBits;
                static constexpr std::uint32_t levelShift = reverseMoveShift - levelBits;
                static constexpr std::uint32_t resultShift = levelShift - resultBits;

                static constexpr std::uint32_t levelMask = 0b11;
                static constexpr std::uint32_t resultMask = 0b11;

                static_assert(PackedReverseMove::numBits + levelBits + resultBits <= 32);

                Entry() = default;

                Entry(const Position& pos, const ReverseMove& reverseMove = ReverseMove{});

                Entry(const Position& pos, const ReverseMove& reverseMove, GameLevel level, GameResult result, std::uint32_t firstGameIndex, std::uint32_t lastGameIndex, std::int64_t eloDiff);

                Entry(const Entry&) = default;
                Entry(Entry&&) = default;
                Entry& operator=(const Entry&) = default;
                Entry& operator=(Entry&&) = default;

                [[nodiscard]] GameLevel level() const;

                [[nodiscard]] GameResult result() const;

                [[nodiscard]] std::int64_t eloDiff() const
                {
                    return signExtend<64 - additionalHashBits>(m_eloDiffAndHashPart2 >> additionalHashBits);
                }

                [[nodiscard]] std::array<std::uint64_t, 2> hash() const
                {
                    return std::array<std::uint64_t, 2>{ m_hashPart1, ((m_eloDiffAndHashPart2 & nbitmask<std::uint64_t>[additionalHashBits]) << 32) | m_packedInfo };
                }

                [[nodiscard]] std::uint32_t count() const
                {
                    return m_count;
                }

                [[nodiscard]] std::uint32_t firstGameIndex() const
                {
                    return m_firstGameIndex;
                }

                [[nodiscard]] std::uint32_t lastGameIndex() const
                {
                    return m_lastGameIndex;
                }

                void combine(const Entry& other)
                {
                    m_eloDiffAndHashPart2 += other.m_eloDiffAndHashPart2 & ~nbitmask<std::uint64_t>[additionalHashBits];
                    m_count += other.m_count;
                    const auto newFirstGame = std::min(m_firstGameIndex, other.m_firstGameIndex);
                    const auto newLastGame = std::max(m_lastGameIndex, other.m_lastGameIndex);
                    m_firstGameIndex = newFirstGame;
                    m_lastGameIndex = newLastGame;
                }

                struct CompareLessWithReverseMove
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        if (lhs.m_hashPart1 < rhs.m_hashPart1) return true;
                        else if (lhs.m_hashPart1 > rhs.m_hashPart1) return false;

                        const auto lhsAdditionalHash = lhs.additionalHash();
                        const auto rhsAdditionalHash = rhs.additionalHash();
                        if (lhsAdditionalHash < rhsAdditionalHash) return true;
                        else if (lhsAdditionalHash > rhsAdditionalHash) return false;

                        return ((lhs.m_packedInfo & (PackedReverseMove::mask << reverseMoveShift)) < (rhs.m_packedInfo & (PackedReverseMove::mask << reverseMoveShift)));
                    }
                };

                struct CompareLessWithoutReverseMove
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        if (lhs.m_hashPart1 < rhs.m_hashPart1) return true;
                        else if (lhs.m_hashPart1 > rhs.m_hashPart1) return false;

                        const auto lhsAdditionalHash = lhs.additionalHash();
                        const auto rhsAdditionalHash = rhs.additionalHash();
                        return lhsAdditionalHash < rhsAdditionalHash;
                    }
                };

                struct CompareLessFull
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        if (lhs.m_hashPart1 < rhs.m_hashPart1) return true;
                        else if (lhs.m_hashPart1 > rhs.m_hashPart1) return false;

                        const auto lhsAdditionalHash = lhs.additionalHash();
                        const auto rhsAdditionalHash = rhs.additionalHash();
                        if (lhsAdditionalHash < rhsAdditionalHash) return true;
                        else if (lhsAdditionalHash > rhsAdditionalHash) return false;

                        return lhs.m_packedInfo < rhs.m_packedInfo;
                    }
                };

                struct CompareEqualWithReverseMove
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        return
                            lhs.m_hashPart1 == rhs.m_hashPart1
                            && lhs.additionalHash() == rhs.additionalHash()
                            && (lhs.m_packedInfo & (PackedReverseMove::mask << reverseMoveShift)) == (rhs.m_packedInfo & (PackedReverseMove::mask << reverseMoveShift));
                    }
                };

                struct CompareEqualWithoutReverseMove
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        return
                            lhs.m_hashPart1 == rhs.m_hashPart1
                            && lhs.additionalHash() == rhs.additionalHash();
                    }
                };

                struct CompareEqualFull
                {
                    [[nodiscard]] bool operator()(const Entry& lhs, const Entry& rhs) const noexcept
                    {
                        return
                            lhs.m_hashPart1 == rhs.m_hashPart1
                            && lhs.additionalHash() == rhs.additionalHash()
                            && lhs.m_packedInfo == rhs.m_packedInfo;
                    }
                };

            private:
                std::uint64_t m_hashPart1;
                std::uint64_t m_eloDiffAndHashPart2;
                std::uint32_t m_packedInfo;
                std::uint32_t m_count;
                std::uint32_t m_firstGameIndex;
                std::uint32_t m_lastGameIndex;

                [[nodiscard]] std::uint32_t additionalHash() const
                {
                    return m_hashPart1 & nbitmask<std::uint32_t>[additionalHashBits];
                }
            };
            static_assert(sizeof(Entry) == 32);

            static_assert(std::is_trivially_copyable_v<Entry>);

            // TODO: A more compact key type.
            using Key = Entry;

            using PositionStats = EnumArray<query::Select, EnumArray2<GameLevel, GameResult, Entry>>;

            using Index = ext::RangeIndex<Key, Entry::CompareLessWithoutReverseMove>;

            struct File
            {
                static std::filesystem::path pathForId(const std::filesystem::path& path, std::uint32_t id);

                File(std::filesystem::path path);

                File(ext::ImmutableSpan<Entry>&& entries);

                File(std::filesystem::path path, Index&& index);

                File(ext::ImmutableSpan<Entry>&& entries, Index&& index);

                friend bool operator<(const File& lhs, const File& rhs) noexcept;

                [[nodiscard]] std::uint32_t id() const;

                [[nodiscard]] const std::filesystem::path& path() const;

                [[nodiscard]] Entry at(std::size_t idx) const;

                [[nodiscard]] const ext::ImmutableSpan<Entry>& entries() const;

                void accumulateStatsFromEntries(
                    const std::vector<Entry>& entries,
                    const query::Request& query,
                    const Key& key,
                    query::PositionQueryOrigin origin,
                    PositionStats& stats);

                void executeQuery(
                    const query::Request& query,
                    const std::vector<Key>& keys,
                    const query::PositionQueries& queries,
                    std::vector<PositionStats>& stats);

            private:
                ext::ImmutableSpan<Entry> m_entries;
                Index m_index;
                std::uint32_t m_id;
            };

            struct FutureFile
            {
                FutureFile(std::future<Index>&& future, std::filesystem::path path);

                friend bool operator<(const FutureFile& lhs, const FutureFile& rhs) noexcept;

                [[nodiscard]] std::uint32_t id() const;

                [[nodiscard]] File get();

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
                    Job(std::filesystem::path path, std::vector<Entry>&& buffer, std::promise<Index>&& promise);

                    std::filesystem::path path;
                    std::vector<Entry> buffer;
                    std::promise<Index> promise;
                };

            public:
                AsyncStorePipeline(std::vector<std::vector<Entry>>&& buffers, std::size_t numSortingThreads = 1);

                AsyncStorePipeline(const AsyncStorePipeline&) = delete;

                ~AsyncStorePipeline();

                [[nodiscard]] std::future<Index> scheduleUnordered(const std::filesystem::path& path, std::vector<Entry>&& elements);

                [[nodiscard]] std::future<Index> scheduleOrdered(const std::filesystem::path& path, std::vector<Entry>&& elements);

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

                void sort(std::vector<Entry>& buffer);

                // works analogously to std::unique but also combines equal values
                void combine(std::vector<Entry>& buffer);

                void prepareData(std::vector<Entry>& buffer);
            };

            struct Partition
            {
                static const std::size_t mergeMemory;

                Partition() = default;

                Partition(std::filesystem::path path);

                void setPath(std::filesystem::path path);
            
                void executeQuery(
                    const query::Request& query,
                    const std::vector<Key>& keys,
                    const query::PositionQueries& queries,
                    std::vector<PositionStats>& stats);

                void mergeAll(std::function<void(const ext::ProgressReport&)> progressCallback);

                // outPath is a path of the file to output to
                void replicateMergeAll(const std::filesystem::path& outPath, std::function<void(const ext::ProgressReport&)> progressCallback);

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

                [[nodiscard]] bool empty() const;

            private:
                std::filesystem::path m_path;
                std::vector<File> m_files;

                // We store it in a set because then we can change insertion
                // order through forcing ids. It's easier to keep it
                // ordered like that. And we need it ordered all the time
                // because of queries to nextId()
                std::set<FutureFile> m_futureFiles;

                std::mutex m_mutex;

                [[nodiscard]] std::filesystem::path pathForId(std::uint32_t id) const;

                [[nodiscard]] std::filesystem::path nextPath() const;

                [[nodiscard]] Index mergeAllIntoFile(const std::filesystem::path& outFilePath, std::function<void(const ext::ProgressReport&)> progressCallback) const;

                void discoverFiles();
            };
        }

        struct Database final : persistence::Database
        {
        private:
            using BaseType = persistence::Database;

            static inline const std::filesystem::path partitionDirectory = "data";

            static inline const DatabaseManifest m_manifest = { "db_delta", true };

            static constexpr std::size_t m_totalNumDirectories = 1;

            static inline const EnumArray<GameLevel, std::string> m_headerNames = {
                "_human",
                "_engine",
                "_server"
            };

            static const std::size_t m_pgnParserMemory;

        public:
            Database(std::filesystem::path path);

            Database(std::filesystem::path path, std::size_t headerBufferMemory);

            [[nodiscard]] static const std::string& key();

            [[nodiscard]] const DatabaseManifest& manifest() const override;

            void clear() override;

            const std::filesystem::path& path() const override;

            [[nodiscard]] query::Response executeQuery(query::Request query) override;

            void mergeAll(MergeProgressCallback progressCallback = {}) override;

            void replicateMergeAll(const std::filesystem::path& path, MergeProgressCallback progressCallback = {}) override;

            ImportStats import(
                std::execution::parallel_unsequenced_policy,
                const ImportablePgnFiles& pgns,
                std::size_t memory,
                std::size_t numThreads = std::thread::hardware_concurrency(),
                ImportProgressCallback progressCallback = {}
            ) override;

            ImportStats import(
                std::execution::sequenced_policy,
                const ImportablePgnFiles& pgns,
                std::size_t memory,
                ImportProgressCallback progressCallback = {}
            ) override;

            ImportStats import(
                const ImportablePgnFiles& pgns,
                std::size_t memory,
                ImportProgressCallback progressCallback = {}
            ) override;

            void flush() override;

        private:
            std::filesystem::path m_path;

            EnumArray<GameLevel, Header> m_headers;
            std::atomic<std::uint32_t> m_nextGameIdx;

            // We only have one partition for this format
            detail::Partition m_partition;

            [[nodiscard]] EnumArray<GameLevel, Header> makeHeaders(const std::filesystem::path& path);

            [[nodiscard]] EnumArray<GameLevel, Header> makeHeaders(const std::filesystem::path& path, std::size_t headerBufferMemory);

            [[nodiscard]] std::uint32_t numGamesInHeaders() const;

            void collectFutureFiles();

            [[nodiscard]] std::vector<PackedGameHeader> queryHeadersByIndices(const std::vector<std::uint32_t>& indices, GameLevel level);

            [[nodiscard]] std::vector<GameHeader> queryHeadersByIndices(const std::vector<std::uint32_t>& indices, const std::vector<query::GameHeaderDestination>& destinations);

            void disableUnsupportedQueryFeatures(query::Request& query) const;

            [[nodiscard]] query::PositionQueryResults commitStatsAsResults(
                const query::Request& query,
                const query::PositionQueries& posQueries,
                std::vector<detail::PositionStats>& stats);

            [[nodiscard]] std::vector<detail::Key> getKeys(const query::PositionQueries& queries);

            ImportStats importPgnsImpl(
                std::execution::sequenced_policy,
                detail::AsyncStorePipeline& pipeline,
                const ImportablePgnFiles& pgns,
                std::function<void(const std::filesystem::path& file)> completionCallback
            );

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
            );

            ImportStats importPgnsImpl(
                std::execution::parallel_unsequenced_policy,
                detail::AsyncStorePipeline& pipeline,
                const ImportablePgnFiles& paths,
                std::size_t bufferSize,
                std::size_t numThreads
            );

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>& entries
            );

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>&& entries
            );

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>& entries,
                std::uint32_t id
            );

            void store(
                detail::AsyncStorePipeline& pipeline,
                std::vector<detail::Entry>&& entries,
                std::uint32_t id
            );
        };
    }
}