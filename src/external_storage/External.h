#pragma once

#include "util/Assert.h"
#include "util/Buffer.h"
#include "util/MemoryAmount.h"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace ext
{
    // TODO: we need to use non-portable functions to allow >32bit file offsets
    //       so consider moving to std::fstream

    struct Exception : public std::runtime_error
    {
        using BaseType = std::runtime_error;

        using BaseType::BaseType;
    };

    namespace detail::except
    {
        [[noreturn]] void throwAppendException(const std::filesystem::path& path, std::size_t requested, std::size_t written);

        [[noreturn]] void throwReadException(const std::filesystem::path& path, std::size_t offset, std::size_t requested, std::size_t read);

        [[noreturn]] void throwOpenException(const std::filesystem::path& path, const std::string& openmode);
    }

    [[nodiscard]] std::filesystem::path uniquePath();

    [[nodiscard]] std::filesystem::path uniquePath(const std::filesystem::path& dir);

    [[nodiscard]] constexpr std::size_t ceilToMultiple(std::size_t value, std::size_t multiple)
    {
        ASSERT(multiple > 0u);
        ASSERT(value + (multiple - 1u) >= value);

        return ((value + (multiple - 1u)) / multiple) * multiple;
    }

    [[nodiscard]] constexpr std::size_t ceilDiv(std::size_t value, std::size_t d)
    {
        ASSERT(d > 0u);
        ASSERT(value + (d - 1u) >= value);

        return (value + (d - 1u)) / d;
    }

    [[nodiscard]] constexpr std::size_t floorToMultiple(std::size_t value, std::size_t multiple)
    {
        ASSERT(multiple > 0u);

        return value / multiple * multiple;
    }

    template <typename T>
    [[nodiscard]] constexpr std::size_t numObjectsPerBufferUnit(std::size_t maxMemoryBytes, std::size_t numBufferUnits)
    {
        return maxMemoryBytes / (numBufferUnits * sizeof(T));
    }

    struct FileDeleter
    {
        void operator()(std::FILE* ptr) const noexcept;
    };

    namespace detail
    {
        using NativeFileHandle = std::FILE*;
        using FileHandle = std::unique_ptr<std::FILE, FileDeleter>;

        [[nodiscard]] auto openFile(const std::filesystem::path& path, const std::string& openmode);

        [[nodiscard]] auto fileTell(NativeFileHandle fh);

        auto fileSeek(NativeFileHandle fh, std::int64_t offset);

        auto fileSeek(NativeFileHandle fh, std::int64_t offset, int origin);

        struct FileBase
        {
            virtual ~FileBase() = default;

            virtual const std::filesystem::path& path() const = 0;

            virtual const std::string& openmode() const = 0;

            virtual bool isOpen() const = 0;

            virtual std::size_t size() const = 0;

            virtual std::size_t read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const = 0;

            virtual std::size_t append(const std::byte* source, std::size_t elementSize, std::size_t count) = 0;

            virtual void flush() = 0;

            virtual bool isPooled() const = 0;

            virtual void truncate() = 0;
        };

        // NOTE: Files are pooled - they are closed and reopened when needed -
        //       therefore it is possible that a file
        //       is deleted while it is seemingly held locked.
        //       If that happens the behaviour is undefined.
        struct PooledFile : FileBase
        {
        private:

            using FilePoolEntry = std::pair<FileHandle, const PooledFile*>;
            using FilePoolEntries = std::list<std::pair<FileHandle, const PooledFile*>>;
            using FilePoolEntryIter = typename FilePoolEntries::iterator;

            template <typename FuncT>
            decltype(auto) withHandle(FuncT&& func)
            {
                return pool().withHandle(*this, std::forward<FuncT>(func));
            }

            template <typename FuncT>
            decltype(auto) withHandle(FuncT&& func) const
            {
                return pool().withHandle(*this, std::forward<FuncT>(func));
            }

            struct FilePool
            {
                static const std::size_t numMaxConcurrentOpenFiles;

                FilePool() = default;

                FilePool(const FilePool&) = delete;
                FilePool(FilePool&&) = delete;
                FilePool& operator=(const FilePool&) = delete;
                FilePool& operator=(FilePool&&) = delete;

                template <typename FuncT>
                decltype(auto) withHandle(const PooledFile& file, FuncT&& func)
                {
                    std::unique_lock<std::mutex> lock(file.m_mutex);

                    return std::forward<FuncT>(func)(getHandle(file));
                }

                [[nodiscard]] FilePoolEntryIter noneEntry();

                void close(PooledFile& file);

            private:
                FilePoolEntries m_files;
                std::mutex m_mutex;

                void closeNoLock(PooledFile& file);

                [[nodiscard]] FileHandle reopen(const PooledFile& file);

                [[nodiscard]] FileHandle open(const PooledFile& file);

                void closeLastFile();

                [[nodiscard]] NativeFileHandle getHandle(const PooledFile& file);

                void updateEntry(const FilePoolEntryIter& it);
            };

            static FilePool& pool();

        public:

            PooledFile(std::filesystem::path path, std::string openmode);

            PooledFile(const PooledFile&) = delete;
            PooledFile(PooledFile&&) = delete;
            PooledFile& operator=(const PooledFile&) = delete;
            PooledFile& operator=(PooledFile&&) = delete;

            ~PooledFile() override;

            friend bool operator==(const PooledFile& lhs, const PooledFile& rhs) noexcept;

            [[nodiscard]] const std::filesystem::path& path() const override;

            [[nodiscard]] const std::string& openmode() const override;

            [[nodiscard]] bool isOpen() const override;

            [[nodiscard]] std::size_t size() const override;

            [[nodiscard]] std::size_t read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const override;

            [[nodiscard]] std::size_t append(const std::byte* source, std::size_t elementSize, std::size_t count) override;

            void flush() override;

            [[nodiscard]] bool isPooled() const override;

            void truncate() override;

        private:
            std::filesystem::path m_path;
            std::string m_openmode;

            // used by the pool
            mutable FilePoolEntryIter m_poolEntry;
            // times opened is NOT concurrent opens but sequential opens
            mutable std::size_t m_timesOpened;
            mutable std::mutex m_mutex;
        };


        struct File : FileBase
        {
            using NativeFileHandle = std::FILE*;

        public:
            static const std::size_t maxUnpooledOpenFiles;

            File(std::filesystem::path path, std::string openmode);

            File(const File&) = delete;
            File(File&&) = delete;
            File& operator=(const File&) = delete;
            File& operator=(File&&) = delete;

            ~File() override;

            friend bool operator==(const File& lhs, const File& rhs) noexcept;

            [[nodiscard]] const std::filesystem::path& path() const override;

            [[nodiscard]] const std::string& openmode() const override;

            void close();

            void open();

            [[nodiscard]] bool isOpen() const override;

            [[nodiscard]] std::size_t size() const override;

            [[nodiscard]] std::size_t read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const override;

            [[nodiscard]] std::size_t append(const std::byte* source, std::size_t elementSize, std::size_t count) override;

            void flush() override;

            [[nodiscard]] bool isPooled() const override;

            void truncate() override;

        private:
            std::filesystem::path m_path;
            std::string m_openmode;
            FileHandle m_handle;

            mutable std::mutex m_mutex;

            static inline std::atomic<std::size_t> m_numOpenFiles = 0;
        };

        struct ThreadPool
        {
            static constexpr std::size_t defaultNumThreads = 8;

            enum struct JobType
            {
                Read,
                Append
            };

            struct Job
            {
                JobType type;
                std::shared_ptr<FileBase> file;
                std::byte* buffer;
                std::promise<std::size_t> promise;
                std::size_t offset;
                std::size_t elementSize;
                std::size_t count;
            };

            struct ThreadPoolSpec
            {
                std::size_t numThreads;
                std::vector<std::filesystem::path> paths;
            };

            static const std::vector<ThreadPoolSpec>& specs();

            static ThreadPool& instance();

            static ThreadPool& instance(const std::filesystem::path& path);

            ThreadPool(const ThreadPool&) = delete;
            ThreadPool(ThreadPool&&) = delete;
            ThreadPool& operator=(const ThreadPool&) = delete;
            ThreadPool& operator=(ThreadPool&&) = delete;

            [[nodiscard]] std::future<std::size_t> scheduleRead(std::shared_ptr<FileBase> file, std::byte* buffer, std::size_t offset, std::size_t elementSize, std::size_t count);

            [[nodiscard]] std::future<std::size_t> scheduleAppend(std::shared_ptr<FileBase> file, const std::byte* buffer, std::size_t elementSize, std::size_t count);

            ~ThreadPool();

        private:
            std::vector<std::thread> m_threads;

            std::queue<Job> m_jobQueue;

            std::mutex m_mutex;
            std::condition_variable m_jobQueueNotEmpty;

            std::atomic<bool> m_done;

            ThreadPool(std::size_t numThreads = defaultNumThreads);

            static std::size_t poolIndexForPath(const std::filesystem::path& path);

            void worker();
        };
    }

    struct Pooled {};
    struct Async {};

    // NOTE: It is assumed that one *physical* file is not accessed concurrently anywhere.
    // NOTE: It is also assumed that the file is not changed by any means while being open.
    struct ImmutableBinaryFile
    {
        ImmutableBinaryFile(std::filesystem::path path);

        ImmutableBinaryFile(Pooled, std::filesystem::path path);

        ImmutableBinaryFile(const ImmutableBinaryFile&) = default;
        ImmutableBinaryFile(ImmutableBinaryFile&&) = default;
        ImmutableBinaryFile& operator=(const ImmutableBinaryFile&) = default;
        ImmutableBinaryFile& operator=(ImmutableBinaryFile&&) = default;

        friend bool operator==(const ImmutableBinaryFile& lhs, const ImmutableBinaryFile& rhs) noexcept;

        [[nodiscard]] bool isOpen() const;

        [[nodiscard]] const std::filesystem::path& path() const;

        [[nodiscard]] std::string openmode() const;

        [[nodiscard]] std::size_t read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const;

        [[nodiscard]] std::future<std::size_t> read(Async, std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const;

        [[nodiscard]] std::size_t size() const;
    private:
        static inline const std::string m_openmode = "rb";

        std::shared_ptr<detail::FileBase> m_file;
        detail::ThreadPool* m_threadPool;
        std::size_t m_size;
    };

    enum struct OutputMode
    {
        Truncate,
        Append
    };

    struct BinaryOutputFile
    {
        BinaryOutputFile(std::filesystem::path path, OutputMode mode = OutputMode::Truncate);

        BinaryOutputFile(Pooled, std::filesystem::path path, OutputMode mode = OutputMode::Truncate);

        BinaryOutputFile(const BinaryOutputFile&) = delete;
        BinaryOutputFile(BinaryOutputFile&&) = default;
        BinaryOutputFile& operator=(const BinaryOutputFile&) = delete;
        BinaryOutputFile& operator=(BinaryOutputFile&&) = default;

        virtual ~BinaryOutputFile();

        [[nodiscard]] bool isOpen() const;

        [[nodiscard]] const std::filesystem::path& path() const;

        [[nodiscard]] std::string openmode() const;

        [[nodiscard]] virtual std::size_t append(const std::byte* source, std::size_t elementSize, std::size_t count) const;

        [[nodiscard]] virtual std::future<std::size_t> append(Async, const std::byte* destination, std::size_t elementSize, std::size_t count) const;

        // reopens the file in readonly mode
        [[nodiscard]] ImmutableBinaryFile seal();

        void flush();

    private:
        static inline const std::string m_openmodeTruncate = "wb";
        static inline const std::string m_openmodeAppend = "ab";

        std::shared_ptr<detail::FileBase> m_file;
        detail::ThreadPool* m_threadPool;
    };

    struct ObservableBinaryOutputFile : BinaryOutputFile
    {
        using CallbackType = std::function<void(const std::byte*, std::size_t, std::size_t)>;

        ObservableBinaryOutputFile(CallbackType callback, std::filesystem::path path, OutputMode mode = OutputMode::Truncate);

        ObservableBinaryOutputFile(Pooled, CallbackType callback, std::filesystem::path path, OutputMode mode = OutputMode::Truncate);

        ObservableBinaryOutputFile(const ObservableBinaryOutputFile&) = delete;
        ObservableBinaryOutputFile(ObservableBinaryOutputFile&&) = default;
        ObservableBinaryOutputFile& operator=(const ObservableBinaryOutputFile&) = delete;
        ObservableBinaryOutputFile& operator=(ObservableBinaryOutputFile&&) = default;

        [[nodiscard]] std::size_t append(const std::byte* source, std::size_t elementSize, std::size_t count) const override;

        [[nodiscard]] std::future<std::size_t> append(Async, const std::byte* source, std::size_t elementSize, std::size_t count) const override;

    private:
        CallbackType m_callback;
    };

    struct BinaryInputOutputFile
    {
        BinaryInputOutputFile(std::filesystem::path path, OutputMode mode = OutputMode::Truncate);

        BinaryInputOutputFile(Pooled, std::filesystem::path path, OutputMode mode = OutputMode::Truncate);

        BinaryInputOutputFile(const BinaryInputOutputFile&) = delete;
        BinaryInputOutputFile(BinaryInputOutputFile&&) = default;
        BinaryInputOutputFile& operator=(const BinaryInputOutputFile&) = delete;
        BinaryInputOutputFile& operator=(BinaryInputOutputFile&&) = default;

        friend bool operator==(const BinaryInputOutputFile& lhs, const BinaryInputOutputFile& rhs) noexcept;

        [[nodiscard]] bool isOpen() const;

        [[nodiscard]] const std::filesystem::path& path() const;

        [[nodiscard]] std::string openmode() const;

        [[nodiscard]] std::size_t read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const;

        [[nodiscard]] std::future<std::size_t> read(Async, std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const;

        [[nodiscard]] std::size_t size() const;

        [[nodiscard]] std::size_t append(const std::byte* source, std::size_t elementSize, std::size_t count) const;

        [[nodiscard]] std::future<std::size_t> append(Async, const std::byte* destination, std::size_t elementSize, std::size_t count) const;

        void truncate();

        // reopens the file in readonly mode
        [[nodiscard]] ImmutableBinaryFile seal();

        void flush();

    private:
        static inline const std::string m_openmodeTruncate = "wb+";
        static inline const std::string m_openmodeAppend = "ab+";

        std::shared_ptr<detail::FileBase> m_file;
        detail::ThreadPool* m_threadPool;
    };

    template <typename T>
    struct ImmutableSpan
    {
        static_assert(std::is_trivially_copyable_v<T>);

        struct SequentialIterator
        {
            struct Sentinel {};

            using value_type = T;
            using difference_type = std::ptrdiff_t;
            using reference = const T &;
            using iterator_category = std::input_iterator_tag;
            using pointer = const T*;

            SequentialIterator(const ImmutableBinaryFile& file, std::size_t begin, std::size_t end, util::DoubleBuffer<T>&& buffer) :
                m_file(file),
                m_fileBegin(begin * sizeof(T)),
                m_fileEnd(end * sizeof(T)),
                m_buffer(std::move(buffer)),
                m_bufBegin(nullptr),
                m_bufEnd(nullptr)
            {
                ASSERT(m_fileBegin <= m_fileEnd);
                ASSERT(m_fileEnd <= m_file.size());
                ASSERT(m_file.size() % sizeof(T) == 0);

                refillBuffer();
                refillBuffer();
            }

            SequentialIterator& operator++()
            {
                ASSERT(m_bufBegin != nullptr);

                ++m_bufBegin;

                if (m_bufBegin == m_bufEnd)
                {
                    refillBuffer();
                }

                return *this;
            }

            [[nodiscard]] bool friend operator==(const SequentialIterator& lhs, Sentinel rhs) noexcept
            {
                return lhs.m_bufBegin == nullptr;
            }

            [[nodiscard]] bool friend operator!=(const SequentialIterator& lhs, Sentinel rhs) noexcept
            {
                return !(lhs == rhs);
            }

            [[nodiscard]] reference operator*() const
            {
                ASSERT(m_bufBegin != nullptr);

                return *m_bufBegin;
            }

            [[nodiscard]] pointer operator->() const
            {
                ASSERT(m_bufBegin != nullptr);

                return m_bufBegin;
            }

        private:
            ImmutableBinaryFile m_file;
            std::size_t m_fileBegin;
            std::size_t m_fileEnd;
            util::DoubleBuffer<T> m_buffer;
            T* m_bufBegin;
            T* m_bufEnd;
            std::future<std::size_t> m_future;

            void refillBuffer()
            {
                waitForBuffer();

                ASSERT(m_bufBegin == m_buffer.data() || m_bufBegin == nullptr);
                ASSERT(!m_future.valid());

                const std::size_t bytesLeft = m_fileEnd - m_fileBegin;
                if (bytesLeft == 0)
                {
                    // simulate empty read so we don't request a read for 0 bytes
                    std::promise<std::size_t> promise;
                    m_future = promise.get_future();
                    promise.set_value(0);
                    return;
                }

                const std::size_t numObjectsToRead = std::min(bytesLeft / sizeof(T), m_buffer.size());

                m_future = m_file.read(
                    Async{},
                    reinterpret_cast<std::byte*>(m_buffer.back_data()),
                    m_fileBegin,
                    sizeof(T),
                    numObjectsToRead
                );
            }

            void waitForBuffer()
            {
                if (!m_future.valid()) return;

                ASSERT(m_bufBegin == m_bufEnd);

                const std::size_t numObjectsRead = m_future.get();
                m_buffer.swap();

                m_fileBegin += numObjectsRead * sizeof(T);
                if (numObjectsRead)
                {
                    m_bufBegin = m_buffer.data();
                    m_bufEnd = m_bufBegin + numObjectsRead;
                }
                else
                {
                    m_bufBegin = nullptr;
                    m_bufEnd = nullptr;
                }
            }
        };

        struct RandomAccessIterator
        {
            template <typename T>
            struct PointerWrapper
            {
                static_assert(std::is_trivially_copyable_v<T>);

                PointerWrapper(const T& value) :
                    m_value(value)
                {
                }

                [[nodiscard]] T operator*() const
                {
                    return m_value;
                }

            private:
                T m_value;
            };

            friend struct ImmutableSpan<T>;

            using value_type = T;
            using difference_type = std::ptrdiff_t;
            using reference = const T &;
            using iterator_category = std::random_access_iterator_tag;
            using pointer = PointerWrapper<T>;

            RandomAccessIterator(const ImmutableBinaryFile& file, std::size_t i) :
                m_file(file),
                m_lastRead{},
                m_idx(i),
                m_lastReadIdx(-1)
            {
                ASSERT(m_idx * sizeof(T) <= m_file.size());
                ASSERT(m_file.size() % sizeof(T) == 0);
            }

            RandomAccessIterator& operator++()
            {
                ++m_idx;
                return *this;
            }

            RandomAccessIterator& operator--()
            {
                --m_idx;
                return *this;
            }

            [[nodiscard]] RandomAccessIterator operator++(int)
            {
                return RandomAccessIterator(m_file, m_idx++);
            }

            [[nodiscard]] RandomAccessIterator operator--(int)
            {
                return RandomAccessIterator(m_file, m_idx--);
            }

            RandomAccessIterator& operator+=(difference_type n)
            {
                m_idx += n;
                return *this;
            }

            RandomAccessIterator& operator-=(difference_type n)
            {
                m_idx -= n;
                return *this;
            }

            [[nodiscard]] friend RandomAccessIterator operator+(const RandomAccessIterator& lhs, difference_type rhs)
            {
                return RandomAccessIterator(lhs.m_file, lhs.m_idx + rhs);
            }

            [[nodiscard]] friend RandomAccessIterator operator+(difference_type lhs, const RandomAccessIterator& rhs)
            {
                return RandomAccessIterator(rhs.m_file, rhs.m_idx + lhs);
            }

            [[nodiscard]] friend RandomAccessIterator operator-(const RandomAccessIterator& lhs, difference_type rhs)
            {
                return RandomAccessIterator(lhs.m_file, lhs.m_idx - rhs);
            }

            [[nodiscard]] friend difference_type operator-(const RandomAccessIterator& lhs, const RandomAccessIterator& rhs)
            {
                return static_cast<difference_type>(lhs.m_idx - rhs.m_idx);
            }

            [[nodiscard]] bool friend operator==(const RandomAccessIterator& lhs, const RandomAccessIterator& rhs) noexcept
            {
                return lhs.m_idx == rhs.m_idx;
            }

            [[nodiscard]] bool friend operator!=(const RandomAccessIterator& lhs, const RandomAccessIterator& rhs) noexcept
            {
                return lhs.m_idx != rhs.m_idx;
            }

            [[nodiscard]] bool friend operator<(const RandomAccessIterator& lhs, const RandomAccessIterator& rhs) noexcept
            {
                return lhs.m_idx < rhs.m_idx;
            }

            [[nodiscard]] bool friend operator>(const RandomAccessIterator& lhs, const RandomAccessIterator& rhs) noexcept
            {
                return lhs.m_idx > rhs.m_idx;
            }

            [[nodiscard]] bool friend operator<=(const RandomAccessIterator& lhs, const RandomAccessIterator& rhs) noexcept
            {
                return lhs.m_idx <= rhs.m_idx;
            }

            [[nodiscard]] bool friend operator>=(const RandomAccessIterator& lhs, const RandomAccessIterator& rhs) noexcept
            {
                return lhs.m_idx >= rhs.m_idx;
            }

            [[nodiscard]] reference operator[](std::size_t i) const
            {
                ASSERT(i < m_file.size());

                const auto idx = m_idx + i;

                if (m_lastReadIdx != idx)
                {
                    const std::size_t elementsRead = m_file.read(
                        reinterpret_cast<std::byte*>(&m_lastRead),
                        idx * sizeof(T),
                        sizeof(T),
                        1u
                    );

                    if (elementsRead != 1)
                    {
                        detail::except::throwReadException(m_file.path(), idx, 1, elementsRead);
                    }
                    m_lastReadIdx = idx;
                }
                return m_lastRead;
            }

            [[nodiscard]] reference operator*() const
            {
                return operator[](0);
            }

            [[nodiscard]] pointer operator->() const
            {
                ASSERT(m_idx < m_file.size());

                return { operator*() };
            }

        private:
            ImmutableBinaryFile m_file;
            mutable T m_lastRead;
            std::size_t m_idx;
            mutable std::size_t m_lastReadIdx;
        };

        using value_type = T;
        using size_type = std::size_t;

        using iterator = SequentialIterator;
        using const_iterator = SequentialIterator;

        ImmutableSpan(const ImmutableBinaryFile& file) :
            m_file(file),
            m_begin(0u),
            m_end(m_file.size() / sizeof(T))
        {
            ASSERT(m_file.size() % sizeof(T) == 0);
        }

        ImmutableSpan(const ImmutableBinaryFile& file, std::size_t begin, std::size_t size) :
            m_file(file),
            m_begin(begin),
            m_end(begin + size)
        {
            ASSERT(m_begin <= m_end);
            ASSERT(m_end * sizeof(T) <= m_file.size());
            ASSERT(m_file.size() % sizeof(T) == 0);
        }

        ImmutableSpan(const RandomAccessIterator& begin, const RandomAccessIterator& end) :
            m_file(begin.m_file),
            m_begin(begin.m_idx),
            m_end(end.m_idx)
        {
            ASSERT(begin.m_file == end.m_file);
            ASSERT(begin.m_idx <= end.m_idx);
            ASSERT(end.m_idx * sizeof(T) <= m_file.size());
        }

        ImmutableSpan(const ImmutableSpan&) = default;
        ImmutableSpan(ImmutableSpan&&) = default;
        ImmutableSpan& operator=(const ImmutableSpan&) = default;
        ImmutableSpan& operator=(ImmutableSpan&&) = default;

        [[nodiscard]] decltype(auto) path() const
        {
            return m_file.path();
        }

        [[nodiscard]] std::size_t size() const
        {
            return m_end - m_begin;
        }

        [[nodiscard]] std::size_t size_bytes() const
        {
            return size() * sizeof(T);
        }

        [[nodiscard]] bool empty() const
        {
            return m_begin == m_end;
        }

        [[nodiscard]] std::size_t read(T* destination, std::size_t offset, std::size_t count) const
        {
            const std::size_t elementsRead = m_file.read(
                reinterpret_cast<std::byte*>(destination),
                (m_begin + offset) * sizeof(T),
                sizeof(T),
                count
            );

            return elementsRead;
        }

        std::future<std::size_t> read(Async, T* destination, std::size_t offset, std::size_t count) const
        {
            return m_file.read(
                Async{},
                reinterpret_cast<std::byte*>(destination),
                (m_begin + offset) * sizeof(T),
                sizeof(T),
                count
            );
        }

        [[nodiscard]] std::size_t read(T* destination) const
        {
            return read(destination, 0, size());
        }

        [[nodiscard]] std::future<std::size_t> read(Async, T* destination) const
        {
            return read(Async{}, destination, 0, size());
        }

        [[nodiscard]] T operator[](std::size_t i) const
        {
            ASSERT(i < size());

            T value;
            const std::size_t elementsRead = read(&value, i, 1u);
            if (elementsRead != 1)
            {
                detail::except::throwReadException(path(), m_begin + i, 1, elementsRead);
            }

            return value;
        }

        [[nodiscard]] T front() const
        {
            ASSERT(!empty());

            return operator[](m_begin);
        }

        [[nodiscard]] T back() const
        {
            ASSERT(!empty());

            return operator[](m_end - 1u);
        }

        [[nodiscard]] auto begin_seq(util::DoubleBuffer<T>&& buffer = util::DoubleBuffer<T>(1024)) const
        {
            return SequentialIterator(m_file, m_begin, m_end, std::move(buffer));
        }

        [[nodiscard]] auto end_seq() const
        {
            return typename SequentialIterator::Sentinel{};
        }

        [[nodiscard]] auto cbegin_seq(util::DoubleBuffer<T>&& buffer = util::DoubleBuffer<T>(1024)) const
        {
            return begin_seq(std::move(buffer));
        }

        [[nodiscard]] auto cend_seq() const
        {
            return end_seq();
        }

        [[nodiscard]] auto begin_rand() const
        {
            return RandomAccessIterator(m_file, m_begin);
        }

        [[nodiscard]] auto end_rand() const
        {
            return RandomAccessIterator(m_file, m_end);
        }

        [[nodiscard]] auto cbegin_rand() const
        {
            return begin_rand();
        }

        [[nodiscard]] auto cend_rand() const
        {
            return end_rand();
        }

        [[nodiscard]] auto begin(util::DoubleBuffer<T>&& buffer = util::DoubleBuffer<T>(1024)) const
        {
            return begin_seq(std::move(buffer));
        }

        [[nodiscard]] auto end() const
        {
            return end_seq();
        }

        [[nodiscard]] auto cbegin(util::DoubleBuffer<T>&& buffer = util::DoubleBuffer<T>(1024)) const
        {
            return begin(std::move(buffer));
        }

        [[nodiscard]] auto cend() const
        {
            return end();
        }

        [[nodiscard]] ImmutableSpan<T> subspan(std::size_t offset, std::size_t s) const
        {
            ASSERT(offset + s <= size());

            return ImmutableSpan(m_file, m_begin + offset, s);
        }

        [[nodiscard]] ImmutableSpan<T> subspan(const RandomAccessIterator& begin, const RandomAccessIterator& end) const
        {
            ASSERT(begin.m_file == end.m_file);
            ASSERT(begin.m_file == m_file);
            ASSERT(begin.m_idx <= end.m_idx);
            ASSERT(m_begin <= begin.m_idx);
            ASSERT(end.m_idx <= m_end);

            return ImmutableSpan(m_file, begin.m_idx, end - begin);
        }

    private:
        ImmutableBinaryFile m_file;
        std::size_t m_begin;
        std::size_t m_end;
    };

    // NOTE: This doesn't really functions like an iterator.
    //       That is because it can't fullfill all requirements - mainly copy constructibility.
    template <typename T>
    struct BackInserter
    {
        static_assert(std::is_trivially_copyable_v<T>);

        using value_type = T;

        // Default buffer size makes it work like it is unbuffered
        BackInserter(BinaryOutputFile& file) :
            m_file(&file),
            m_buffer(1),
            m_nextEmpty(m_buffer.data()),
            m_live(true),
            m_numObjectsToWrite(0)
        {
        }

        BackInserter(BinaryOutputFile& file, util::DoubleBuffer<T>&& buffer) :
            m_file(&file),
            m_buffer(std::move(buffer)),
            m_nextEmpty(m_buffer.data()),
            m_live(true),
            m_numObjectsToWrite(0)
        {
        }

        BackInserter(const BackInserter&) = delete;
        BackInserter(BackInserter&&) = default;
        BackInserter& operator=(const BackInserter&) = delete;
        BackInserter& operator=(BackInserter&&) = default;

        ~BackInserter()
        {
            // Don't throw from a destructor
            try
            {
                flush();
                m_live = false;
            }
            catch (...)
            {
            }
        }

        [[nodiscard]] const std::filesystem::path& path() const
        {
            ASSERT(m_live);

            return m_file->path();
        }

        template <typename... Ts>
        void emplace(Ts&& ... args)
        {
            ASSERT(m_live);

            *(m_nextEmpty++) = T{ std::forward<Ts>(args)... };

            if (m_nextEmpty == m_buffer.data() + m_buffer.size())
            {
                writeBuffer();
            }
        }

        // pods don't benefit from move semantics
        void push(const T& value)
        {
            ASSERT(m_live);

            *(m_nextEmpty++) = value;

            if (m_nextEmpty == m_buffer.data() + m_buffer.size())
            {
                writeBuffer();
            }
        }

        void append(const T* data, std::size_t count)
        {
            ASSERT(m_live);

            const std::size_t bufferSizeLeft = m_buffer.size() - (m_nextEmpty - m_buffer.data());
            // TODO: should be <=? Or has to be < because we have to leave space for emplace/push?
            if (count < bufferSizeLeft)
            {
                // if we can fit it in the buffer with some space left then do it
                std::copy(data, data + count, m_nextEmpty);
                m_nextEmpty += count;
            }
            else
            {
                // TODO: try to write to a buffer after swapping

                // if we would fill the buffer completely or it doesn't fit then flush what we have
                // and write straight from the passed data
                writeBuffer();

                // since this write omits the buffer we have to make
                // sure it's sequenced after the previous one completes
                waitForBufferWritten();

                const std::size_t numWritten = m_file->append(reinterpret_cast<const std::byte*>(data), sizeof(T), count);
                m_file->flush();

                if (numWritten != count)
                {
                    detail::except::throwAppendException(path(), count, numWritten);
                }
            }
        }

        void flush()
        {
            if (m_live)
            {
                writeBuffer();
                waitForBufferWritten();
                m_file->flush();
            }
        }

        [[nodiscard]] util::DoubleBuffer<T> buffer() &&
        {
            flush();
            m_live = false;
            return std::move(m_buffer);
        }

    private:
        BinaryOutputFile* m_file;
        util::DoubleBuffer<T> m_buffer;
        T* m_nextEmpty;
        bool m_live;
        std::future<std::size_t> m_future;
        std::size_t m_numObjectsToWrite;

        void writeBuffer()
        {
            if (m_buffer.data() == nullptr)
            {
                return;
            }

            waitForBufferWritten();

            m_buffer.swap();
            const std::size_t numObjectsToWrite = m_nextEmpty - m_buffer.back_data();

            m_numObjectsToWrite = numObjectsToWrite;

            if (numObjectsToWrite)
            {
                m_future = m_file->append(
                    Async{},
                    reinterpret_cast<const std::byte*>(m_buffer.back_data()),
                    sizeof(T),
                    m_numObjectsToWrite
                );
            }

            m_nextEmpty = m_buffer.data();
        }

        void waitForBufferWritten()
        {
            if (m_future.valid())
            {
                ASSERT(m_buffer.data() != nullptr);

                const std::size_t numWrittenObjects = m_future.get();
                if (numWrittenObjects != m_numObjectsToWrite)
                {
                    detail::except::throwAppendException(path(), m_numObjectsToWrite, numWrittenObjects);
                }
            }
        }
    };

    template <typename T>
    struct Vector
    {
        static_assert(std::is_trivially_copyable_v<T>);

        using value_type = T;

        // it "borrows" the file, allows its later retrieval
        Vector(BinaryInputOutputFile&& file, util::DoubleBuffer<T>&& buffer = util::DoubleBuffer<T>(1024)) :
            m_file(std::move(file)),
            m_buffer(std::move(buffer)),
            m_nextEmpty(m_buffer.data()),
            m_size(m_file.size()),
            m_numObjectsToWrite(0)
        {
        }

        Vector(const Vector&) = delete;
        Vector(Vector&&) = default;
        Vector& operator=(const Vector&) = delete;
        Vector& operator=(Vector&&) = default;

        ~Vector()
        {
            // Don't throw from a destructor
            try
            {
                flush();
            }
            catch (std::runtime_error&)
            {

            }
        }

        [[nodiscard]] decltype(auto) path() const
        {
            return m_file.path();
        }

        [[nodiscard]] std::size_t size() const
        {
            return m_size;
        }

        [[nodiscard]] std::size_t size_bytes() const
        {
            return size() * sizeof(T);
        }

        [[nodiscard]] bool empty() const
        {
            return m_size == 0;
        }

        void clear()
        {
            flush();
            m_file.truncate();
            m_size = 0;
        }

        [[nodiscard]] std::size_t readNoFlush(T* destination, std::size_t offset, std::size_t count) const
        {
            const std::size_t elementsRead = m_file.read(
                reinterpret_cast<std::byte*>(destination),
                offset * sizeof(T),
                sizeof(T),
                count
            );

            return elementsRead;
        }

        [[nodiscard]] std::future<std::size_t> readNoFlush(Async, T* destination, std::size_t offset, std::size_t count) const
        {
            return m_file.read(
                Async{},
                reinterpret_cast<std::byte*>(destination),
                offset * sizeof(T),
                sizeof(T),
                count
            );
        }

        [[nodiscard]] std::size_t readNoFlush(T* destination) const
        {
            return readNoFlush(destination, 0, size());
        }

        [[nodiscard]] std::size_t readNoFlush(Async, T* destination) const
        {
            return readNoFlush(Async{}, destination, 0, size());
        }

        [[nodiscard]] std::size_t read(T* destination, std::size_t offset, std::size_t count)
        {
            flush();

            return readNoFlush(destination, offset, count);
        }

        [[nodiscard]] std::future<std::size_t> read(Async, T* destination, std::size_t offset, std::size_t count)
        {
            flush();

            return readNoFlush(Async{}, destination, offset, count);
        }

        [[nodiscard]] std::size_t read(T* destination)
        {
            return read(destination, 0, size());
        }

        [[nodiscard]] std::size_t read(Async, T* destination)
        {
            return read(Async{}, destination, 0, size());
        }

        [[nodiscard]] T operator[](std::size_t i)
        {
            ASSERT(i < size());

            T value;
            const std::size_t readElements = read(&value, i, 1u);
            if (readElements != 1u)
            {
                detail::except::throwReadException(path(), i, 1, readElements);
            }

            return value;
        }

        [[nodiscard]] T front()
        {
            ASSERT(!empty());

            return operator[](0);
        }

        [[nodiscard]] T back()
        {
            ASSERT(!empty());

            return operator[](size() - 1u);
        }

        template <typename... Ts>
        void emplace_back(Ts&& ... args)
        {
            ++m_size;
            *(m_nextEmpty++) = T{ std::forward<Ts>(args)... };

            if (m_nextEmpty == m_buffer.data() + m_buffer.size())
            {
                writeBuffer();
            }
        }

        // pods don't benefit from move semantics
        void push_back(const T& value)
        {
            ++m_size;
            *(m_nextEmpty++) = value;

            if (m_nextEmpty == m_buffer.data() + m_buffer.size())
            {
                writeBuffer();
            }
        }

        void append(const T* data, std::size_t count)
        {
            m_size += count;
            const std::size_t bufferSizeLeft = m_buffer.size() - (m_nextEmpty - m_buffer.data());
            if (count < bufferSizeLeft)
            {
                // if we can fit it in the buffer with some space left then do it
                std::copy(data, data + count, m_nextEmpty);
                m_nextEmpty += count;
            }
            else
            {
                // if we would fill the buffer completely or it doesn't fit then flush what we have
                // and write straight from the passed data
                writeBuffer();

                // since this write omits the buffer we have to make
                // sure it's sequenced after the previous one completes
                waitForBufferWritten();

                const std::size_t numWritten = m_file.append(reinterpret_cast<const std::byte*>(data), sizeof(T), count);
                m_file.flush();

                if (numWritten != count)
                {
                    detail::except::throwAppendException(path(), count, numWritten);
                }
            }
        }

        void flush()
        {
            writeBuffer();
            waitForBufferWritten();
            m_file.flush();
        }

    private:
        BinaryInputOutputFile m_file;
        util::DoubleBuffer<T> m_buffer;
        T* m_nextEmpty;
        std::size_t m_size;
        std::future<std::size_t> m_future;
        std::size_t m_numObjectsToWrite;

        void writeBuffer()
        {
            if (m_buffer.data() == nullptr)
            {
                return;
            }

            const std::size_t numObjectsToWrite = m_nextEmpty - m_buffer.data();
            if (numObjectsToWrite == 0u)
            {
                return;
            }

            waitForBufferWritten();

            m_buffer.swap();
            m_numObjectsToWrite = numObjectsToWrite;
            m_future = m_file.append(
                Async{},
                reinterpret_cast<const std::byte*>(m_buffer.back_data()),
                sizeof(T),
                m_numObjectsToWrite
            );

            m_nextEmpty = m_buffer.data();
        }

        void waitForBufferWritten()
        {
            if (m_future.valid())
            {
                const std::size_t numWrittenObjects = m_future.get();
                if (numWrittenObjects != m_numObjectsToWrite)
                {
                    detail::except::throwAppendException(path(), m_numObjectsToWrite, numWrittenObjects);
                }
            }
        }
    };

    template <typename BeginIt, typename EndIt>
    struct IterRange
    {
        template <typename ContT>
        IterRange(ContT&& cont) :
            m_begin(std::begin(std::forward<ContT>(cont))),
            m_end(std::end(std::forward<ContT>(cont)))
        {
        }

        IterRange(const BeginIt& begin, const EndIt& end) :
            m_begin(begin),
            m_end(end)
        {
        }

        IterRange(BeginIt&& begin, EndIt&& end) :
            m_begin(std::move(begin)),
            m_end(std::move(end))
        {
        }

        [[nodiscard]] BeginIt& begin()
        {
            return m_begin;
        }

        [[nodiscard]] EndIt& end()
        {
            return m_end;
        }

        [[nodiscard]] const BeginIt& begin() const
        {
            return m_begin;
        }

        [[nodiscard]] const EndIt& end() const
        {
            return m_end;
        }

        [[nodiscard]] IterRange subrange(BeginIt&& begin, EndIt&& end) const
        {
            return { std::move(begin), std::move(end) };
        }

        [[nodiscard]] auto distance() const
        {
            return std::distance(m_begin, m_end);
        }

    private:
        BeginIt m_begin;
        EndIt m_end;
    };

    template <typename ContT>
    using ContainerIterRange = IterRange<
        decltype(std::begin(std::declval<ContT>())),
        decltype(std::end(std::declval<ContT>()))
    >;

    // the directory is not deleted afterwards as we cannot know when it can be safely deleted
    struct TemporaryPaths
    {
        TemporaryPaths(std::filesystem::path dir = "");

        TemporaryPaths(const TemporaryPaths&) = delete;
        TemporaryPaths(TemporaryPaths&&) = default;

        TemporaryPaths& operator=(const TemporaryPaths&) = delete;
        TemporaryPaths& operator=(TemporaryPaths&&) = default;

        [[nodiscard]] std::filesystem::path& next();

        void clear();

        ~TemporaryPaths();

    private:
        std::filesystem::path m_dir;
        std::vector<std::filesystem::path> m_paths;
    };

    struct Progress
    {
        std::size_t workDone;
        std::size_t workTotal;

        [[nodiscard]] double ratio() const
        {
            return static_cast<double>(workDone) / static_cast<double>(workTotal);
        }
    };

    using ProgressCallback = std::function<void(const Progress&)>;

    struct AuxilaryStorage
    {
        std::size_t memory;
        std::filesystem::path tempdir = ".ext";
    };

    namespace detail
    {
        struct ProgressTracker
        {
            ProgressTracker(ProgressCallback progressCallback) :
                m_progress{ 0, 0 },
                m_callback(std::move(progressCallback))
            {
            }

            void doCallback() const
            {
                if (m_callback)
                {
                    m_callback(m_progress);
                }
            }

            void onWorkDone(std::size_t work)
            {
                m_progress.workDone += work;
                doCallback();
            }

            void setTotalWork(std::size_t work)
            {
                m_progress.workTotal = work;
            }

            [[nodiscard]] const Progress& progress() const
            {
                return m_progress;
            }

        private:
            Progress m_progress;
            ProgressCallback m_callback;
        };
    }

    template <typename T>
    [[nodiscard]] std::size_t writeFile(const std::filesystem::path& path, const T* data, const std::size_t count)
    {
        static_assert(std::is_trivially_copyable_v<T>);

        BinaryOutputFile file(path, OutputMode::Truncate);
        return file.append(reinterpret_cast<const std::byte*>(data), sizeof(T), count);
    }

    template <typename T>
    [[nodiscard]] std::future<std::size_t> writeFile(Async, const std::filesystem::path& path, const T* data, const std::size_t count)
    {
        static_assert(std::is_trivially_copyable_v<T>);

        BinaryOutputFile file(Pooled{}, path, OutputMode::Truncate);
        // This is fine, the async pipeline uses shared_ptr so the file will be kept alive.
        return file.append(Async{}, reinterpret_cast<const std::byte*>(data), sizeof(T), count);
    }

    template <typename T>
    [[nodiscard]] std::vector<T> readFile(const std::filesystem::path& path)
    {
        static_assert(std::is_trivially_copyable_v<T>);

        ImmutableBinaryFile file(path);
        std::vector<T> data(file.size() / sizeof(T));
        (void)file.read(reinterpret_cast<std::byte*>(data.data()), 0, sizeof(T), file.size() / sizeof(T));
        return data;
    }

    template <typename T>
    [[nodiscard]] std::future<std::size_t> readFile(Async, const std::filesystem::path& path, std::vector<T>& data)
    {
        static_assert(std::is_trivially_copyable_v<T>);

        ImmutableBinaryFile file(path);
        data.clear();
        data.resize(file.size() / sizeof(T));
        // This is fine, the async pipeline uses shared_ptr so the file will be kept alive.
        return file.read(Async{}, reinterpret_cast<std::byte*>(data.data()), 0, sizeof(T), file.size() / sizeof(T));
    }

    struct MergePass
    {
        // This directory is only a hint. It's not used actively.
        // It usually makes sense to have 2 interchanging directories
        // for multipass merge, so it makes sense to include both.
        // The data can actually be read from somewhere else.
        std::filesystem::path readDir;

        // This directory is used for the resulting files.
        std::filesystem::path writeDir;

        MergePass(const std::filesystem::path& read, const std::filesystem::path& write) :
            readDir(read),
            writeDir(write)
        {
        }

        void invert()
        {
            std::swap(readDir, writeDir);
        }
    };

    struct MergePlan
    {
        std::vector<MergePass> passes;

        void invert()
        {
            for (auto& pass : passes)
            {
                pass.invert();
            }
        }

        int numPasses() const
        {
            return static_cast<int>(passes.size());
        }

        [[nodiscard]] std::filesystem::path writeDirForPass(int passId) const
        {
            const int numPlannedPasses = numPasses();

            if (numPlannedPasses == 0)
            {
                return std::filesystem::path(".");
            }

            if (passId < numPlannedPasses)
            {
                return passes[passId].writeDir;
            }
            else
            {
                // If this pass wasn't planned then swap read/write
                // dirs with every pass. This means that for the first pass 
                // after the plan ended we want to use the readDir.
                if ((passId - numPlannedPasses) % 2 == 0)
                {
                    return passes.back().readDir;
                }
                else
                {
                    return passes.back().writeDir;
                }
            }
        }
    };

    using MergePassFinishedCallback = std::function<void(int)>;

    struct MergeCallbacks
    {
        ProgressCallback progressCallback;
        MergePassFinishedCallback passFinishedCallback;
    };

    namespace detail::merge
    {
        extern const std::size_t maxBatchSize;
        extern const MemoryAmount outputBufferSize;
        extern const MemoryAmount inputBufferSize;
        
        static constexpr std::size_t priorityQueueMergeThreshold = 24;

        [[nodiscard]] std::size_t merge_assess_work(
            std::vector<std::size_t>::const_iterator inSizesBegin,
            std::vector<std::size_t>::const_iterator inSizesEnd
        );

        template <typename T, typename ContainerT>
        [[nodiscard]] std::size_t merge_assess_work(ContainerIterRange<ContainerT> in)
        {
            static_assert(std::is_same_v<typename ContainerT::value_type, ImmutableSpan<T>>);

            std::vector<std::size_t> sizes;
            sizes.reserve(in.distance());
            for (auto& i : in)
            {
                sizes.emplace_back(i.size());
            }
            return merge_assess_work(std::begin(sizes), std::end(sizes));
        }

        // new merge implementation
        // the merge is stable - ie. it takes as many values from the first input as possible, then next, and so on
        template <typename T, typename CompT, typename FuncT>
        void merge_for_each_no_recurse(
            ContainerIterRange<std::vector<ImmutableSpan<T>>> in,
            FuncT&& func,
            CompT cmp,
            detail::ProgressTracker& progress
        )
        {
            // TODO: sfinae if other types can be merged
            using InputRange = ContainerIterRange<ImmutableSpan<T>>;

            const std::size_t numInputs = in.distance();

            ASSERT(numInputs <= maxBatchSize);

            const std::size_t inputBufferElements = inputBufferSize.elements<T>() / 2 + 1;

            const std::size_t progressCallbackThreshold = outputBufferSize.elements<T>() / 2 + 1;

            // gather non empty ranges
            std::vector<InputRange> iters;
            iters.reserve(numInputs);
            for (auto& i : in)
            {
                const std::size_t size = i.size();
                if (size == 0)
                {
                    continue;
                }

                const std::size_t bufferSize = std::min(inputBufferElements, size);

                iters.emplace_back(i.begin(util::DoubleBuffer<T>(bufferSize)), i.end());
            }

            ASSERT(iters.size() > 1);

            std::size_t numNewProcessed = 0;

            {
                {
                    // Priority queue requires a comparator with a reversed comparison.
                    // Also we ensure that the merge is stable by forcing order of iter indices.
                    struct Comp : CompT
                    {
                        Comp(CompT t) : CompT(t) {}

                        bool operator()(
                            const std::pair<T, std::size_t>& lhs, 
                            const std::pair<T, std::size_t>& rhs
                            ) const noexcept
                        {
                            if (CompT::operator()(rhs.first, lhs.first)) return true;
                            else if (CompT::operator()(lhs.first, rhs.first)) return false;
                            return lhs.second > rhs.second;
                        }
                    };

                    // We use a priority queue unless the number of files is small
                    if (iters.size() > priorityQueueMergeThreshold)
                    {
                        std::priority_queue<
                            std::pair<T, std::size_t>, 
                            std::vector<std::pair<T, std::size_t>>, 
                            Comp
                        > queue{ Comp(cmp) };

                        for (std::size_t i = 0; i < iters.size(); ++i)
                        {
                            ASSERT(iters[i].begin() != iters[i].end());
                            queue.emplace(*(iters[i].begin()), i);
                        }

                        while (queue.size() > priorityQueueMergeThreshold)
                        {
                            auto [value, minIdx] = queue.top();
                            queue.pop();

                            // write the minimum value
                            func(value);

                            // update iterator
                            auto& it = ++iters[minIdx].begin();

                            // if it's at the end remove it
                            if (it != iters[minIdx].end())
                            {
                                queue.emplace(*it, minIdx);
                            }

                            ++numNewProcessed;
                            if (numNewProcessed >= progressCallbackThreshold)
                            {
                                progress.onWorkDone(numNewProcessed);
                                numNewProcessed = 0;
                            }
                        }

                        // When merging with a priority_queue we don't erase immediately.
                        iters.erase(
                            std::remove_if(
                                iters.begin(), 
                                iters.end(), 
                                [](const auto& iter) { 
                                    return iter.begin() == iter.end(); 
                                }
                            ), 
                            iters.end()
                        );
                    }
                }

                ASSERT(iters.size() <= priorityQueueMergeThreshold);
                // Finish with the linear scan procedure for low number of files.
                {
                    // this will hold the current values for each iterator
                    std::vector<T> nextValues(iters.size());

                    // we ensure that removal leaves the elements in the same order
                    auto removeIter = [&iters, &nextValues](std::size_t i) {
                        iters.erase(std::begin(iters) + i);
                        nextValues.erase(std::begin(nextValues) + i);
                    };

                    // assign current values
                    for (std::size_t i = 0; i < iters.size(); ++i)
                    {
                        ASSERT(iters[i].begin() != iters[i].end());
                        nextValues[i] = *(iters[i].begin());
                    }

                    while (!iters.empty())
                    {
                        std::size_t minIdx = 0;

                        // do a search for minimum on contiguous memory
                        const std::size_t numIters = iters.size();
                        for (std::size_t i = 1; i < numIters; ++i)
                        {
                            if (cmp(nextValues[i], nextValues[minIdx]))
                            {
                                minIdx = i;
                            }
                        }

                        // write the minimum value
                        func(nextValues[minIdx]);

                        // update iterator
                        auto& it = ++iters[minIdx].begin();

                        // if it's at the end remove it
                        if (it == iters[minIdx].end())
                        {
                            removeIter(minIdx);
                        }
                        else
                        {
                            // else we update the next value for this iterator
                            nextValues[minIdx] = *it;
                        }

                        ++numNewProcessed;
                        if (numNewProcessed >= progressCallbackThreshold)
                        {
                            progress.onWorkDone(numNewProcessed);
                            numNewProcessed = 0;
                        }
                    }
                }
            }

            progress.onWorkDone(numNewProcessed);
        }


        // TODO: I think it would be better if the size of merge input and output
        //       were always constant and specified in the settings.
        //       In particular current implementation introduces big latency when
        //       merging a small amount of files.

        template <typename T, typename CompT>
        void merge_no_recurse(
            BinaryOutputFile& outFile,
            ContainerIterRange<std::vector<ImmutableSpan<T>>> in,
            CompT cmp,
            detail::ProgressTracker& progress
        )
        {
            using InputRange = ContainerIterRange<ImmutableSpan<T>>;

            const std::size_t numInputs = in.distance();

            ASSERT(numInputs <= maxBatchSize);

            const std::size_t inputBufferElements = inputBufferSize.elements<T>() / 2 + 1;

            const std::size_t outputBufferElements = outputBufferSize.elements<T>() / 2 + 1;;

            BackInserter<T> out(outFile, util::DoubleBuffer<T>(outputBufferElements));
            auto outFunc = [&out](const T& value) {
                out.emplace(value);
            };

            merge_for_each_no_recurse<T>(
                in,
                outFunc,
                cmp,
                progress
            );
        }

        template <typename T, typename CompT, typename FuncT>
        int merge_for_each_impl(
            const MergePlan& plan,
            MergePassFinishedCallback& passFinishedCallback,
            ContainerIterRange<std::vector<ImmutableSpan<T>>> in,
            FuncT&& func,
            CompT cmp,
            detail::ProgressTracker& progress
        )
        {
            ASSERT(plan.numPasses() > 0);

            std::size_t numInputs = in.distance();

            // This is only used to ensure the temporary files live long enough.
            // We have to ensure they are deleted after parts
            TemporaryPaths temporaryFilesRead{};

            // In the first iteration this is empty because parts
            // from `in` are used.
            // In the following iterations `in` is reassigned to point to this.
            // This is only used as storage, should not be accessed directly.
            std::vector<ImmutableSpan<T>> partsRead;

            int passId = 0;

            if (numInputs > maxBatchSize)
            {
                while (numInputs > maxBatchSize)
                {
                    std::vector<ImmutableSpan<T>> partsWrite;
                    partsWrite.reserve(numInputs / maxBatchSize + 1);

                    // Do one pass.
                    // In one pass we merge each group of at most maxNumMergedInputs files.
                    // The resulting files (paths) are managed by temporaryFilesWrite.
                    // Their content is represented in spans in partsWrite.
                    // After each pass we delete temporaryFilesRead.

                    // We create a new temporary file bank for writes
                    TemporaryPaths temporaryFilesWrite(plan.writeDirForPass(passId));


                    // Merge each group of at most maxNumMergedInputs
                    std::size_t offset = 0;
                    for (; offset + maxBatchSize < numInputs; offset += maxBatchSize)
                    {
                        BinaryOutputFile partOut(temporaryFilesWrite.next());
                        merge_no_recurse<T>(
                            partOut,
                            {
                                std::next(std::begin(in), offset),
                                std::next(std::begin(in), offset + maxBatchSize)
                            },
                            cmp,
                            progress
                            );

                        partsWrite.emplace_back(partOut.seal());
                    }

                    // We may have some input files left (less than a full group)
                    if (offset < numInputs)
                    {
                        // Sometimes this will be just one files but we want to copy it anyway.
                        // Maybe it's worth special-casing this.
                        BinaryOutputFile partOut(temporaryFilesWrite.next());
                        merge_no_recurse<T>(
                            partOut,
                            {
                                std::next(std::begin(in), offset),
                                std::end(in)
                            },
                            cmp,
                            progress
                            );

                        partsWrite.emplace_back(partOut.seal());
                    }

                    // Pass finished.
                    // We reassign temporary paths to a longer-lived objects.
                    // Old temporary read files are deleted here.
                    temporaryFilesRead = std::move(temporaryFilesWrite);

                    // We move the written parts to be next read from.
                    partsRead = std::move(partsWrite);
                    in = ContainerIterRange<std::vector<ImmutableSpan<T>>>(partsRead);
                    numInputs = in.distance();

                    // At this point all unneeded temporary files are deleted.
                    // We can make a callback to the user so he can delete
                    // the original files if he wants.
                    if (passFinishedCallback)
                    {
                        passFinishedCallback(passId);
                    }

                    ++passId;
                }
            }

            // We're left with numInputs <= maxNumMergedInputs.
            // temporaryFilesRead makes sure that if there are any
            // temp files they are still not deleted.
            // Now that the amount of files is small we can 
            merge_for_each_no_recurse<T>(
                in, 
                std::forward<FuncT>(func), 
                cmp, 
                progress
            );

            return passId;
        }

        template <typename T, typename CompT>
        int merge_impl2(
            const MergePlan& plan,
            MergePassFinishedCallback& passFinishedCallback,
            ContainerIterRange<std::vector<ImmutableSpan<T>>> in,
            const std::filesystem::path& outPath,
            CompT cmp,
            detail::ProgressTracker& progress
        )
        {
            using InputRange = ContainerIterRange<ImmutableSpan<T>>;

            const std::size_t outputBufferElements = outputBufferSize.elements<T>() / 2 + 1;

            BinaryOutputFile outFile(outPath);
            BackInserter<T> out(outFile, util::DoubleBuffer<T>(outputBufferElements));
            auto outFunc = [&out](const T& value) {
                out.emplace(value);
            };

            const int nextPassId = merge_for_each_impl<T>(
                plan,
                passFinishedCallback,
                in,
                outFunc,
                cmp,
                progress
            );

            passFinishedCallback(nextPassId);

            return nextPassId + 1;
        }
    }

    template <typename T>
    MergePlan make_merge_plan(
        const std::vector<ImmutableSpan<T>>& in,
        std::filesystem::path temp1,
        std::filesystem::path temp2
    )
    {
        MergePlan plan{};

        std::size_t numFiles = in.size();
        if (numFiles > 1)
        {
            std::size_t numPasses = 1;
            std::size_t numFilesCurrent = numFiles;
            while (numFilesCurrent > detail::merge::maxBatchSize)
            {
                numPasses += 1;
                numFilesCurrent /= detail::merge::maxBatchSize;
            }

            for (int i = 0; i < numPasses; ++i)
            {
                plan.passes.emplace_back(temp1, temp2);
                std::swap(temp1, temp2);
            }
        }

        return plan;
    }

    // Each group contains consecutive spans.
    // If there's only one span in the group then its size may
    // be larger than the limit. That's the only case when this happens.
    template <typename T>
    std::vector<std::vector<ImmutableSpan<T>>> groupConsecutiveSpans(
        std::vector<ImmutableSpan<T>>&& spans,
        MemoryAmount maxGroupSize
    )
    {
        const std::size_t maxGroupSizeBytes = maxGroupSize;

        std::vector<std::vector<ImmutableSpan<T>>> groups{};

        // set it as if the last group is full so we create a new one.
        std::size_t lastGroupSizeBytes = maxGroupSize.bytes() + 1;
        for (auto&& span : spans)
        {
            const std::size_t spanSizeBytes = span.size_bytes();
            if (lastGroupSizeBytes + spanSizeBytes > maxGroupSizeBytes)
            {
                groups.emplace_back();
                lastGroupSizeBytes = 0;
            }

            lastGroupSizeBytes += spanSizeBytes;
            groups.back().emplace_back(std::move(span));
        }

        return groups;
    }

    template <typename T>
    [[nodiscard]] std::size_t merge_assess_work(const std::vector<ImmutableSpan<T>>& in)
    {
        return detail::merge::merge_assess_work<T, std::vector<ImmutableSpan<T>>>(
            ContainerIterRange<std::vector<ImmutableSpan<T>>>(in)
            );
    }

    template <typename T, typename CompT = std::less<>>
    void merge(
        const MergePlan& plan,
        MergeCallbacks& callbacks,
        const std::vector<ImmutableSpan<T>>& in,
        const std::filesystem::path& outPath,
        CompT cmp = CompT{}
    )
    {
        auto progress = detail::ProgressTracker(callbacks.progressCallback);
        auto totalWork = merge_assess_work(in);
        progress.setTotalWork(totalWork);

        detail::merge::merge_impl2<T>(
            plan,
            callbacks.passFinishedCallback,
            ContainerIterRange<std::vector<ImmutableSpan<T>>>(in),
            outPath,
            cmp,
            progress
            );
    }

    template <typename T, typename FuncT, typename CompT = std::less<>>
    void merge_for_each(
        const MergePlan& plan,
        MergeCallbacks& callbacks,
        const std::vector<ImmutableSpan<T>>& in,
        FuncT&& func,
        CompT cmp = CompT{}
    )
    {
        auto progress = detail::ProgressTracker(callbacks.progressCallback);
        auto totalWork = merge_assess_work(in);
        progress.setTotalWork(totalWork);

        detail::merge::merge_for_each_impl<T>(
            plan,
            callbacks.passFinishedCallback,
            ContainerIterRange< std::vector<ImmutableSpan<T>>>(in),
            std::forward<FuncT>(func),
            cmp,
            progress
            );
    }

    template <typename RandomIterT, typename T = typename RandomIterT::value_type>
    struct IterValuePair
    {
        RandomIterT it;
        T value;
    };

    template <typename KeyType, typename CompareT>
    struct RangeIndexEntry
    {
        static_assert(std::is_empty_v<CompareT>);

        std::size_t low;
        std::size_t high;
        KeyType lowValue;
        KeyType highValue;

        template <typename KeyType, typename V>
        [[nodiscard]] friend bool operator<(const RangeIndexEntry<KeyType, CompareT>& lhs, const RangeIndexEntry<V, CompareT>& rhs) noexcept
        {
            return CompareT{}(lhs.highValue, rhs.lowValue);
        }

        template <typename KeyType, typename V>
        [[nodiscard]] friend bool operator<(const RangeIndexEntry<KeyType, CompareT>& lhs, const V& rhs) noexcept
        {
            return CompareT{}(lhs.highValue, rhs);
        }

        template <typename KeyType, typename V>
        [[nodiscard]] friend bool operator<(const KeyType& lhs, const RangeIndexEntry<V, CompareT>& rhs) noexcept
        {
            return CompareT{}(lhs, rhs.lowValue);
        }

        template <typename KeyType, typename V>
        [[nodiscard]] friend bool operator>(const RangeIndexEntry<KeyType, CompareT>& lhs, const RangeIndexEntry<V, CompareT>& rhs) noexcept
        {
            return CompareT{}(rhs.lowValue, lhs.highValue);
        }

        template <typename KeyType, typename V>
        [[nodiscard]] friend bool operator>(const RangeIndexEntry<KeyType, CompareT>& lhs, const V& rhs) noexcept
        {
            return CompareT{}(rhs, lhs.highValue);
        }

        template <typename KeyType, typename V>
        [[nodiscard]] friend bool operator>(const KeyType& lhs, const RangeIndexEntry<V, CompareT>& rhs) noexcept
        {
            return CompareT{}(rhs.lowValue, lhs);
        }
    };

    template <typename KeyType, typename CompareT>
    struct RangeIndex
    {
        static_assert(std::is_empty_v<CompareT>);

        using EntryType = RangeIndexEntry<KeyType, CompareT>;
        using IterValueType = IterValuePair<std::size_t, KeyType>;

        RangeIndex() = default;

        RangeIndex(std::vector<RangeIndexEntry<KeyType, CompareT>>&& entries) :
            m_entries(std::move(entries))
        {
        }

        [[nodiscard]] auto begin() const
        {
            return m_entries.cbegin();
        }

        [[nodiscard]] auto end() const
        {
            return m_entries.cend();
        }

        [[nodiscard]] auto cbegin() const
        {
            return m_entries.cbegin();
        }

        [[nodiscard]] auto cend() const
        {
            return m_entries.cend();
        }

        [[nodiscard]] const RangeIndexEntry<KeyType, CompareT>* data() const
        {
            return m_entries.data();
        }

        [[nodiscard]] std::size_t size() const
        {
            return m_entries.size();
        }

        // end is returned when there is no range with the given key
        [[nodiscard]] std::pair<IterValueType, IterValueType> equal_range(const KeyType& key) const
        {
            const auto end = m_entries.back().high + 1;

            auto cmp = CompareT{};

            // Find a range entry that contains keys[i] or, if there is none, get
            // the next range.
            auto [a, b] = std::equal_range(m_entries.begin(), m_entries.end(), key);

            KeyType lowValue{}, highValue{};
            std::size_t low = end;
            std::size_t high = end;

            if (b == m_entries.begin() || a == m_entries.end())
            {
                // All values are greater (or lower).
                // We keep the low and high pointing to end - this skips the search.
            }
            else
            {
                // a can equal b. It's perfectly fine.
                const auto& e0 = *a;
                const auto& e1 = *(b - 1);
                lowValue = e0.lowValue;
                highValue = e1.highValue;

                // If no range entry in the index contains the key then
                // the key doesn't exist in the data.
                // This check let's us remove unneeded reads later in the search algorithm.
                if (cmp(key, lowValue) || cmp(highValue, key))
                {
                    low = end;
                    high = end;
                }
                else
                {
                    low = e0.low;
                    high = e1.high + 1;
                }
            }

            return { { low, lowValue }, { high, highValue } };
        }

    private:
        std::vector<RangeIndexEntry<KeyType, CompareT>> m_entries;
    };

    namespace detail::equal_range
    {
        [[nodiscard]] std::pair<std::size_t, std::size_t> neighbourhood(
            std::size_t begin,
            std::size_t end,
            std::size_t mid,
            std::size_t size
        );

        struct Identity
        {
            template <typename T>
            [[nodiscard]] auto operator()(T v) const
            {
                return v;
            }
        };

        template <typename T, std::size_t I>
        struct Box : T { Box(T&& t) : T(std::forward<T>(t)) {} };

        template <typename ToArithmeticT = Identity, typename ToSizeTT = Identity>
        struct Interpolate : Box<ToArithmeticT, 0>, Box<ToSizeTT, 1>
        {
            Interpolate(ToArithmeticT&& f0 = ToArithmeticT{}, ToSizeTT&& f1 = ToSizeTT{}) :
                Box<ToArithmeticT, 0>(std::forward<ToArithmeticT>(f0)),
                Box<ToSizeTT, 1>(std::forward<ToSizeTT>(f1))
            {
            }

            template <typename T>
            [[nodiscard]] auto operator()(
                std::size_t low,
                std::size_t high,
                const T& lowValue,
                const T& highValue,
                const T& key
                ) const
            {
                ASSERT(low != high);

                const auto b_lowValue = Box<ToArithmeticT, 0>::operator()(lowValue);
                const auto b_highValue = Box<ToArithmeticT, 0>::operator()(highValue);
                const auto b_key = Box<ToArithmeticT, 0>::operator()(key);
                const auto b_s = static_cast<decltype(b_key)>(high - low - 1u);
                const auto d = b_lowValue < b_highValue ?
                    Box<ToSizeTT, 1>::operator()((b_key - b_lowValue) * b_s / (b_highValue - b_lowValue))
                    : Box<ToSizeTT, 1>::operator()((b_lowValue - b_key) * b_s / (b_lowValue - b_highValue));
                const auto mid = low + d;
                return mid;
            }
        };

        template <typename ToArithmeticT, typename ToSizeT>
        auto makeInterpolator(ToArithmeticT&& a, ToSizeT&& b)
        {
            return detail::equal_range::Interpolate(
                std::forward<ToArithmeticT>(a),
                std::forward<ToSizeT>(b)
            );
        }

        struct Binary
        {
            template <typename T>
            [[nodiscard]] auto operator()(
                std::size_t low,
                std::size_t high,
                const T& lowValue,
                const T& highValue,
                const T& key
                ) const
            {
                ASSERT(low != high);

                const auto d = (high - low) / 2u;
                const auto mid = low + d;
                return mid;
            }
        };

        struct DoCrossUpdates {};
        struct NoCrossUpdates {};

        template <typename KeyType>
        using Range = std::vector<std::pair<IterValuePair<std::size_t, KeyType>, IterValuePair<std::size_t, KeyType>>>;

        extern const MemoryAmount maxSeqReadSize;

        // EntryType must be convertible to KeyType - values of type KeyType are stored for further reference
        template <
            typename CrossT,
            typename EntryType,
            typename KeyType,
            typename CompareT = std::less<>,
            typename KeyExtractorT = Identity,
            typename MiddleT = Interpolate<>
        >
            [[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>> equal_range_multiple_impl(
                CrossT,
                const ImmutableSpan<EntryType>& data,
                Range<KeyType>&& iters,
                const std::vector<KeyType>& keys,
                CompareT cmp = CompareT{},
                KeyExtractorT extractKey = KeyExtractorT{},
                MiddleT middle = MiddleT{}
            )
        {
            // 32KiB should be about how much we can read using 'constant' time.
            const std::size_t maxNumSeqReadElements = std::max(static_cast<std::size_t>(3), maxSeqReadSize.bytes() / sizeof(EntryType));
            ASSERT(maxNumSeqReadElements >= 3);

            const std::size_t end = data.size();

            util::Buffer<EntryType> buffer(maxNumSeqReadElements);

            auto readToBuffer = [&](std::size_t begin, std::size_t end) {
                ASSERT(begin != end);
                ASSERT(end - begin <= maxNumSeqReadElements);

                (void)data.read(buffer.data(), begin, end - begin);

                return end - begin;
            };

            std::vector<std::uint8_t> isCompleted(keys.size(), false);
            std::vector<std::pair<std::size_t, std::size_t>> results;
            results.reserve(keys.size());

            for (std::size_t i = 0; i < keys.size(); ++i)
            {
                auto crossUpdateRange = [&](std::size_t a, std::size_t b, const EntryType* values)
                {
                    if constexpr (std::is_same_v<CrossT, DoCrossUpdates>)
                    {
                        // Try to use values already present in memory to narrow the search
                        // for ranges that are not being actively searched right now.
                        const std::size_t count = b - a;
                        ASSERT(count > 0);

                        for (std::size_t j = i + 1; j < keys.size(); ++j)
                        {
                            if (isCompleted[j]) continue;

                            const auto& key = keys[j];
                            auto& [aa, bb] = iters[j];
                            auto& [low, lowValue] = aa;
                            auto& [high, highValue] = bb;

                            const auto [lbx, ubx] = std::equal_range(values, values + count, key, cmp);
                            const std::size_t lb = a + static_cast<std::size_t>(std::distance(values, lbx));
                            const std::size_t ub = a + static_cast<std::size_t>(std::distance(values, ubx));

                            if (lb != a && ub != b)
                            {
                                if (lb == ub)
                                {
                                    // the range is empty
                                    low = end;
                                    high = end;
                                    isCompleted[j] = true;
                                    continue;
                                }
                                else
                                {
                                    // we found the whole range
                                    low = lb;
                                    high = ub;
                                    isCompleted[j] = true;
                                    continue;
                                }
                            }

                            if (lb != a || ub != b)
                            {
                                // try to narrow from sides
                                // exactly one if will be executed as
                                // the case when we can narrow from both sides
                                // was handled above
                                // NOTE: we can safely access *lbx and *(ubx - 1) because we have sentinels
                                if (lb != a && lb > low)
                                {
                                    low = lb;
                                    lowValue = extractKey(*lbx);
                                }
                                if (ub != b && ub < high)
                                {
                                    high = ub;
                                    highValue = extractKey(*(ubx - 1));
                                }
                            }
                        }
                    }
                };

                auto accessAndCrossUpdateRange = [&](std::size_t low, std::size_t high)
                {
                    ASSERT(low != high);

                    const std::size_t count = readToBuffer(low, high);
                    if (count > 2)
                    {
                        // updating needs access to one before and one after the buffer
                        crossUpdateRange(low + 1, high - 1, buffer.data() + 1);
                    }
                    return std::make_pair(buffer.data(), count);
                };

                auto accessAndCrossUpdateRangeWithSentinels = [&](std::size_t low, std::size_t high, std::size_t numSentinels)
                {
                    const std::size_t count = readToBuffer(low, high);

                    ASSERT(count > numSentinels * 2);

                    // updating needs access to one before and one after the buffer
                    crossUpdateRange(low + 1, high - 1, buffer.data() + 1);
                    return std::make_pair(buffer.data() + numSentinels, count - numSentinels * 2);
                };

                auto access = [&](std::size_t it)
                {
                    return data[it];
                };

                const auto& [a, b] = iters[i];
                std::size_t low = a.it;
                std::size_t high = b.it;

                // handle a situation that can arise from cross updates
                // or we just have an empty range - in that case lowValue and highValue are placeholders
                if (low == high || isCompleted[i])
                {
                    isCompleted[i] = true;
                    results.emplace_back(low, high);
                    continue;
                }

                KeyType lowValue = a.value;
                KeyType highValue = b.value;
                const KeyType& key = keys[i];

                while (cmp(lowValue, highValue) && !cmp(key, lowValue) && !cmp(highValue, key))
                {
                    ASSERT(low < high);

                    {
                        // If we can load the whole range into memory then do that.
                        const std::size_t count = high - low;
                        if (count <= maxNumSeqReadElements)
                        {
                            const auto [buf, count] = accessAndCrossUpdateRange(low, high);

                            const auto [lbx, ubx] = std::equal_range(buf, buf + count, key, cmp);
                            const std::size_t lb = low + static_cast<std::size_t>(std::distance(buf, lbx));
                            const std::size_t ub = low + static_cast<std::size_t>(std::distance(buf, ubx));

                            isCompleted[i] = true;
                            results.emplace_back(lb, ub);

                            break;
                        }
                    }

                    std::size_t mid = middle(low, high, lowValue, highValue, key);

                    ASSERT(mid >= low);
                    ASSERT(mid < high);

                    {
                        // this procedure is similar to crossUpdateRange but here
                        // we can actually push the result already
                        // and we also have to know whether to continue or not

                        auto [a, b] = neighbourhood(low, high, mid, maxNumSeqReadElements);

                        // We always have more than 2 values because we always speculativly search through 3
                        const auto [buf, count] = accessAndCrossUpdateRangeWithSentinels(a, b, 1);
                        a += 1;
                        b -= 1;

                        const auto [lbx, ubx] = std::equal_range(buf, buf + count, key, cmp);
                        const std::size_t lb = a + static_cast<std::size_t>(std::distance(buf, lbx));
                        const std::size_t ub = a + static_cast<std::size_t>(std::distance(buf, ubx));

                        if (lb != a && ub != b)
                        {
                            // if we're in the middle
                            if (lb == ub)
                            {
                                isCompleted[i] = true;
                                results.emplace_back(end, end);
                                break;
                            }
                            else
                            {
                                // we found the range
                                isCompleted[i] = true;
                                results.emplace_back(lb, ub);
                                break;
                            }
                        }

                        if (lb != a || ub != b)
                        {
                            // if we can narrow search from at least one side
                            // NOTE: we can safely access *lbx and *(ubx - 1) because we have sentinels
                            if (lb != a)
                            {
                                low = lb;
                                lowValue = extractKey(*lbx);
                            }
                            if (ub != b)
                            {
                                high = ub;
                                highValue = extractKey(*(ubx - 1));
                            }
                            continue;
                        }

                        // ASSERT(extractKey(buf[mid - a]) == key);
                    }

                    // *mid == key here
                    // we increase mid to point to after the element equal to key
                    // this is so if *mid is the last value equal to key we
                    // can still properly find the lower bound
                    ++mid;

                    // if we cannot narrow the search from any side that means
                    // that mid contains the key value and it spans
                    // too far to reach with the buffer size
                    {
                        // Find the lower bound by increasing the search window starting from mid.
                        std::size_t count = mid - low;

                        {
                            std::size_t rc = maxNumSeqReadElements;
                            auto last = mid;
                            while (rc < count)
                            {
                                if (cmp(extractKey(access(mid - rc)), key))
                                {
                                    low = mid - rc;
                                    mid = last;
                                    count = mid - low;
                                    break;
                                }
                                last = mid - rc;
                                rc *= 2;
                            }
                        }

                        // Move low to point to lower_bound
                        while (count > 0)
                        {
                            if (count <= maxNumSeqReadElements)
                            {
                                const auto [buf, _] = accessAndCrossUpdateRange(low, low + count);

                                ASSERT(count == _);

                                const auto lbx = std::lower_bound(buf, buf + count, key, cmp);
                                low += static_cast<std::size_t>(std::distance(buf, lbx));

                                break;
                            }
                            else
                            {
                                auto it = low;
                                const std::size_t step = count / 2;
                                it += step;
                                if (cmp(extractKey(access(it)), key))
                                {
                                    low = ++it;
                                    count -= step + 1;
                                }
                                else
                                {
                                    count = step;
                                }
                            }
                        }
                    }

                    {
                        // Find the upper bound by increasing the search window starting from mid.
                        std::size_t count = high - mid;

                        {
                            std::size_t rc = maxNumSeqReadElements;
                            auto last = mid;
                            while (rc < count)
                            {
                                if (cmp(key, extractKey(access(mid + rc))))
                                {
                                    high = mid + rc;
                                    mid = last;
                                    count = high - mid;
                                    break;
                                }
                                last = mid + rc;
                                rc *= 2;
                            }
                        }

                        // Move mid to point to upper_bound
                        while (count > 0)
                        {
                            if (count <= maxNumSeqReadElements)
                            {
                                const auto [buf, _] = accessAndCrossUpdateRange(mid, mid + count);

                                ASSERT(count == _);

                                const auto ubx = std::upper_bound(buf, buf + count, key, cmp);
                                mid += static_cast<std::size_t>(std::distance(buf, ubx));

                                break;
                            }
                            else
                            {
                                auto it = mid;
                                const std::size_t step = count / 2;
                                it += step;
                                if (!cmp(key, extractKey(access(it)))) {
                                    mid = ++it;
                                    count -= step + 1;
                                }
                                else
                                {
                                    count = step;
                                }
                            }
                        }
                    }

                    ASSERT(low < mid); // There should be at least one value

                    isCompleted[i] = true;
                    results.emplace_back(low, mid);
                    break;
                }

                if (!isCompleted[i])
                {
                    ASSERT(low != high);

                    if (!cmp(key, lowValue) && !cmp(lowValue, key))
                    {
                        results.emplace_back(low, high);
                    }
                    else
                    {
                        results.emplace_back(end, end);
                    }
                }
            }

            return results;
        }

        template <
            typename CrossT,
            typename EntryType,
            typename KeyType,
            typename CompareT = std::less<>,
            typename KeyExtractorT = Identity,
            typename MiddleT = Interpolate<>
        >
            [[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>> equal_range_multiple_indexed_impl(
                CrossT,
                const ImmutableSpan<EntryType>& data,
                const RangeIndex<KeyType, CompareT>& index,
                const std::vector<KeyType>& keys,
                CompareT cmp = CompareT{},
                KeyExtractorT extractKey = KeyExtractorT{},
                MiddleT middle = MiddleT{}
            )
        {
            std::size_t begin = 0;
            std::size_t end = data.size();

            if (begin == end) return std::vector<std::pair<std::size_t, std::size_t>>(keys.size(), { end, end });

            Range<KeyType> ranges;
            ranges.reserve(keys.size());
            for (int i = 0; i < keys.size(); ++i)
            {
                ranges.emplace_back(index.equal_range(keys[i]));
            }

            return equal_range_multiple_impl(
                CrossT{},
                data,
                std::move(ranges),
                keys,
                std::move(cmp),
                std::move(extractKey),
                std::move(middle)
            );
        }

        template <
            typename CrossT,
            typename EntryType,
            typename KeyType,
            typename CompareT = std::less<>,
            typename KeyExtractorT = Identity,
            typename MiddleT = Interpolate<>
        >
            [[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>> equal_range_multiple_impl(
                CrossT,
                const ImmutableSpan<EntryType>& data,
                const std::vector<KeyType>& keys,
                CompareT cmp = CompareT{},
                KeyExtractorT extractKey = KeyExtractorT{},
                MiddleT middle = MiddleT{}
            )
        {
            std::size_t begin = 0;
            std::size_t end = data.size();

            if (begin == end) return std::vector<std::pair<std::size_t, std::size_t>>(keys.size(), { end, end });

            Range<KeyType> ranges;
            ranges.reserve(keys.size());
            const KeyType lowValue = extractKey(data[begin]);
            const KeyType highValue = (end - begin == 1) ? lowValue : extractKey(data[end - 1u]);
            for (int i = 0; i < keys.size(); ++i)
            {
                IterValuePair<std::size_t, KeyType> aa{ begin, lowValue };
                IterValuePair<std::size_t, KeyType> bb{ end, highValue };
                ranges.emplace_back(aa, bb);
            }

            return equal_range_multiple_impl(
                CrossT{},
                data,
                std::move(ranges),
                keys,
                std::move(cmp),
                std::move(extractKey),
                std::move(middle)
            );
        }
    }

    template <
        typename EntryType,
        typename KeyType,
        typename CompareT = std::less<>,
        typename KeyExtractorT = detail::equal_range::Identity,
        typename ToArithmeticT = detail::equal_range::Identity,
        typename ToSizeTT = detail::equal_range::Identity
    >
        [[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>> equal_range_multiple_interp(
            const ImmutableSpan<EntryType>& data,
            const std::vector<KeyType>& keys,
            CompareT cmp = CompareT{},
            KeyExtractorT extractKey = KeyExtractorT{},
            ToArithmeticT toArithmetic = ToArithmeticT{},
            ToSizeTT toSizeT = ToSizeTT{}
        )
    {
        return detail::equal_range::equal_range_multiple_impl(
            detail::equal_range::NoCrossUpdates{},
            data,
            keys,
            std::move(cmp),
            std::move(extractKey),
            detail::equal_range::makeInterpolator(
                std::move(toArithmetic),
                std::move(toSizeT)
            )
        );
    }

    template <
        typename EntryType,
        typename KeyType,
        typename CompareT = std::less<>,
        typename KeyExtractorT = detail::equal_range::Identity,
        typename ToArithmeticT = detail::equal_range::Identity,
        typename ToSizeTT = detail::equal_range::Identity
    >
        [[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>> equal_range_multiple_interp_cross(
            const ImmutableSpan<EntryType>& data,
            const std::vector<KeyType>& keys,
            CompareT cmp = CompareT{},
            KeyExtractorT extractKey = KeyExtractorT{},
            ToArithmeticT toArithmetic = ToArithmeticT{},
            ToSizeTT toSizeT = ToSizeTT{}
        )
    {
        return detail::equal_range::equal_range_multiple_impl(
            detail::equal_range::DoCrossUpdates{},
            data,
            keys,
            std::move(cmp),
            std::move(extractKey),
            detail::equal_range::makeInterpolator(
                std::move(toArithmetic),
                std::move(toSizeT)
            )
        );
    }

    template <
        typename EntryType,
        typename KeyType,
        typename CompareT = std::less<>,
        typename KeyExtractorT = detail::equal_range::Identity,
        typename ToArithmeticT = detail::equal_range::Identity,
        typename ToSizeTT = detail::equal_range::Identity
    >
        [[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>> equal_range_multiple_interp_indexed(
            const ImmutableSpan<EntryType>& data,
            const RangeIndex<KeyType, CompareT>& index,
            const std::vector<KeyType>& keys,
            CompareT cmp = CompareT{},
            KeyExtractorT extractKey = KeyExtractorT{},
            ToArithmeticT toArithmetic = ToArithmeticT{},
            ToSizeTT toSizeT = ToSizeTT{}
        )
    {
        return detail::equal_range::equal_range_multiple_indexed_impl(
            detail::equal_range::NoCrossUpdates{},
            data,
            index,
            keys,
            std::move(cmp),
            std::move(extractKey),
            detail::equal_range::makeInterpolator(
                std::move(toArithmetic),
                std::move(toSizeT)
            )
        );
    }

    template <
        typename EntryType,
        typename KeyType,
        typename CompareT = std::less<>,
        typename KeyExtractorT = detail::equal_range::Identity,
        typename ToArithmeticT = detail::equal_range::Identity,
        typename ToSizeTT = detail::equal_range::Identity
    >
        [[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>> equal_range_multiple_interp_indexed_cross(
            const ImmutableSpan<EntryType>& data,
            const RangeIndex<KeyType, CompareT>& index,
            const std::vector<KeyType>& keys,
            CompareT cmp = CompareT{},
            KeyExtractorT extractKey = KeyExtractorT{},
            ToArithmeticT toArithmetic = ToArithmeticT{},
            ToSizeTT toSizeT = ToSizeTT{}
        )
    {
        return detail::equal_range::equal_range_multiple_indexed_impl(
            detail::equal_range::DoCrossUpdates{},
            data,
            index,
            keys,
            std::move(cmp),
            std::move(extractKey),
            detail::equal_range::makeInterpolator(
                std::move(toArithmetic),
                std::move(toSizeT)
            )
        );
    }

    template <
        typename EntryType,
        typename KeyType,
        typename CompareT = std::less<>,
        typename KeyExtractorT = detail::equal_range::Identity
    >
        [[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>> equal_range_multiple_bin(
            const ImmutableSpan<EntryType>& data,
            const std::vector<KeyType>& keys,
            CompareT cmp = CompareT{},
            KeyExtractorT extractKey = KeyExtractorT{}
        )
    {
        return detail::equal_range::equal_range_multiple_impl(
            detail::equal_range::NoCrossUpdates{},
            data,
            keys,
            std::move(cmp),
            std::move(extractKey),
            detail::equal_range::Binary{}
        );
    }

    template <
        typename EntryType,
        typename KeyType,
        typename CompareT = std::less<>,
        typename KeyExtractorT = detail::equal_range::Identity
    >
        [[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>> equal_range_multiple_bin_cross(
            const ImmutableSpan<EntryType>& data,
            const std::vector<KeyType>& keys,
            CompareT cmp = CompareT{},
            KeyExtractorT extractKey = KeyExtractorT{}
        )
    {
        return detail::equal_range::equal_range_multiple_impl(
            detail::equal_range::DoCrossUpdates{},
            data,
            keys,
            std::move(cmp),
            std::move(extractKey),
            detail::equal_range::Binary{}
        );
    }

    template <
        typename EntryType,
        typename KeyType,
        typename CompareT = std::less<>,
        typename KeyExtractorT = detail::equal_range::Identity
    >
        [[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>> equal_range_multiple_bin_indexed(
            const ImmutableSpan<EntryType>& data,
            const RangeIndex<KeyType, CompareT>& index,
            const std::vector<KeyType>& keys,
            CompareT cmp = CompareT{},
            KeyExtractorT extractKey = KeyExtractorT{}
        )
    {
        return detail::equal_range::equal_range_multiple_indexed_impl(
            detail::equal_range::NoCrossUpdates{},
            data,
            index,
            keys,
            std::move(cmp),
            std::move(extractKey),
            detail::equal_range::Binary{}
        );
    }

    template <
        typename EntryType,
        typename KeyType,
        typename CompareT = std::less<>,
        typename KeyExtractorT = detail::equal_range::Identity
    >
        [[nodiscard]] std::vector<std::pair<std::size_t, std::size_t>> equal_range_multiple_bin_indexed_cross(
            const ImmutableSpan<EntryType>& data,
            const RangeIndex<KeyType, CompareT>& index,
            const std::vector<KeyType>& keys,
            CompareT cmp = CompareT{},
            KeyExtractorT extractKey = KeyExtractorT{}
        )
    {
        return detail::equal_range::equal_range_multiple_indexed_impl(
            detail::equal_range::DoCrossUpdates{},
            data,
            index,
            keys,
            std::move(cmp),
            std::move(extractKey),
            detail::equal_range::Binary{}
        );
    }

    namespace detail
    {
        template <
            typename BeginT,
            typename EndT,
            typename CompareT = std::less<>,
            typename KeyExtractT = detail::equal_range::Identity
        >
            auto makeIndexImpl(
                BeginT iter,
                EndT end,
                std::size_t maxNumEntriesInRange,
                CompareT cmp = CompareT{},
                KeyExtractT key = KeyExtractT{}
            )
        {
            using EntryType = typename BeginT::value_type;
            using KeyType = decltype(key(std::declval<EntryType>()));

            ASSERT(iter != end);

            std::vector<RangeIndexEntry<KeyType, CompareT>> iters;

            EntryType startValue = *iter;
            EntryType endValue = startValue;
            EntryType firstOfNextRange = startValue;
            EntryType prevValue = startValue;
            std::size_t startIdx = 0;
            std::size_t firstOfNextRangeIdx = 0;
            std::size_t offset = 0;
            while (iter != end)
            {
                prevValue = *iter;
                ++iter;
                ++offset;

                // Go through the largest span of equal values
                while (iter != end)
                {
                    if (cmp(prevValue, *iter))
                    {
                        break;
                    }

                    prevValue = *iter;
                    ++offset;
                    ++iter;
                }

                // We either reached an end in which case we create
                // the last range spanning up to the end.
                // Or we hit the first value different than the startValue

                while (iter != end)
                {
                    // If the value changes then update the
                    // range divisor position and respective values.
                    // We do it even when the already selected span is longer
                    // than maxNumEntriesInRange.
                    if (cmp(prevValue, *iter))
                    {
                        firstOfNextRange = *iter;
                        firstOfNextRangeIdx = startIdx + offset;
                        endValue = prevValue;
                    }

                    // Either maxNumEntriesInRange long or as long as
                    // the equal value span from the first while loop.
                    if (offset >= maxNumEntriesInRange)
                    {
                        break;
                    }

                    prevValue = *iter;
                    ++offset;
                    ++iter;
                }

                RangeIndexEntry<KeyType, CompareT> e;
                if (iter == end)
                {
                    // startValue is the lowValue of the range
                    // prevValue is the highValue of the range
                    // startIdx is low
                    // startIdx + offset - 1 is high

                    e.low = startIdx;
                    e.lowValue = key(startValue);
                    e.high = startIdx + offset - 1u;
                    e.highValue = key(prevValue);
                }
                else
                {
                    // startValue is the lowValue of the range
                    // endValue is the highValue of the range
                    // startIdx is low
                    // firstOfNextRangeIdx - 1 is high
                    // firstOfNextRange is the lowValue of the next range
                    // firstOfNextRangeIdx is the startIdx of the next range

                    e.low = startIdx;
                    e.lowValue = key(startValue);
                    e.high = firstOfNextRangeIdx - 1u;
                    e.highValue = key(endValue);

                    // "How many already read values go to the next range"
                    offset -= firstOfNextRangeIdx - startIdx;

                    startIdx = firstOfNextRangeIdx;
                    startValue = firstOfNextRange;
                }
                iters.emplace_back(e);
            }

            return RangeIndex<KeyType, CompareT>(std::move(iters));
        }
    }

    // TODO: sequential make index (from a file)
    // TODO: sequential incremental index maker
    // Each index entry range starts with the first unique value.
    // Ie. at most only one range contains any given value.
    template <
        typename EntryType,
        typename CompareT = std::less<>,
        typename KeyExtractT = detail::equal_range::Identity
    >
        auto makeIndex(
            const std::vector<EntryType> & values,
            std::size_t maxNumEntriesInRange,
            CompareT cmp = CompareT{},
            KeyExtractT key = KeyExtractT{}
        )
    {
        ASSERT(values.size() > 0);

        return detail::makeIndexImpl(values.begin(), values.end(), maxNumEntriesInRange, cmp, key);
    }

    extern const MemoryAmount defaultIndexBuilderMemoryAmount;

    template <
        typename EntryType,
        typename CompareT = std::less<>,
        typename KeyExtractT = detail::equal_range::Identity
    >
        auto makeIndex(
            const ImmutableSpan<EntryType> & values,
            std::size_t maxNumEntriesInRange, // the actual number of entries covered by a range can be bigger
                                              // but only when lowValue == highValue
            CompareT cmp = CompareT{},
            KeyExtractT key = KeyExtractT{},
            util::DoubleBuffer<EntryType> buffer = util::DoubleBuffer<EntryType>(
                defaultIndexBuilderMemoryAmount / sizeof(EntryType)
                )
        )
    {
        ASSERT(values.size() > 0);

        return detail::makeIndexImpl(values.begin(std::move(buffer)), values.end(), maxNumEntriesInRange, cmp, key);
    }

    template <
        typename EntryType,
        typename CompareT = std::less<>,
        typename KeyExtractT = detail::equal_range::Identity
    >
        struct IndexBuilder : KeyExtractT
    {
        static_assert(std::is_empty_v<CompareT>);

        using KeyType = decltype(std::declval<KeyExtractT>()(std::declval<EntryType>()));
        using IndexType = RangeIndex<KeyType, CompareT>;

        IndexBuilder(std::size_t maxNumEntriesInRange, CompareT cmp = CompareT{}, KeyExtractT key = KeyExtractT{}) :
            KeyExtractT(key),
            m_ranges{},
            m_startValue{},
            m_endValue{},
            m_firstOfNextRange{},
            m_prevValue{},
            m_maxNumEntriesInRange(maxNumEntriesInRange),
            m_startIdx{},
            m_firstOfNextRangeIdx{},
            m_offset{ 0 },
            m_state(0)
        {
        }

        void append(const EntryType* begin, std::size_t count)
        {
            if (count == 0)
            {
                return;
            }

            appendImpl(begin, count);
        }

        void append(const EntryType value)
        {
            appendImpl(&value, 1);
        }

        IndexType end()
        {
            if (m_offset != 0)
            {
                RangeIndexEntry<KeyType, CompareT> e;

                e.low = m_startIdx;
                e.lowValue = KeyExtractT::operator()(m_startValue);
                e.high = m_startIdx + m_offset - 1u;
                e.highValue = KeyExtractT::operator()(m_prevValue);

                m_ranges.emplace_back(e);
            }

            return IndexType(std::move(m_ranges));
        }

    private:
        std::vector<RangeIndexEntry<KeyType, CompareT>> m_ranges;
        EntryType m_startValue;
        EntryType m_endValue;
        EntryType m_firstOfNextRange;
        EntryType m_prevValue;
        std::size_t m_maxNumEntriesInRange;
        std::size_t m_startIdx;
        std::size_t m_firstOfNextRangeIdx;
        std::size_t m_offset;
        std::size_t m_state;

        void appendImpl(const EntryType* begin, std::size_t count)
        {
            const EntryType* end = begin + count;

            while (begin != end)
            {
                switch (m_state)
                {
                case 0:
                    m_startValue = *begin;
                    m_endValue = m_startValue;
                    m_firstOfNextRange = m_startValue;
                    m_prevValue = m_startValue;
                    m_startIdx = 0;
                    m_firstOfNextRangeIdx = 0;
                    m_state = 1;
                    m_offset = 0;

                case 1:
                    m_prevValue = *begin;
                    ++begin;
                    ++m_offset;
                    m_state = 2;

                case 2:
                    // Go through the largest span of equal values
                    while (begin != end)
                    {
                        if (CompareT{}(m_prevValue, *begin))
                        {
                            break;
                        }

                        m_prevValue = *begin;
                        ++m_offset;
                        ++begin;
                    }
                    if (begin == end)
                    {
                        return;
                    }

                    m_state = 3;

                case 3:

                    // We either reached an end in which case we create
                    // the last range spanning up to the end.
                    // Or we hit the first value different than the startValue

                    while (begin != end)
                    {
                        // If the value changes then update the
                        // range divisor position and respective values.
                        // We do it even when the already selected span is longer
                        // than maxNumEntriesInRange.
                        if (CompareT{}(m_prevValue, *begin))
                        {
                            m_firstOfNextRange = *begin;
                            m_firstOfNextRangeIdx = m_startIdx + m_offset;
                            m_endValue = m_prevValue;
                        }

                        // Either maxNumEntriesInRange long or as long as
                        // the equal value span from the first while loop.
                        if (m_offset >= m_maxNumEntriesInRange)
                        {
                            break;
                        }

                        m_prevValue = *begin;
                        ++m_offset;
                        ++begin;
                    }
                    if (begin == end)
                    {
                        return;
                    }

                    m_state = 4;

                case 4:
                    RangeIndexEntry<KeyType, CompareT> e;

                    // startValue is the lowValue of the range
                    // endValue is the highValue of the range
                    // startIdx is low
                    // firstOfNextRangeIdx - 1 is high
                    // firstOfNextRange is the lowValue of the next range
                    // firstOfNextRangeIdx is the startIdx of the next range

                    e.low = m_startIdx;
                    e.lowValue = KeyExtractT::operator()(m_startValue);
                    e.high = m_firstOfNextRangeIdx - 1u;
                    e.highValue = KeyExtractT::operator()(m_endValue);

                    // "How many already read values go to the next range"
                    m_offset -= m_firstOfNextRangeIdx - m_startIdx;

                    m_startIdx = m_firstOfNextRangeIdx;
                    m_startValue = m_firstOfNextRange;

                    m_ranges.emplace_back(e);

                    m_state = 1;
                }
            }
        }
    };
}
