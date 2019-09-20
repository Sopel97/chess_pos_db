#pragma once

#define _CRT_SECURE_NO_WARNINGS

#include "Assert.h"

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <iterator>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

namespace ext
{
    // TODO: concurrency support

    struct Exception : public std::runtime_error
    {
        using BaseType = std::runtime_error;

        using BaseType::BaseType;
    };

    [[nodiscard]] std::filesystem::path uniquePath()
    {
        constexpr int length = 16;

        static const char allowedChars[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

        static const std::uniform_int_distribution<unsigned short> dChar(0, sizeof(allowedChars) - 2u);

        static thread_local std::minstd_rand rng;

        std::string s;
        for (int i = 0; i < length; ++i)
        {
            s += allowedChars[dChar(rng)];
        }

        return s;
    }

    [[nodiscard]] std::filesystem::path uniquePath(const std::filesystem::path& dir)
    {
        return dir / uniquePath();
    }

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
        void operator()(std::FILE* ptr) const noexcept
        {
            std::fclose(ptr);
        }
    };

    namespace detail
    {
        using FileHandle = std::unique_ptr<std::FILE, FileDeleter>;

        [[nodiscard]] auto openFile(const std::filesystem::path& path, const std::string& openmode)
        {
            std::string pathStr = path.string();
            auto h = FileHandle(std::fopen(pathStr.c_str(), openmode.c_str()));
            if (h == nullptr)
            {
                throw Exception("Cannot open file.");
            }
            return h;
        }

        // NOTE: Files are pooled - they are closed and reopened when needed -
        //       therefore it is possible that a file
        //       is deleted while it is seemingly held locked.
        //       If that happens the behaviour is undefined.
        struct File
        {
            using NativeFileHandle = std::FILE*;

        private:

            using FilePoolEntry = std::pair<FileHandle, File*>;
            using FilePoolEntries = std::list<std::pair<FileHandle, File*>>;
            using FilePoolEntryIter = typename FilePoolEntries::iterator;

            template <typename FuncT>
            decltype(auto) withHandle(FuncT&& func)
            {
                return pool().withHandle(*this, std::forward<FuncT>(func));
            }

            struct FilePool
            {
                static constexpr std::size_t numMaxConcurrentOpenFiles = 256;

                FilePool() = default;

                FilePool(const FilePool&) = delete;
                FilePool(FilePool&&) = delete;
                FilePool& operator=(const FilePool&) = delete;
                FilePool& operator=(FilePool&&) = delete;

                template <typename FuncT>
                decltype(auto) withHandle(File& file, FuncT&& func)
                {
                    return std::forward<FuncT>(func)(getHandle(file));
                }

                [[nodiscard]] FilePoolEntryIter noneEntry()
                {
                    return m_files.end();
                }

                void close(File& file)
                {
                    if (file.m_poolEntry != noneEntry())
                    {
                        m_files.erase(file.m_poolEntry);
                        file.m_poolEntry = noneEntry();
                    }
                }

            private:
                FilePoolEntries m_files;

                [[nodiscard]] FileHandle reopen(File& file)
                {
                    ASSERT(file.m_timesOpened > 0u);

                    // in particular change 'w' to 'a' so we don't truncate the file
                    std::string openmode = file.m_openmode;
                    for (auto& c : openmode)
                    {
                        if (c == 'w') c = 'a';
                    }
                    return openFile(file.m_path, openmode);
                }

                [[nodiscard]] FileHandle open(File& file)
                {
                    ASSERT(file.m_timesOpened == 0u);

                    return openFile(file.m_path, file.m_openmode);
                }

                void closeLastFile()
                {
                    ASSERT(!m_files.empty());

                    File& file = *(m_files.front().second);
                    file.m_poolEntry = noneEntry();
                    m_files.pop_front();
                }

                [[nodiscard]] NativeFileHandle getHandle(File& file)
                {
                    if (file.m_poolEntry != noneEntry())
                    {
                        ASSERT(file.m_timesOpened > 0);
                        // the file is already open
                        // we only have to move it to the back
                        updateEntry(file.m_poolEntry);
                        return file.m_poolEntry->first.get();
                    }

                    // we have to open the file

                    // if the pool is full then make some space
                    if (m_files.size() == numMaxConcurrentOpenFiles)
                    {
                        closeLastFile();
                    }

                    // if we open for the first time we may want to truncate, we don't want that later
                    FileHandle handle{};
                    if (file.m_timesOpened > 0u)
                    {
                        handle = reopen(file);
                    }
                    else
                    {
                        handle = open(file);
                    }

                    m_files.emplace_back(std::move(handle), &file);
                    file.m_poolEntry = std::prev(m_files.end());
                    file.m_timesOpened += 1;
                    return file.m_poolEntry->first.get();
                }

                void updateEntry(const FilePoolEntryIter& it)
                {
                    m_files.splice(m_files.end(), m_files, it);
                }
            };

            static FilePool& pool()
            {
                static FilePool s_pool;
                return s_pool;
            }

        public:

            File(std::filesystem::path path, std::string openmode) :
                m_path(std::move(path)),
                m_openmode(std::move(openmode)),
                m_poolEntry(pool().noneEntry()),
                m_timesOpened(0)
            {
            }

            File(const File&) = delete;
            File(File&&) = default;
            File& operator=(const File&) = delete;
            File& operator=(File&&) = default;

            ~File()
            {
                pool().close(*this);
            }

            [[nodiscard]] friend bool operator==(const File& lhs, const File& rhs) noexcept
            {
                return &lhs == &rhs;
            }

            [[nodiscard]] const std::filesystem::path& path() const
            {
                return m_path;
            }

            [[nodiscard]] const std::string& openmode() const
            {
                return m_openmode;
            }

            [[nodiscard]] bool isOpen()
            {
                return m_poolEntry != pool().noneEntry();
            }

            [[nodiscard]] auto size()
            {
                return withHandle([&](NativeFileHandle handle) {
                    const auto originalPos = std::ftell(handle);
                    std::fseek(handle, 0, SEEK_END);
                    const std::size_t s = std::ftell(handle);
                    std::fseek(handle, originalPos, SEEK_SET);
                    return s;
                });
            }

            void seekset(std::size_t offset)
            {
                return withHandle([&](NativeFileHandle handle) {
                    std::fseek(handle, static_cast<long>(offset), SEEK_SET);
                });
            }

            void seekToEnd()
            {
                return withHandle([&](NativeFileHandle handle) {
                    std::fseek(handle, 0, SEEK_END);
                });
            }

            [[nodiscard]] std::size_t read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count)
            {
                return withHandle([&](NativeFileHandle handle) {
                    std::fseek(handle, static_cast<long>(offset), SEEK_SET);
                    return std::fread(static_cast<void*>(destination), elementSize, count, handle);
                });
            }

            [[nodiscard]] std::size_t write(const std::byte* source, std::size_t elementSize, std::size_t count)
            {
                return withHandle([&](NativeFileHandle handle) {
                    return std::fwrite(static_cast<const void*>(source), elementSize, count, handle);
                });
            }

            void flush()
            {
                withHandle([&](NativeFileHandle handle) {
                    std::fflush(handle);
                });
            }

        private:
            std::filesystem::path m_path;
            std::string m_openmode;

            // used by the pool
            FilePoolEntryIter m_poolEntry;
            // times opened is NOT concurrent opens but sequential opens
            std::size_t m_timesOpened;
        };
    }

    // NOTE: It is assumed that one *physical* file is not accessed concurrently anywhere.
    // NOTE: It is also assumed that the file is not changed by any means while being open.
    struct ImmutableBinaryFile
    {
        ImmutableBinaryFile(std::filesystem::path path) :
            m_file(std::make_shared<detail::File>(std::move(path), m_openmode)),
            m_size(m_file->size())
        {
        }

        ImmutableBinaryFile(const ImmutableBinaryFile&) = default;
        ImmutableBinaryFile(ImmutableBinaryFile&&) = default;
        ImmutableBinaryFile& operator=(const ImmutableBinaryFile&) = default;
        ImmutableBinaryFile& operator=(ImmutableBinaryFile&&) = default;

        [[nodiscard]] friend bool operator==(const ImmutableBinaryFile& lhs, const ImmutableBinaryFile& rhs) noexcept
        {
            return lhs.m_file == rhs.m_file;
        }

        [[nodiscard]] decltype(auto) isOpen() const
        {
            return m_file->isOpen();
        }

        [[nodiscard]] decltype(auto) path() const
        {
            return m_file->path();
        }

        [[nodiscard]] decltype(auto) openmode() const
        {
            return m_openmode;
        }

        [[nodiscard]] std::size_t read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const
        {
            return m_file->read(destination, offset, elementSize, count);
        }

        [[nodiscard]] std::size_t size() const
        {
            return m_size;
        }

    private:
        static inline const std::string m_openmode = "rb";

        std::shared_ptr<detail::File> m_file;
        std::size_t m_size;

        void seek(std::size_t offset) const
        {
            m_file->seekset(offset);
        }
    };

    enum struct OpenMode
    {
        Truncate,
        Append
    };

    struct BinaryOutputFile
    {
        BinaryOutputFile(std::filesystem::path path, OpenMode mode = OpenMode::Truncate) :
            m_file(std::make_unique<detail::File>(std::move(path), mode == OpenMode::Append ? m_openmodeAppend : m_openmodeTruncate))
        {
        }

        BinaryOutputFile(const BinaryOutputFile&) = delete;
        BinaryOutputFile(BinaryOutputFile&&) = default;
        BinaryOutputFile& operator=(const BinaryOutputFile&) = delete;
        BinaryOutputFile& operator=(BinaryOutputFile&&) = default;

        [[nodiscard]] friend bool operator==(const BinaryOutputFile& lhs, const BinaryOutputFile& rhs) noexcept
        {
            return &lhs == &rhs;
        }

        [[nodiscard]] decltype(auto) isOpen() const
        {
            return m_file->isOpen();
        }

        [[nodiscard]] decltype(auto) path() const
        {
            return m_file->path();
        }

        [[nodiscard]] decltype(auto) openmode() const
        {
            return m_file->openmode();
        }

        void write(const std::byte* source, std::size_t elementSize, std::size_t count) const
        {
            const std::size_t elementsWritten = m_file->write(source, elementSize, count);
            if (elementsWritten != count)
            {
                throw Exception("Cannot write all bytes.");
            }
        }

        // reopens the file in readonly mode
        [[nodiscard]] ImmutableBinaryFile seal()
        {
            flush();
            ImmutableBinaryFile f(m_file->path());
            m_file.reset();
            return f;
        }

        void flush()
        {
            m_file->flush();
        }

    private:
        static inline const std::string m_openmodeTruncate = "wb";
        static inline const std::string m_openmodeAppend = "ab";

        std::unique_ptr<detail::File> m_file;
    };

    struct BinaryInputOutputFile
    {
        BinaryInputOutputFile(std::filesystem::path path, OpenMode mode = OpenMode::Truncate) :
            m_file(std::make_unique<detail::File>(std::move(path), mode == OpenMode::Append ? m_openmodeAppend : m_openmodeTruncate))
        {
        }

        BinaryInputOutputFile(const BinaryInputOutputFile&) = delete;
        BinaryInputOutputFile(BinaryInputOutputFile&&) = default;
        BinaryInputOutputFile& operator=(const BinaryInputOutputFile&) = delete;
        BinaryInputOutputFile& operator=(BinaryInputOutputFile&&) = default;

        [[nodiscard]] friend bool operator==(const BinaryInputOutputFile& lhs, const BinaryInputOutputFile& rhs) noexcept
        {
            return &lhs == &rhs;
        }

        [[nodiscard]] decltype(auto) isOpen() const
        {
            return m_file->isOpen();
        }

        [[nodiscard]] decltype(auto) path() const
        {
            return m_file->path();
        }

        [[nodiscard]] decltype(auto) openmode() const
        {
            return m_file->openmode();
        }

        [[nodiscard]] std::size_t read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const
        {
            return m_file->read(destination, offset, elementSize, count);
        }

        [[nodiscard]] std::size_t size() const
        {
            return m_file->size();
        }

        void write(const std::byte* source, std::size_t elementSize, std::size_t count) const
        {
            seekToEnd();
            const std::size_t elementsWritten = m_file->write(source, elementSize, count);
            if (elementsWritten != count)
            {
                throw Exception("Cannot write all bytes.");
            }
        }

        // reopens the file in readonly mode
        [[nodiscard]] ImmutableBinaryFile seal()
        {
            flush();
            ImmutableBinaryFile f(m_file->path());
            m_file.reset();
            return f;
        }

        void flush()
        {
            m_file->flush();
        }

    private:
        static inline const std::string m_openmodeTruncate = "wb+";
        static inline const std::string m_openmodeAppend = "ab+";

        std::unique_ptr<detail::File> m_file;

        void seek(std::size_t offset) const
        {
            m_file->seekset(offset);
        }

        void seekToEnd() const
        {
            m_file->seekToEnd();
        }
    };

    template <typename T>
    struct Buffer
    {
        Buffer(std::size_t size) :
            m_data(std::make_unique<T[]>(size)),
            m_size(size)
        {
            ASSERT(size > 0u);
        }

        [[nodiscard]] T* data()
        {
            return m_data.get();
        }

        [[nodiscard]] const T* data() const
        {
            return m_data.get();
        }

        [[nodiscard]] const T& operator[](std::size_t i) const
        {
            return m_data[i];
        }

        [[nodiscard]] T& operator[](std::size_t i)
        {
            return m_data[i];
        }

        [[nodiscard]] std::size_t size() const
        {
            return m_size;
        }

        [[nodiscard]] std::size_t size_bytes() const
        {
            return size() * sizeof(T);
        }

    private:
        std::unique_ptr<T[]> m_data;
        std::size_t m_size;
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

            SequentialIterator(const ImmutableBinaryFile& file, std::size_t begin, std::size_t end, Buffer<T>&& buffer) :
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
            Buffer<T> m_buffer;
            T* m_bufBegin;
            T* m_bufEnd;

            void refillBuffer()
            {
                ASSERT(m_bufBegin == m_bufEnd);

                const std::size_t bytesLeft = m_fileEnd - m_fileBegin;
                if (bytesLeft == 0)
                {
                    m_bufBegin = nullptr;
                    m_bufEnd = nullptr;
                    return;
                }

                const std::size_t numObjectsToRead = std::min(bytesLeft / sizeof(T), m_buffer.size());

                const std::size_t numObjectsRead = m_file.read(
                    reinterpret_cast<std::byte*>(m_buffer.data()),
                    m_fileBegin,
                    sizeof(T),
                    numObjectsToRead
                );

                m_fileBegin += numObjectsToRead * sizeof(T);
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
                        throw Exception("Cannot read file.");
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
        }

        ImmutableSpan(const ImmutableBinaryFile& file, std::size_t begin, std::size_t size) :
            m_file(file),
            m_begin(begin),
            m_end(begin + size)
        {
            ASSERT(begin <= end);
            ASSERT(end * sizeof(T) <= m_file.size());
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

        void read(T* destination, std::size_t offset, std::size_t count) const
        {
            const std::size_t elementsRead = m_file.read(
                reinterpret_cast<std::byte*>(destination),
                offset * sizeof(T),
                sizeof(T),
                count
            );

            if (elementsRead != count)
            {
                throw Exception("Cannot read file.");
            }
        }

        [[nodiscard]] std::size_t read(T* destination) const
        {
            read(destination, 0, size());
            return size();
        }

        [[nodiscard]] T operator[](std::size_t i) const
        {
            ASSERT(i < size());

            T value;
            read(&value, i, 1u);

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

        [[nodiscard]] auto begin_seq(Buffer<T>&& buffer = Buffer<T>(1024)) const
        {
            return SequentialIterator(m_file, m_begin, m_end, std::move(buffer));
        }

        [[nodiscard]] auto end_seq() const
        {
            return typename SequentialIterator::Sentinel{};
        }

        [[nodiscard]] auto cbegin_seq(Buffer<T>&& buffer = Buffer<T>(1024)) const
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

        [[nodiscard]] auto begin(Buffer<T>&& buffer = Buffer<T>(1024)) const
        {
            return begin_seq(std::move(buffer));
        }

        [[nodiscard]] auto end() const
        {
            return end_seq();
        }

        [[nodiscard]] auto cbegin(Buffer<T>&& buffer = Buffer<T>(1024)) const
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

        // it "borrows" the file, allows its later retrieval
        BackInserter(BinaryOutputFile&& file, Buffer<T>&& buffer = Buffer<T>(1024)) :
            m_file(std::move(file)),
            m_buffer(std::move(buffer)),
            m_nextEmpty(m_buffer.data()),
            m_live(true)
        {
        }

        BackInserter(const BackInserter&) = delete;
        BackInserter(BackInserter&&) = default;
        BackInserter& operator=(const BackInserter&) = delete;
        BackInserter& operator=(BackInserter&&) = default;

        ~BackInserter()
        {
            flushBuffer();
        }

        [[nodiscard]] decltype(auto) path() const
        {
            return m_file.path();
        }

        template <typename... Ts>
        void emplace(Ts&& ... args)
        {
            *(m_nextEmpty++) = T{ std::forward<Ts>(args)... };

            if (m_nextEmpty == m_buffer.data() + m_buffer.size())
            {
                flushBuffer();
            }
        }

        // pods don't benefit from move semantics
        void push(const T& value)
        {
            *(m_nextEmpty++) = value;

            if (m_nextEmpty == m_buffer.data() + m_buffer.size())
            {
                flushBuffer();
            }
        }

        void append(const T* data, std::size_t count)
        {
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
                flushBuffer();
                m_file.write(reinterpret_cast<const std::byte*>(data), sizeof(T), count);
                m_file.flush();
            }
        }

        void flush()
        {
            flushBuffer();
        }

        [[nodiscard]] BinaryOutputFile release() &&
        {
            if (m_live)
            {
                flushBuffer();
                m_live = false;
            }
            return std::move(m_file);
        }

        [[nodiscard]] Buffer<T> buffer() &&
        {
            if (m_live)
            {
                flushBuffer();
                m_live = false;
            }
            return std::move(m_buffer);
        }

    private:
        BinaryOutputFile m_file;
        Buffer<T> m_buffer;
        T* m_nextEmpty;
        bool m_live;

        void flushBuffer()
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

            m_file.write(
                reinterpret_cast<const std::byte*>(m_buffer.data()),
                sizeof(T),
                numObjectsToWrite
            );

            m_file.flush();

            m_nextEmpty = m_buffer.data();
        }
    };

    template <typename T>
    struct Vector
    {
        static_assert(std::is_trivially_copyable_v<T>);

        using value_type = T;

        // it "borrows" the file, allows its later retrieval
        Vector(BinaryInputOutputFile&& file, Buffer<T>&& buffer = Buffer<T>(1024)) :
            m_file(std::move(file)),
            m_buffer(std::move(buffer)),
            m_nextEmpty(m_buffer.data()),
            m_size(m_file.size())
        {
        }

        Vector(const Vector&) = delete;
        Vector(Vector&&) = default;
        Vector& operator=(const Vector&) = delete;
        Vector& operator=(Vector&&) = default;

        ~Vector()
        {
            flushBuffer();
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

        void read(T* destination, std::size_t offset, std::size_t count) const
        {
            flushBuffer();

            const std::size_t elementsRead = m_file.read(
                reinterpret_cast<std::byte*>(destination),
                offset * sizeof(T),
                sizeof(T),
                count
            );

            if (elementsRead != count)
            {
                throw Exception("Cannot read file.");
            }
        }

        [[nodiscard]] std::size_t read(T* destination) const
        {
            read(destination, 0, size());
            return size();
        }

        [[nodiscard]] T operator[](std::size_t i) const
        {
            ASSERT(i < size());

            T value;
            read(&value, i, 1u);

            return value;
        }

        [[nodiscard]] T front() const
        {
            ASSERT(!empty());

            return operator[](0);
        }

        [[nodiscard]] T back() const
        {
            ASSERT(!empty());

            return operator[](size() - 1u);
        }

        template <typename... Ts>
        void emplace_back(Ts&& ... args)
        {
            *(m_nextEmpty++) = T{ std::forward<Ts>(args)... };

            if (m_nextEmpty == m_buffer.data() + m_buffer.size())
            {
                flushBuffer();
            }

            m_size += 1;
        }

        // pods don't benefit from move semantics
        void push_back(const T& value)
        {
            *(m_nextEmpty++) = value;

            if (m_nextEmpty == m_buffer.data() + m_buffer.size())
            {
                flushBuffer();
            }

            m_size += 1;
        }

        void append(const T* data, std::size_t count)
        {
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
                flushBuffer();
                m_file.write(reinterpret_cast<const std::byte*>(data), sizeof(T), count);
                m_file.flush();
            }

            m_size += count;
        }

        void flush()
        {
            flushBuffer();
        }

    private:
        BinaryInputOutputFile m_file;
        Buffer<T> m_buffer;
        T* m_nextEmpty;
        std::size_t m_size;

        void flushBuffer()
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

            m_file.write(
                reinterpret_cast<const std::byte*>(m_buffer.data()),
                sizeof(T),
                numObjectsToWrite
            );

            m_file.flush();

            m_nextEmpty = m_buffer.data();
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
        TemporaryPaths(std::filesystem::path dir = "") :
            m_dir(std::move(dir))
        {
            std::filesystem::create_directories(dir);
        }

        [[nodiscard]] std::filesystem::path& next()
        {
            return m_paths.emplace_back(uniquePath(m_dir));
        }

        [[nodiscard]] const std::filesystem::path& last()
        {
            return m_paths.back();
        }

        ~TemporaryPaths()
        {
            for (auto& path : m_paths)
            {
                std::filesystem::remove(path);
            }
        }

    private:
        std::filesystem::path m_dir;
        std::vector<std::filesystem::path> m_paths;
    };

    // these "borrow" the file for the function call
    template <typename T, typename FuncT>
    void withBackInserter(BinaryOutputFile& file, FuncT&& func)
    {
        auto backInserter = BackInserter<T>(std::move(file));
        std::forward<FuncT>(func)(backInserter);
        file = std::move(backInserter).release();
    }
    template <typename T, typename FuncT>
    void withBackInserterUnbuffered(BinaryOutputFile& file, FuncT&& func)
    {
        static Buffer<T> buffer(1);
        auto backInserter = BackInserter<T>(std::move(file), std::move(buffer));
        std::forward<FuncT>(func)(backInserter);
        buffer = std::move(backInserter).buffer();
        file = std::move(backInserter).release();
    }

    template <typename T, typename FuncT>
    void withBackInserter(BinaryOutputFile& file, Buffer<T>&& buffer, FuncT&& func)
    {
        auto backInserter = BackInserter<T>(std::move(file), std::move(buffer));
        std::forward<FuncT>(func)(backInserter);
        file = std::move(backInserter).release();
    }

    struct ProgressReport
    {
        std::size_t workDone;
        std::size_t workTotal;

        [[nodiscard]] double ratio() const
        {
            return static_cast<double>(workDone) / static_cast<double>(workTotal);
        }
    };

    struct AuxilaryStorage
    {
        std::size_t memory;
        std::filesystem::path tempdir = ".ext";
    };

    namespace detail
    {
        struct NoProgressCallback
        {
            void operator()(ProgressReport) const noexcept {};
        };

        template <typename T>
        constexpr bool isCallbackEmpty = std::is_same_v<std::remove_cv_t<std::remove_reference_t<T>>, NoProgressCallback>;

        template <typename CallbackT>
        struct Progress
        {
            static constexpr bool hasCallback = !isCallbackEmpty<CallbackT>;

            Progress(CallbackT& callback) :
                m_report{ 0, 0 },
                m_callback(callback)
            {
            }

            void doCallback() const
            {
                if constexpr (hasCallback)
                {
                    m_callback(m_report);
                }
            }

            void reportWork(std::size_t work)
            {
                if constexpr (hasCallback)
                {
                    m_report.workDone += work;
                    doCallback();
                }
            }

            void setTotalWork(std::size_t work)
            {
                if constexpr (hasCallback)
                {
                    m_report.workTotal = work;
                }
            }

            [[nodiscard]] std::size_t totalWork() const
            {
                return m_report.workTotal;
            }

        private:
            ProgressReport m_report;
            CallbackT& m_callback;
        };

        [[nodiscard]] auto noProgressCallback()
        {
            static NoProgressCallback callback;
            return Progress(callback);
        }
    }

    template <typename T>
    void writeFile(const std::filesystem::path& path, const T* data, const std::size_t count)
    {
        const std::string pathString = path.string();
        auto file = std::fopen(pathString.c_str(), "wb");
        if (file == nullptr)
        {
            throw Exception("Cannot open file.");
        }
        const std::size_t objectsWritten = std::fwrite(data, sizeof(T), count, file);
        if (objectsWritten != count)
        {
            throw Exception("Cannot write all elements.");
        }

        std::fclose(file);
    }

    template <typename T>
    void copy(const ImmutableSpan<T>& in, BinaryOutputFile& outFile)
    {
        withBackInserter<T>(outFile, [&](BackInserter<T>& out) {
            for (auto& v : in)
            {
                out.push(v);
            }
            });
    }

    template <typename T>
    void concat(const std::vector<ImmutableSpan<T>>& in, BinaryOutputFile& outFile)
    {
        withBackInserter<T>(outFile, [&](BackInserter<T>& out) {
            for (auto& i : in)
            {
                for (auto& v : i)
                {
                    out.push(v);
                }
            }
            });
    }

    namespace detail::merge
    {
        static constexpr std::size_t outputBufferSizeMultiplier = 2;
        static constexpr std::size_t maxNumMergedInputs = 16;

        [[nodiscard]] std::size_t merge_assess_work(
            std::vector<std::size_t>::const_iterator inSizesBegin,
            std::vector<std::size_t>::const_iterator inSizesEnd
        )
        {
            const std::size_t numInputs = std::distance(inSizesBegin, inSizesEnd);

            if (numInputs <= maxNumMergedInputs)
            {
                return std::accumulate(inSizesBegin, inSizesEnd, static_cast<std::size_t>(0));
            }

            // prepare at most maxNumMergedInputs parts
            const std::size_t numInputsPerPart = ceilDiv(numInputs, maxNumMergedInputs);
            std::size_t writes = 0;
            std::size_t offset = 0;
            for (; offset + numInputsPerPart < numInputs; offset += numInputsPerPart)
            {
                writes += merge_assess_work(std::next(inSizesBegin, offset), std::next(inSizesBegin, offset + numInputsPerPart));
            }

            // we may have some inputs left if numInputs % numInputsPerPart != 0
            if (offset + 1u < numInputs)
            {
                writes += merge_assess_work(std::next(inSizesBegin, offset), inSizesEnd);
            }

            return writes + std::accumulate(inSizesBegin, inSizesEnd, static_cast<std::size_t>(0));
        }

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

        // the merge is stable - ie. it takes as many values from the first input as possible, then next, and so on
        template <typename T, typename ContainerT, typename CompT, typename ProgressCallbackT>
        void merge_no_recurse(
            std::size_t maxMemoryBytes,
            ContainerIterRange<ContainerT> in,
            BinaryOutputFile& outFile,
            CompT cmp,
            detail::Progress<ProgressCallbackT>& progress
        )
        {
            // TODO: sfinae if other types can be merged
            static_assert(std::is_same_v<typename ContainerT::value_type, ImmutableSpan<T>>);

            using InputRange = ContainerIterRange<ImmutableSpan<T>>;

            const std::size_t numInputs = in.distance();

            ASSERT(numInputs <= maxNumMergedInputs);

            const std::size_t bufferUnitSize = numObjectsPerBufferUnit<T>(
                maxMemoryBytes, numInputs + outputBufferSizeMultiplier
                );

            // gather non empty ranges
            std::vector<InputRange> iters;
            iters.reserve(numInputs);
            std::size_t maxInputBufferSize = 0;
            std::size_t numTotal = 0;
            for (auto& i : in)
            {
                const std::size_t size = i.size();
                if (size == 0)
                {
                    continue;
                }

                const std::size_t bufferSize = std::min(bufferUnitSize, size);
                maxInputBufferSize = std::max(maxInputBufferSize, bufferSize);
                numTotal += size;

                iters.emplace_back(i.begin(Buffer<T>(bufferSize)), i.end());
            }

            ASSERT(iters.size() > 1);

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
                nextValues[i] = *(iters[i].begin());
            }

            std::size_t numProcessed = 0;
            const std::size_t outputBufferSize = maxInputBufferSize * outputBufferSizeMultiplier;
            withBackInserter<T>(outFile, Buffer<T>(outputBufferSize), [&](BackInserter<T>& out) {
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
                    out.push(nextValues[minIdx]);

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

                    ++numProcessed;
                    if (numProcessed >= outputBufferSize)
                    {
                        progress.reportWork(numProcessed);
                        numProcessed = 0;
                    }
                }
                });

            progress.reportWork(numProcessed);
        }

        template <typename T, typename ContainerT, typename CompT, typename ProgressCallbackT>
        void merge_impl(
            std::size_t maxMemoryBytes,
            const std::filesystem::path& tempdir,
            ContainerIterRange<ContainerT> in,
            BinaryOutputFile& outFile,
            CompT cmp,
            detail::Progress<ProgressCallbackT>& progress
        )
        {
            // TODO: sfinae if other types can be merged
            static_assert(std::is_same_v<typename ContainerT::value_type, ImmutableSpan<T>>);

            const std::size_t numInputs = in.distance();

            if (numInputs <= maxNumMergedInputs)
            {
                merge_no_recurse<T, ContainerT>(maxMemoryBytes, in, outFile, cmp, progress);
                return;
            }

            // IMPORTANT: temporary files need to be deleted after parts!
            TemporaryPaths temporaryFiles(tempdir);

            std::vector<ImmutableSpan<T>> parts;
            parts.reserve(maxNumMergedInputs);

            // prepare at most maxNumMergedInputs parts
            const std::size_t numInputsPerPart = ceilDiv(numInputs, maxNumMergedInputs);
            std::size_t offset = 0;
            for (; offset + numInputsPerPart < numInputs; offset += numInputsPerPart)
            {
                BinaryOutputFile partOut(temporaryFiles.next());
                merge_impl<T, ContainerT>(
                    maxMemoryBytes,
                    tempdir,
                    {
                        std::next(std::begin(in), offset),
                        std::next(std::begin(in), offset + numInputsPerPart)
                    },
                    partOut,
                    cmp,
                    progress
                    );

                parts.emplace_back(partOut.seal());
            }

            // we may have some inputs left if numInputs % numInputsPerPart != 0
            if (offset != numInputs)
            {
                if (offset + 1u == numInputs)
                {
                    // the last input is singular, don't copy
                    parts.emplace_back(*std::next(std::begin(in), offset));
                }
                else
                {
                    BinaryOutputFile partOut(temporaryFiles.next());
                    merge_impl<T, ContainerT>(
                        maxMemoryBytes,
                        tempdir,
                        {
                            std::next(std::begin(in), offset),
                            std::end(in)
                        },
                        partOut,
                        cmp,
                        progress
                        );

                    parts.emplace_back(partOut.seal());
                }
            }

            merge_no_recurse<T, ContainerT>(maxMemoryBytes, parts, outFile, cmp, progress);
        }
    }

    template <typename T, typename CompT = std::less<>>
    void merge(
        const AuxilaryStorage & aux,
        const std::vector<ImmutableSpan<T>> & in,
        BinaryOutputFile & outFile,
        CompT cmp = CompT{}
    )
    {
        auto progress = detail::noProgressCallback();
        return detail::merge::merge_impl<T, const std::vector<ImmutableSpan<T>>>(
            aux.memory, aux.tempdir, in, outFile, cmp, progress
            );
    }

    template <typename T, typename ProgressCallbackT, typename CompT = std::less<>>
    void merge(ProgressCallbackT && callback,
        const AuxilaryStorage & aux,
        const std::vector<ImmutableSpan<T>> & in,
        BinaryOutputFile & outFile,
        CompT cmp = CompT{}
    )
    {
        auto progress = detail::Progress(callback);
        progress.setTotalWork(detail::merge::merge_assess_work(in));
        return detail::merge::merge_impl<T, const std::vector<ImmutableSpan<T>>>(
            aux.memory, aux.tempdir, in, outFile, cmp, progress
            );
    }

    template <typename T>
    [[nodiscard]] std::size_t merge_assess_work(const std::vector<ImmutableSpan<T>>& in)
    {
        return detail::sort::merge_assess_work(in);
    }

    namespace detail::sort
    {
        template <typename T>
        [[nodiscard]] std::size_t sort_assess_work(std::size_t maxMemoryBytes, const ImmutableSpan<T>& in)
        {
            const std::size_t inputSize = in.size();
            const std::size_t bufferSize = numObjectsPerBufferUnit<T>(maxMemoryBytes, 1);

            const std::size_t numParts = ceilDiv(inputSize, bufferSize);

            if (numParts <= 1)
            {
                return inputSize;
            }

            std::vector<std::size_t> parts;
            parts.reserve(numParts);

            std::size_t offset = 0;
            for (; offset + bufferSize < inputSize; offset += bufferSize)
            {
                parts.emplace_back(bufferSize);
            }

            if (offset != inputSize)
            {
                parts.emplace_back(inputSize - offset);
            }

            return inputSize + detail::merge::merge_assess_work(std::begin(parts), std::end(parts));
        }

        template <typename T, typename SortT, typename CompT, typename ProgressCallbackT>
        void sort_impl(
            std::size_t maxMemoryBytes,
            const std::filesystem::path& tempdir,
            const ImmutableSpan<T>& in,
            BinaryOutputFile& outFile,
            SortT sort,
            CompT cmp,
            detail::Progress<ProgressCallbackT>& progress
        )
        {
            const std::size_t inputSize = in.size();

            const std::size_t bufferSize = numObjectsPerBufferUnit<T>(maxMemoryBytes, 1);
            const std::size_t numParts = ceilDiv(inputSize, bufferSize);

            if (numParts == 1)
            {
                Buffer<T> buffer(inputSize);
                in.read(buffer.data(), 0, inputSize);
                sort(buffer.data(), buffer.data() + inputSize, cmp);

                withBackInserterUnbuffered<T>(outFile, [&](BackInserter<T>& out) {
                    out.append(buffer.data(), inputSize);
                    });

                progress.reportWork(inputSize);
                return;
            }

            // IMPORTANT: temporary files need to be deleted after parts!
            TemporaryPaths temporaryFiles(tempdir);

            std::vector<ImmutableSpan<T>> parts;
            Buffer<T> buffer(bufferSize);

            std::size_t offset = 0;
            for (; offset + bufferSize < inputSize; offset += bufferSize)
            {
                (void)in.read(buffer.data(), offset, bufferSize);
                sort(buffer.data(), buffer.data() + bufferSize, cmp);

                BinaryOutputFile partOut(temporaryFiles.next());
                withBackInserterUnbuffered<T>(partOut, [&](BackInserter<T>& out) {
                    out.append(buffer.data(), bufferSize);
                    });
                parts.emplace_back(partOut.seal());

                progress.reportWork(bufferSize);
            }

            if (offset != inputSize)
            {
                const std::size_t left = inputSize - offset;
                (void)in.read(buffer.data(), offset, left);
                sort(buffer.data(), buffer.data() + left, cmp);

                BinaryOutputFile partOut(temporaryFiles.next());
                withBackInserterUnbuffered<T>(partOut, [&](BackInserter<T>& out) {
                    out.append(buffer.data(), left);
                    });
                parts.emplace_back(partOut.seal());

                progress.reportWork(left);
            }

            return detail::merge::merge_impl<T, const std::vector<ImmutableSpan<T>>>(
                maxMemoryBytes, tempdir, parts, outFile, cmp, progress
                );
        }
    }

    template <typename T, typename CompT = std::less<>>
    void sort(
        const AuxilaryStorage & aux,
        const ImmutableSpan<T> & in,
        BinaryOutputFile & outFile,
        CompT cmp = CompT{}
    )
    {
        auto progress = detail::noProgressCallback();
        return detail::sort::sort_impl<T>(
            aux.memory,
            aux.tempdir,
            in,
            outFile,
            [](auto... args) { std::sort(std::forward<decltype(args)>(args)...); },
            cmp,
            progress
            );
    }

    template <typename T, typename ProgressCallbackT, typename CompT = std::less<>>
    void sort(
        ProgressCallbackT && callback,
        const AuxilaryStorage & aux,
        const ImmutableSpan<T> & in,
        BinaryOutputFile & outFile,
        CompT cmp = CompT{}
    )
    {
        auto progress = detail::Progress(callback);
        progress.setTotalWork(detail::sort::sort_assess_work(aux.memory, in));
        return detail::sort::sort_impl<T>(
            aux.memory,
            aux.tempdir,
            in,
            outFile,
            [](auto... args) { std::sort(std::forward<decltype(args)>(args)...); },
            cmp,
            progress
            );
    }

    template <typename T, typename CompT = std::less<>>
    void stable_sort(
        const AuxilaryStorage & aux,
        const ImmutableSpan<T> & in,
        BinaryOutputFile & outFile,
        CompT cmp = CompT{}
    )
    {
        auto progress = detail::noProgressCallback();
        return detail::sort::sort_impl<T>(
            aux.memory,
            aux.tempdir,
            in,
            outFile,
            [](auto... args) { std::stable_sort(std::forward<decltype(args)>(args)...); },
            cmp,
            progress
            );
    }

    template <typename T, typename ProgressCallbackT, typename CompT = std::less<>>
    void stable_sort(
        ProgressCallbackT && callback,
        const AuxilaryStorage & aux,
        const ImmutableSpan<T> & in,
        BinaryOutputFile & outFile,
        CompT cmp = CompT{}
    )
    {
        auto progress = detail::Progress(callback);
        progress.setTotalWork(detail::sort::sort_assess_work(aux.memory, in));
        return detail::sort::sort_impl<T>(
            aux.memory,
            aux.tempdir,
            in,
            outFile,
            [](auto... args) { std::stable_sort(std::forward<decltype(args)>(args)...); },
            cmp,
            progress
            );
    }

    template <typename T>
    [[nodiscard]] std::size_t sort_assess_work(const AuxilaryStorage& aux, const ImmutableSpan<T>& in)
    {
        return detail::sort::sort_assess_work(aux.memory, in);
    }

    template <typename T>
    [[nodiscard]] std::size_t stable_sort_assess_work(const AuxilaryStorage& aux, const ImmutableSpan<T>& in)
    {
        return detail::sort::sort_assess_work(aux.memory, in);
    }

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
        )
        {
            const std::size_t leftSize = static_cast<std::size_t>(mid - begin);
            const std::size_t rightSize = static_cast<std::size_t>(end - mid);
            const std::size_t count = rightSize + leftSize;
            if (count <= size)
            {
                return { begin, end };
            }

            const std::size_t radius = size / 2u;

            // here count > size so only we know there is enough space for radius elements
            // at least on one side
            if (leftSize < radius)
            {
                // align to left and span the rest
                end = begin + size;
            }
            else if (rightSize < radius)
            {
                // align to right and span the rest
                begin = end - size;
            }
            else
            {
                // both sides are big enough
                begin = mid - radius;
                end = mid + radius;
            }

            return { begin, end };
        }

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

        template <typename RandomIterT, typename T = typename RandomIterT::value_type>
        struct IterValuePair
        {
            RandomIterT it;
            T value;
        };

        struct DoCrossUpdates {};
        struct NoCrossUpdates {};

        template <typename KeyType>
        using Range = std::vector<std::pair<IterValuePair<std::size_t, KeyType>, IterValuePair<std::size_t, KeyType>>>;

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
            // 32KiB should be abount how much we can read using 'constant' time.
            constexpr std::size_t maxSeqReadSize = 32 * 1024;
            constexpr std::size_t maxNumSeqReadElements = std::max(static_cast<std::size_t>(3), maxSeqReadSize / sizeof(EntryType));
            static_assert(maxNumSeqReadElements >= 3, "We need at least 3 values at once to properly narrow the search");

            const std::size_t end = data.size();

            Buffer<EntryType> buffer(maxNumSeqReadElements);

            auto readToBuffer = [&](std::size_t begin, std::size_t end) {
                ASSERT(begin != end);
                ASSERT(end - begin <= maxNumSeqReadElements);

                data.read(buffer.data(), begin, end - begin);

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

                        ASSERT(extractKey(buf[mid - a]) == key);
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
                // Find a range entry that contains keys[i] or, if there is none, get
                // the next range.
                // NOTE: we don't use cmp here because we're comparing index entries,
                // not data values.
                auto [a, b] = std::equal_range(index.begin(), index.end(), keys[i]);

                KeyType lowValue{}, highValue{};
                std::size_t low = end;
                std::size_t high = end;

                if (b == index.begin() || a == index.end())
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
                    if (cmp(keys[i], lowValue) || cmp(highValue, keys[i]))
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

                IterValuePair<std::size_t, KeyType> aa{ low, lowValue };
                IterValuePair<std::size_t, KeyType> bb{ high, highValue };
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
            MiddleT middle = MiddleT{})
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

    template <
        typename EntryType, 
        typename CompareT = std::less<>,
        typename KeyExtractT = detail::equal_range::Identity
    >
    auto makeIndex(
        const std::vector<EntryType> & values, 
        std::size_t size,
        CompareT cmp = CompareT{},
        KeyExtractT key = KeyExtractT{}
    )
    {
        using KeyType = decltype(key(std::declval<EntryType>()));

        ASSERT(values.size() > 0);

        if (size == 0)
        {
            RangeIndexEntry<KeyType, CompareT> e{
                0, values.size() - 1u,
                key(values.front()), key(values.back()) 
            };
            return RangeIndex<KeyType, CompareT>({ e });
        }

        std::vector<RangeIndexEntry<KeyType, CompareT>> iters;
        iters.reserve(size);

        for (std::size_t i = 0; i < size; ++i)
        {
            // We look at values at equally spaced points and look
            // for the first occurence.
            // This guarantees that there is either one entry every N values
            // or that there's a long span of identical values - index
            // entry will hold bounds of the whole span.
            const std::size_t idx = values.size() * i / size;

            // duplicates are unwanted
            if (i == 0 || cmp(iters.back().lowValue, key(values[idx])))
            {
                const auto it = std::lower_bound(values.begin(), values.begin() + (idx + 1u), values[idx], cmp);
                std::size_t idxx = static_cast<std::size_t>(std::distance(values.begin(), it));
                RangeIndexEntry<KeyType, CompareT> e{ idxx, idxx, key(values[idx]) };
                iters.emplace_back(e);
            }
        }

        ASSERT(!iters.empty());

        for (std::size_t i = 1; i < iters.size(); ++i)
        {
            const auto& e = iters[i];
            auto& pe = iters[i - 1];
            pe.high = e.low - 1;
            pe.highValue = key(values[pe.high]);
        }
        iters.back().high = values.size() - 1u;
        iters.back().highValue = key(values.back());

        ASSERT(iters.front().low == 0);
        ASSERT(iters.back().high == values.size() - 1u);
        ASSERT(iters.front().lowValue == key(values.front()));
        ASSERT(iters.back().highValue == key(values.back()));

        return RangeIndex<KeyType, CompareT>(std::move(iters));
    }
}
