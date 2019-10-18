#include "External.h"

#include "util/Assert.h"

#include "Configuration.h"
#include "util/MemoryAmount.h"
#include "Logger.h"

#include <iostream>

#include <atomic>
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <numeric>
#include <queue>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

namespace ext
{
    namespace detail::except
    {
        [[noreturn]] void throwAppendException(const std::filesystem::path& path, std::size_t requested, std::size_t written)
        {
            throw Exception(
                "Cannot append to file " + path.string()
                + ". Written " + std::to_string(written) + " out of " + std::to_string(requested) + " elements."
            );
        }

        [[noreturn]] void throwReadException(const std::filesystem::path& path, std::size_t offset, std::size_t requested, std::size_t read)
        {
            throw Exception(
                "Cannot read from file " + path.string()
                + ". Read " + std::to_string(read) + " out of " + std::to_string(requested) + " elements at offset " + std::to_string(offset) + "."
            );
        }

        [[noreturn]] void throwOpenException(const std::filesystem::path& path, const std::string& openmode)
        {
            throw Exception(
                "Cannot open file " + path.string()
                + " with openmode + " + openmode
            );
        }
    }

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

    void FileDeleter::operator()(std::FILE* ptr) const noexcept
    {
        std::fclose(ptr);
    }

    namespace detail
    {
        [[nodiscard]] auto openFile(const std::filesystem::path& path, const std::string& openmode)
        {
            std::string pathStr = path.string();
            auto h = FileHandle(std::fopen(pathStr.c_str(), openmode.c_str()));
            if (h == nullptr)
            {
                except::throwOpenException(path, openmode);
            }
            return h;
        }

        [[nodiscard]] auto fileTell(NativeFileHandle fh)
        {
            return _ftelli64_nolock(fh);
        }

        auto fileSeek(NativeFileHandle fh, std::int64_t offset)
        {
            return _fseeki64_nolock(fh, offset, SEEK_SET);
        }

        auto fileSeek(NativeFileHandle fh, std::int64_t offset, int origin)
        {
            return _fseeki64_nolock(fh, offset, origin);
        }

        const std::size_t PooledFile::FilePool::numMaxConcurrentOpenFiles = cfg::g_config["ext"]["max_concurrent_open_pooled_files"].get<std::size_t>();

        [[nodiscard]] PooledFile::FilePoolEntryIter PooledFile::FilePool::noneEntry()
        {
            return m_files.end();
        }

        void PooledFile::FilePool::close(PooledFile& file)
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            closeNoLock(file);
        }

        void PooledFile::FilePool::closeNoLock(PooledFile& file)
        {
            if (file.m_poolEntry != noneEntry())
            {
                std::unique_lock<std::mutex> lock(file.m_mutex);

                m_files.erase(file.m_poolEntry);
                file.m_poolEntry = noneEntry();
            }
        }

        [[nodiscard]] FileHandle PooledFile::FilePool::reopen(const PooledFile& file)
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

        [[nodiscard]] FileHandle PooledFile::FilePool::open(const PooledFile& file)
        {
            ASSERT(file.m_timesOpened == 0u);

            return openFile(file.m_path, file.m_openmode);
        }

        void PooledFile::FilePool::closeLastFile()
        {
            ASSERT(!m_files.empty());

            const PooledFile& file = *(m_files.front().second);

            std::unique_lock<std::mutex> lock(file.m_mutex);

            file.m_poolEntry = noneEntry();
            m_files.pop_front();
        }

        [[nodiscard]] NativeFileHandle PooledFile::FilePool::getHandle(const PooledFile& file)
        {
            std::unique_lock<std::mutex> lock(m_mutex);

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

            // TODO: we may want to unlock the mutex for opening the files
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

        void PooledFile::FilePool::updateEntry(const FilePoolEntryIter& it)
        {
            // Move the entry to the end (back)
            m_files.splice(m_files.end(), m_files, it);
        }

        PooledFile::FilePool& PooledFile::pool()
        {
            static FilePool s_pool;
            return s_pool;
        }

        PooledFile::PooledFile(std::filesystem::path path, std::string openmode) :
            m_path(std::move(path)),
            m_openmode(std::move(openmode)),
            m_poolEntry(pool().noneEntry()),
            m_timesOpened(0)
        {
        }

        PooledFile::~PooledFile()
        {
            pool().close(*this);
        }

        [[nodiscard]] bool operator==(const PooledFile& lhs, const PooledFile& rhs) noexcept
        {
            return &lhs == &rhs;
        }

        [[nodiscard]] const std::filesystem::path& PooledFile::path() const
        {
            return m_path;
        }

        [[nodiscard]] const std::string& PooledFile::openmode() const
        {
            return m_openmode;
        }

        [[nodiscard]] bool PooledFile::isOpen() const
        {
            return m_poolEntry != pool().noneEntry();
        }

        [[nodiscard]] std::size_t PooledFile::size() const
        {
            return withHandle([&](NativeFileHandle handle) {
                const auto originalPos = fileTell(handle);
                fileSeek(handle, 0, SEEK_END);
                const std::size_t s = fileTell(handle);
                fileSeek(handle, originalPos, SEEK_SET);
                return s;
                });
        }

        [[nodiscard]] std::size_t PooledFile::read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const
        {
            return withHandle([&](NativeFileHandle handle) {
                fileSeek(handle, offset, SEEK_SET);
                return std::fread(static_cast<void*>(destination), elementSize, count, handle);
                });
        }

        [[nodiscard]] std::size_t PooledFile::append(const std::byte* source, std::size_t elementSize, std::size_t count)
        {
            return withHandle([&](NativeFileHandle handle) {
                fileSeek(handle, 0, SEEK_END);
                return std::fwrite(static_cast<const void*>(source), elementSize, count, handle);
                });
        }

        void PooledFile::flush()
        {
            withHandle([&](NativeFileHandle handle) {
                std::fflush(handle);
                });
        }

        [[nodiscard]] bool PooledFile::isPooled() const
        {
            return true;
        }

        void PooledFile::truncate()
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            // We have to close the file but we cannot use pool's method
            // because it would try to lock the file again.
            // This is pretty much the only way we can ensure atomicity here.
            if (m_poolEntry != pool().noneEntry())
            {
                std::fclose(m_poolEntry->first.get());
                std::filesystem::resize_file(m_path, 0);
                // We don't care about openmode here because we truncate anyway
                m_poolEntry->first = detail::openFile(m_path, m_openmode);
            }
            else
            {
                std::filesystem::resize_file(m_path, 0);
            }
        }

        inline const std::size_t File::maxUnpooledOpenFiles = cfg::g_config["ext"]["max_concurrent_open_unpooled_files"].get<std::size_t>();

        File::File(std::filesystem::path path, std::string openmode) :
            m_path(std::move(path)),
            m_openmode(std::move(openmode))
        {
            open();
        }

        File::~File()
        {
            close();
        }

        [[nodiscard]] bool operator==(const File& lhs, const File& rhs) noexcept
        {
            return &lhs == &rhs;
        }

        [[nodiscard]] const std::filesystem::path& File::path() const
        {
            return m_path;
        }

        [[nodiscard]] const std::string& File::openmode() const
        {
            return m_openmode;
        }

        void File::close()
        {
            m_handle.reset();
            m_numOpenFiles -= 1;
        }

        void File::open()
        {
            // This is not designed to be a hard limits.
            // It it prone to data races.
            // We only want to reasonably restrict the number of
            // unpooled files open at once so that
            // we don't fail to open a PooledFile
            if (m_numOpenFiles.load() >= maxUnpooledOpenFiles)
            {
                except::throwOpenException(m_path, m_openmode);
            }

            m_handle = openFile(m_path, m_openmode);
            m_numOpenFiles += 1;
        }

        [[nodiscard]] bool File::isOpen() const
        {
            return m_handle != nullptr;
        }

        [[nodiscard]] std::size_t File::size() const
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            ASSERT(m_handle.get() != nullptr);
            const auto originalPos = fileTell(m_handle.get());
            fileSeek(m_handle.get(), 0, SEEK_END);
            const auto s = fileTell(m_handle.get());
            fileSeek(m_handle.get(), originalPos, SEEK_SET);
            return s;
        }

        [[nodiscard]] std::size_t File::read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            ASSERT(m_handle.get() != nullptr);
            fileSeek(m_handle.get(), offset, SEEK_SET);
            return std::fread(static_cast<void*>(destination), elementSize, count, m_handle.get());
        }

        [[nodiscard]] std::size_t File::append(const std::byte* source, std::size_t elementSize, std::size_t count)
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            ASSERT(m_handle.get() != nullptr);
            fileSeek(m_handle.get(), 0, SEEK_END);
            return std::fwrite(static_cast<const void*>(source), elementSize, count, m_handle.get());
        }

        void File::flush()
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            ASSERT(m_handle.get() != nullptr);
            std::fflush(m_handle.get());
        }

        [[nodiscard]] bool File::isPooled() const
        {
            return false;
        }

        void File::truncate()
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            ASSERT(m_handle.get() != nullptr);
            close();
            std::filesystem::resize_file(m_path, 0);
            open();
        }

        const std::vector<ThreadPool::ThreadPoolSpec>& ThreadPool::specs()
        {
            static const std::vector<ThreadPoolSpec> s_specs = []() {
                std::vector<ThreadPoolSpec> s_paths;

                for (auto&& specJson : cfg::g_config["ext"]["thread_pools"])
                {
                    auto& spec = s_paths.emplace_back();
                    specJson["threads"].get_to(spec.numThreads);
                    for (auto&& path : specJson["paths"])
                    {
                        spec.paths.emplace_back(path.get<std::string>());
                    }
                }

                return s_paths;
            }();

            return s_specs;
        }

        ThreadPool& ThreadPool::instance()
        {
            static ThreadPool s_instance(cfg::g_config["ext"]["default_thread_pool"]["threads"].get<std::size_t>());
            return s_instance;
        }

        ThreadPool& ThreadPool::instance(const std::filesystem::path& path)
        {
            static std::vector<std::unique_ptr<ThreadPool>> s_instances = []() {
                std::vector<std::unique_ptr<ThreadPool>> s_instances;

                for (auto&& spec : specs())
                {
                    Logger::instance().logInfo(": Creating thread pool for paths: ");
                    for (auto&& path : spec.paths)
                    {
                        Logger::instance().logInfo(":     ", path);
                    }
                    s_instances.emplace_back(new ThreadPool(spec.numThreads));
                }

                return s_instances;
            }();

            const std::size_t i = poolIndexForPath(path);
            if (i == -1)
            {
                return instance();
            }

            return *s_instances[i];
        }

        [[nodiscard]] std::future<std::size_t> ThreadPool::scheduleRead(std::shared_ptr<FileBase> file, std::byte* buffer, std::size_t offset, std::size_t elementSize, std::size_t count)
        {
            Job job{
                JobType::Read,
                std::move(file),
                buffer,
                {},
                offset,
                elementSize,
                count
            };
            std::future<std::size_t> future = job.promise.get_future();

            std::unique_lock<std::mutex> lock(m_mutex);

            m_jobQueue.emplace(std::move(job));
            if (m_jobQueue.size() == 1)
            {
                lock.unlock();
                m_jobQueueNotEmpty.notify_one();
            }

            return future;
        }

        [[nodiscard]] std::future<std::size_t> ThreadPool::scheduleAppend(std::shared_ptr<FileBase> file, const std::byte* buffer, std::size_t elementSize, std::size_t count)
        {
            Job job{
                JobType::Append,
                std::move(file),
                const_cast<std::byte*>(buffer),
                {},
                {},
                elementSize,
                count
            };
            std::future<std::size_t> future = job.promise.get_future();

            std::unique_lock<std::mutex> lock(m_mutex);

            m_jobQueue.emplace(std::move(job));
            if (m_jobQueue.size() == 1)
            {
                lock.unlock();
                m_jobQueueNotEmpty.notify_one();
            }

            return future;
        }

        ThreadPool::~ThreadPool()
        {
            m_done.store(true);
            m_jobQueueNotEmpty.notify_one();

            for (auto& thread : m_threads)
            {
                if (thread.joinable())
                {
                    thread.join();
                }
            }
        }

        ThreadPool::ThreadPool(std::size_t numThreads) :
            m_done(false)
        {
            Logger::instance().logInfo(": Creating thread pool with ", numThreads, " threads.");
            m_threads.reserve(numThreads);
            for (std::size_t i = 0; i < numThreads; ++i)
            {
                m_threads.emplace_back([this]() { worker(); });
            }
        }

        std::size_t ThreadPool::poolIndexForPath(const std::filesystem::path& path)
        {
            auto absolute = std::filesystem::canonical(path);
            const auto& poolSpecs = specs();
            for (std::size_t i = 0; i < poolSpecs.size(); ++i)
            {
                auto&& spec = poolSpecs[i];
                for (const auto& path : spec.paths)
                {
                    auto originalPath = absolute;
                    for (;;)
                    {
                        if (path == originalPath)
                        {
                            return i;
                        }

                        auto parent = originalPath.parent_path();
                        if (parent == originalPath)
                        {
                            break;
                        }
                        originalPath = std::move(parent);
                    }
                }
            }

            return -1;
        }

        void ThreadPool::worker()
        {
            for (;;)
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_jobQueueNotEmpty.wait(lock, [this]() { return !m_jobQueue.empty() || m_done.load(); });
                if (m_jobQueue.empty())
                {
                    lock.unlock();
                    m_jobQueueNotEmpty.notify_one();
                    return;
                }

                Job job = std::move(m_jobQueue.front());
                m_jobQueue.pop();
                lock.unlock();

                if (job.type == JobType::Read)
                {
                    const std::size_t r = job.file->read(job.buffer, job.offset, job.elementSize, job.count);
                    job.promise.set_value(r);
                }
                else // job.type == JobType::Append
                {
                    const std::size_t r = job.file->append(job.buffer, job.elementSize, job.count);
                    job.promise.set_value(r);
                }
            }
        }
    }

    ImmutableBinaryFile::ImmutableBinaryFile(std::filesystem::path path) :
        m_file(std::make_shared<detail::File>(std::move(path), m_openmode)),
        m_threadPool(&detail::ThreadPool::instance(m_file->path())),
        m_size(m_file->size())
    {
    }

    ImmutableBinaryFile::ImmutableBinaryFile(Pooled, std::filesystem::path path) :
        m_file(std::make_shared<detail::PooledFile>(std::move(path), m_openmode)),
        m_threadPool(&detail::ThreadPool::instance(m_file->path())),
        m_size(m_file->size())
    {
    }

    [[nodiscard]] bool operator==(const ImmutableBinaryFile& lhs, const ImmutableBinaryFile& rhs) noexcept
    {
        return lhs.m_file == rhs.m_file;
    }

    [[nodiscard]] bool ImmutableBinaryFile::isOpen() const
    {
        return m_file->isOpen();
    }

    [[nodiscard]] const std::filesystem::path& ImmutableBinaryFile::path() const
    {
        return m_file->path();
    }

    [[nodiscard]] std::string ImmutableBinaryFile::openmode() const
    {
        return m_openmode;
    }

    [[nodiscard]] std::size_t ImmutableBinaryFile::read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const
    {
        return m_file->read(destination, offset, elementSize, count);
    }

    [[nodiscard]] std::future<std::size_t> ImmutableBinaryFile::read(Async, std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const
    {
        return m_threadPool->scheduleRead(m_file, destination, offset, elementSize, count);
    }

    [[nodiscard]] std::size_t ImmutableBinaryFile::size() const
    {
        return m_size;
    }

    BinaryOutputFile::BinaryOutputFile(std::filesystem::path path, OutputMode mode) :
        m_file(std::make_shared<detail::File>(std::move(path), mode == OutputMode::Append ? m_openmodeAppend : m_openmodeTruncate)),
        m_threadPool(&detail::ThreadPool::instance(m_file->path()))
    {
    }

    BinaryOutputFile::BinaryOutputFile(Pooled, std::filesystem::path path, OutputMode mode) :
        m_file(std::make_shared<detail::PooledFile>(std::move(path), mode == OutputMode::Append ? m_openmodeAppend : m_openmodeTruncate)),
        m_threadPool(&detail::ThreadPool::instance(m_file->path()))
    {
    }

    BinaryOutputFile::~BinaryOutputFile()
    {

    }

    [[nodiscard]] bool BinaryOutputFile::isOpen() const
    {
        return m_file->isOpen();
    }

    [[nodiscard]] const std::filesystem::path& BinaryOutputFile::path() const
    {
        return m_file->path();
    }

    [[nodiscard]] std::string BinaryOutputFile::openmode() const
    {
        return m_file->openmode();
    }

    [[nodiscard]] std::size_t BinaryOutputFile::append(const std::byte* source, std::size_t elementSize, std::size_t count) const
    {
        return m_file->append(source, elementSize, count);
    }

    [[nodiscard]] std::future<std::size_t> BinaryOutputFile::append(Async, const std::byte* destination, std::size_t elementSize, std::size_t count) const
    {
        return m_threadPool->scheduleAppend(m_file, destination, elementSize, count);
    }

    // reopens the file in readonly mode
    [[nodiscard]] ImmutableBinaryFile BinaryOutputFile::seal()
    {
        flush();

        if (m_file->isPooled())
        {
            ImmutableBinaryFile f(Pooled{}, m_file->path());
            m_file.reset();
            return f;
        }
        else
        {
            ImmutableBinaryFile f(m_file->path());
            m_file.reset();
            return f;
        }
    }

    void BinaryOutputFile::flush()
    {
        m_file->flush();
    }

    ObservableBinaryOutputFile::ObservableBinaryOutputFile(ObservableBinaryOutputFile::CallbackType callback, std::filesystem::path path, OutputMode mode) :
        BinaryOutputFile(std::move(path), mode),
        m_callback(std::move(callback))
    {
    }

    ObservableBinaryOutputFile::ObservableBinaryOutputFile(Pooled, ObservableBinaryOutputFile::CallbackType callback, std::filesystem::path path, OutputMode mode) :
        BinaryOutputFile(std::move(path), mode),
        m_callback(std::move(callback))
    {
    }

    [[nodiscard]] std::size_t ObservableBinaryOutputFile::append(const std::byte* source, std::size_t elementSize, std::size_t count) const
    {
        m_callback(source, elementSize, count);
        return BinaryOutputFile::append(source, elementSize, count);
    }

    [[nodiscard]] std::future<std::size_t> ObservableBinaryOutputFile::append(Async, const std::byte* source, std::size_t elementSize, std::size_t count) const
    {
        m_callback(source, elementSize, count);
        return BinaryOutputFile::append(Async{}, source, elementSize, count);
    }

    BinaryInputOutputFile::BinaryInputOutputFile(std::filesystem::path path, OutputMode mode) :
        m_file(std::make_shared<detail::File>(std::move(path), mode == OutputMode::Append ? m_openmodeAppend : m_openmodeTruncate)),
        m_threadPool(&detail::ThreadPool::instance(m_file->path()))
    {
    }

    BinaryInputOutputFile::BinaryInputOutputFile(Pooled, std::filesystem::path path, OutputMode mode) :
        m_file(std::make_shared<detail::PooledFile>(std::move(path), mode == OutputMode::Append ? m_openmodeAppend : m_openmodeTruncate)),
        m_threadPool(&detail::ThreadPool::instance(m_file->path()))
    {
    }

    [[nodiscard]] bool operator==(const BinaryInputOutputFile& lhs, const BinaryInputOutputFile& rhs) noexcept
    {
        return &lhs == &rhs;
    }

    [[nodiscard]] bool BinaryInputOutputFile::isOpen() const
    {
        return m_file->isOpen();
    }

    [[nodiscard]] const std::filesystem::path& BinaryInputOutputFile::path() const
    {
        return m_file->path();
    }

    [[nodiscard]] std::string BinaryInputOutputFile::openmode() const
    {
        return m_file->openmode();
    }

    [[nodiscard]] std::size_t BinaryInputOutputFile::read(std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const
    {
        return m_file->read(destination, offset, elementSize, count);
    }

    [[nodiscard]] std::future<std::size_t> BinaryInputOutputFile::read(Async, std::byte* destination, std::size_t offset, std::size_t elementSize, std::size_t count) const
    {
        return m_threadPool->scheduleRead(m_file, destination, offset, elementSize, count);
    }

    [[nodiscard]] std::size_t BinaryInputOutputFile::size() const
    {
        return m_file->size();
    }

    [[nodiscard]] std::size_t BinaryInputOutputFile::append(const std::byte* source, std::size_t elementSize, std::size_t count) const
    {
        return m_file->append(source, elementSize, count);
    }

    [[nodiscard]] std::future<std::size_t> BinaryInputOutputFile::append(Async, const std::byte* destination, std::size_t elementSize, std::size_t count) const
    {
        return m_threadPool->scheduleAppend(m_file, destination, elementSize, count);
    }

    void BinaryInputOutputFile::truncate()
    {
        m_file->truncate();
    }

    // reopens the file in readonly mode
    [[nodiscard]] ImmutableBinaryFile BinaryInputOutputFile::seal()
    {
        flush();
        ImmutableBinaryFile f(m_file->path());
        m_file.reset();
        return f;
    }

    void BinaryInputOutputFile::flush()
    {
        m_file->flush();
    }

    TemporaryPaths::TemporaryPaths(std::filesystem::path dir) :
        m_dir(std::move(dir))
    {
        std::filesystem::create_directories(m_dir);
    }

    [[nodiscard]] std::filesystem::path& TemporaryPaths::next()
    {
        return m_paths.emplace_back(uniquePath(m_dir));
    }

    TemporaryPaths::~TemporaryPaths()
    {
        for (auto& path : m_paths)
        {
            std::filesystem::remove(path);
        }
    }

    namespace detail::merge
    {
        const std::size_t maxOutputBufferSizeMultiplier = cfg::g_config["ext"]["merge"]["max_output_buffer_size_multiplier"].get<std::size_t>();
        const std::size_t maxNumMergedInputs = cfg::g_config["ext"]["merge"]["max_batch_size"].get<std::size_t>();

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
            const std::size_t numInputsPerPart = maxNumMergedInputs;
            std::size_t writes = 0;
            std::size_t offset = 0;
            std::size_t parts = 0;
            for (; offset + numInputsPerPart < numInputs; offset += numInputsPerPart)
            {
                writes += merge_assess_work(std::next(inSizesBegin, offset), std::next(inSizesBegin, offset + numInputsPerPart));
                parts += 1;
            }

            // we may have some inputs left if numInputs % numInputsPerPart != 0
            if (offset < numInputs)
            {
                if (parts + (numInputs - offset) > maxNumMergedInputs)
                {
                    writes += merge_assess_work(std::next(inSizesBegin, offset), inSizesEnd);
                }
            }

            return writes + std::accumulate(inSizesBegin, inSizesEnd, static_cast<std::size_t>(0));
        }
    }

    namespace detail::equal_range
    {
        const std::size_t maxSeqReadSize = cfg::g_config["ext"]["equal_range"]["max_random_read_size"].get<MemoryAmount>();

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
    }

    const std::size_t defaultIndexBuilderMemoryAmount = cfg::g_config["ext"]["index"]["builder_buffer_size"].get<MemoryAmount>();
}
