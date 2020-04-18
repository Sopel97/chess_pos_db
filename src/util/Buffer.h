#pragma once

#include "Assert.h"

#include <cstdint>
#include <memory>
#include <type_traits>

namespace util
{
    template <typename T>
    struct Buffer
    {
        static_assert(std::is_trivially_copyable_v<T>);

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
    struct DoubleBuffer
    {
        static_assert(std::is_trivially_copyable_v<T>);

        // NOTE: total buffer volume is 2*size
        DoubleBuffer(std::size_t size) :
            m_front(size),
            m_back(size)
        {
            ASSERT(size > 0u);
        }

        [[nodiscard]] T* back_data()
        {
            return m_back.data();
        }

        [[nodiscard]] const T* back_data() const
        {
            return m_back.data();
        }

        [[nodiscard]] T* data()
        {
            return m_front.data();
        }

        [[nodiscard]] const T* data() const
        {
            return m_front.data();
        }

        [[nodiscard]] const T& operator[](std::size_t i) const
        {
            return m_front[i];
        }

        [[nodiscard]] T& operator[](std::size_t i)
        {
            return m_front[i];
        }

        void swap()
        {
            std::swap(m_front, m_back);
        }

        [[nodiscard]] std::size_t size() const
        {
            return m_front.size();
        }

        [[nodiscard]] std::size_t size_bytes() const
        {
            return m_front.size_bytes();
        }

    private:
        // TODO: maybe do one joint allocation
        Buffer<T> m_front;
        Buffer<T> m_back;
    };
}
