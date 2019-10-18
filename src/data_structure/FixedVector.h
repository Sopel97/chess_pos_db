#pragma once

#include <iterator>
#include <type_traits>

#include "util/Assert.h"

template <typename T, std::size_t CapacityV>
struct FixedVector
{
private:
    using StorageType = typename std::aligned_storage<sizeof(T), alignof(T)>::type;

public:
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = int;
    using reference = T &;
    using const_reference = const T &;
    using pointer = T *;
    using const_pointer = const T*;
    using iterator = T *;
    using const_iterator = const T*;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    FixedVector() :
        m_size(0)
    {
    }

    FixedVector(size_type s) :
        m_size(s)
    {
        ASSERT(s <= CapacityV);

        for (size_type i = 0; i < s; ++i)
        {
            new (&(m_data[i])) T{};
        }
    }

    FixedVector(size_type s, const T& v) :
        m_size(s)
    {
        ASSERT(s <= CapacityV);

        for (size_type i = 0; i < s; ++i)
        {
            new (&(m_data[i])) T(v);
        }
    }

    FixedVector(const FixedVector& other) :
        m_size(other.m_size)
    {
        for (size_type i = 0; i < m_size; ++i)
        {
            new (&(m_data[i])) T(other[i]);
        }
    }

    FixedVector(FixedVector&& other) noexcept :
        m_size(other.m_size)
    {
        for (size_type i = 0; i < m_size; ++i)
        {
            new (&(m_data[i])) T(std::move(other[i]));
        }
    }

    ~FixedVector()
    {
        clear();
    }

    FixedVector& operator=(const FixedVector& other)
    {
        size_type i = 0;
        for (; i < m_size && i < other.m_size; ++i)
        {
            (*this)[i] = other[i];
        }

        for (; i < m_size; ++i)
        {
            (*this)[i].~T();
        }

        for (; i < other.m_size; ++i)
        {
            new (&(m_data[i])) T(other[i]);
        }

        m_size = other.m_size;

        return *this;
    }

    FixedVector& operator=(FixedVector&& other) noexcept
    {
        size_type i = 0;
        for (; i < m_size && i < other.m_size; ++i)
        {
            (*this)[i] = std::move(other[i]);
        }

        for (; i < m_size; ++i)
        {
            (*this)[i].~T();
        }

        for (; i < other.m_size; ++i)
        {
            new (&(m_data[i])) T(std::move(other[i]));
        }

        m_size = other.m_size;

        return *this;
    }

    void resize(size_type newSize)
    {
        ASSERT(newSize <= CapacityV);

        size_type i = newSize < m_size ? newSize : m_size;

        for (; i < m_size; ++i)
        {
            (*this)[i].~T();
        }

        for (; i < newSize; ++i)
        {
            new (&(m_data[i])) T;
        }

        m_size = newSize;
    }

    [[nodiscard]] pointer data()
    {
        return reinterpret_cast<T*>(&(m_data[0]));
    }

    [[nodiscard]] const_pointer data() const
    {
        return reinterpret_cast<const T*>(&(m_data[0]));
    }

    [[nodiscard]] reference operator[](size_type i)
    {
        ASSERT(i < m_size);

        return (data()[i]);
    }

    [[nodiscard]] const_reference operator[](size_type i) const
    {
        ASSERT(i < m_size);

        return (data()[i]);
    }

    [[nodiscard]] reference at(size_type i)
    {
        if (i >= m_size)
        {
            throw std::out_of_range("");
        }

        return (data()[i]);
    }

    [[nodiscard]] const_reference at(size_type i) const
    {
        if (i >= m_size)
        {
            throw std::out_of_range("");
        }

        return (data()[i]);
    }

    [[nodiscard]] reference front()
    {
        ASSERT(m_size > 0);

        return (*this)[0];
    }

    [[nodiscard]] const_reference front() const
    {
        ASSERT(m_size > 0);

        return (*this)[0];
    }

    [[nodiscard]] reference back()
    {
        ASSERT(m_size > 0);

        return (*this)[m_size - 1];
    }

    [[nodiscard]] const_reference back() const
    {
        ASSERT(m_size > 0);

        return (*this)[m_size - 1];
    }

    [[nodiscard]] iterator begin()
    {
        return data();
    }

    [[nodiscard]] iterator end()
    {
        return data() + m_size;
    }

    [[nodiscard]] const_iterator begin() const
    {
        return data();
    }

    [[nodiscard]] const_iterator end() const
    {
        return data() + m_size;
    }

    [[nodiscard]] const_iterator cbegin() const
    {
        return data();
    }

    [[nodiscard]] const_iterator cend() const
    {
        return data() + m_size;
    }

    [[nodiscard]] reverse_iterator rbegin()
    {
        return reverse_iterator(end());
    }

    [[nodiscard]] reverse_iterator rend()
    {
        return reverse_iterator(begin());
    }

    [[nodiscard]] const_reverse_iterator rbegin() const
    {
        return const_reverse_iterator(end());
    }

    [[nodiscard]] const_reverse_iterator rend() const
    {
        return const_reverse_iterator(begin());
    }

    [[nodiscard]] const_reverse_iterator crbegin() const
    {
        return const_reverse_iterator(cend());
    }

    [[nodiscard]] const_reverse_iterator crend() const
    {
        return const_reverse_iterator(cbegin());
    }

    [[nodiscard]] bool empty() const
    {
        return m_size == 0;
    }

    [[nodiscard]] size_type size() const
    {
        return m_size;
    }

    [[nodiscard]] size_type capacity() const
    {
        return CapacityV;
    }

    void reserve(size_type n)
    {
        // do nothing
        // it's for interface compatibility
    }

    void clear()
    {
        for (int i = 0; i < m_size; ++i)
        {
            (*this)[i].~T();
        }
        m_size = 0;
    }

    template<typename... ArgsTs>
    reference emplace_back(ArgsTs&& ... args)
    {
        ASSERT(m_size < CapacityV);

        new (&(m_data[m_size])) T(std::forward<ArgsTs>(args)...);
        ++m_size;
        return back();
    }

    reference push_back(const T& value)
    {
        ASSERT(m_size < CapacityV);

        new (&(m_data[m_size])) T(value);
        ++m_size;
        return back();
    }

    reference push_back(T&& value)
    {
        ASSERT(m_size < CapacityV);

        new (&(m_data[m_size])) T(std::move(value));
        ++m_size;
        return back();
    }

    void pop_back()
    {
        ASSERT(m_size > 0);

        --m_size;
        (*this)[m_size].~T();
    }

private:
    size_type m_size;
    StorageType m_data[CapacityV];
};
