#pragma once

#include <functional>
#include <memory>

namespace util
{
    template <typename T>
    struct LazyCached
    {
        LazyCached() = delete;

        LazyCached(std::function<T()>&& factory) :
            m_value{},
            m_factory(std::move(factory))
        {
        }

        LazyCached(const T& value) :
            m_value(std::make_unique<T>(value)),
            m_factory{}
        {
        }

        LazyCached(T&& value) :
            m_value(std::make_unique<T>(std::move(value))),
            m_factory{}
        {
        }

        LazyCached(const LazyCached& other) = delete;
        LazyCached(LazyCached&& other) noexcept = default;

        LazyCached& operator=(const LazyCached& other) = delete;
        LazyCached& operator=(LazyCached&& other) noexcept = default;

        const T& operator*() const
        {
            ensurePresent();
            return *m_value;
        }

        T& operator*()
        {
            ensurePresent();
            return *m_value;
        }

        const T* operator->() const
        {
            ensurePresent();
            return m_value.get();
        }

        T* operator->()
        {
            ensurePresent();
            return m_value.get();
        }

    private:
        mutable std::unique_ptr<T> m_value;
        std::function<T()> m_factory;

        void ensurePresent() const
        {
            if (m_value == nullptr)
            {
                m_value = std::make_unique<T>(m_factory());
            }
        }
    };
}
