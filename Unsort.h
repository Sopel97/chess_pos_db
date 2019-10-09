#pragma once

#include "Assert.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <numeric>
#include <vector>

struct Unsorter
{
    explicit Unsorter(std::vector<std::size_t>&& originalIndices) :
        m_originalIndices(std::move(originalIndices))
    {
    }

    template <typename T>
    void operator()(std::vector<T>& values) const
    {
        const std::size_t size = values.size();

        ASSERT(size == m_originalIndices.size());

        std::vector<T> cpy(values.size());
        for (std::size_t i = 0; i < size; ++i)
        {
            cpy[m_originalIndices[i]] = std::move(values[i]);
        }

        values.swap(cpy);
    }

private:
    std::vector<std::size_t> m_originalIndices;
};

template <typename T, typename CompareT = std::less<>>
[[nodiscard]] Unsorter reversibleSort(std::vector<T> & values, CompareT cmp = CompareT{})
{
    const std::size_t size = values.size();
    std::vector<std::size_t> originalIndices(size);
    std::iota(originalIndices.begin(), originalIndices.end(), 0);

    std::sort(
        originalIndices.begin(),
        originalIndices.end(),
        [&values, cmp](std::size_t lhs, std::size_t rhs) {
            return cmp(values[lhs], values[rhs]);
        }
    );
    std::sort(values.begin(), values.end(), cmp);

    return Unsorter(std::move(originalIndices));
}
