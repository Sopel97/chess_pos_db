#pragma once

#include "util/Assert.h"

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

namespace detail
{
    // https://stackoverflow.com/a/17074810/3763139

    template <typename T, typename Compare>
    static std::vector<std::size_t> sort_permutation(
        const std::vector<T>& vec,
        Compare& compare)
    {
        std::vector<std::size_t> p(vec.size());
        std::iota(p.begin(), p.end(), 0);
        std::sort(p.begin(), p.end(),
            [&](std::size_t i, std::size_t j) { return compare(vec[i], vec[j]); });
        return p;
    }

    template <typename T>
    static void apply_permutation_in_place(
        std::vector<T>& vec,
        const std::vector<std::size_t>& p)
    {
        std::vector<std::uint8_t> done(vec.size());
        for (std::size_t i = 0; i < vec.size(); ++i)
        {
            if (done[i])
            {
                continue;
            }
            done[i] = true;
            std::size_t prev_j = i;
            std::size_t j = p[i];
            while (i != j)
            {
                std::swap(vec[prev_j], vec[j]);
                done[j] = true;
                prev_j = j;
                j = p[j];
            }
        }
    }
}

template <typename T, typename CompareT = std::less<>>
[[nodiscard]] Unsorter reversibleSort(std::vector<T> & values, CompareT cmp = CompareT{})
{
    auto perm = detail::sort_permutation(values, cmp);

    detail::apply_permutation_in_place(values, perm);

    return Unsorter(std::move(perm));
}

template <typename T, typename U, typename CompareT = std::less<>>
[[nodiscard]] Unsorter reversibleZipSort(std::vector<T>& keys, std::vector<U>& values, CompareT cmp = CompareT{})
{
    auto perm = detail::sort_permutation(keys, cmp);

    detail::apply_permutation_in_place(keys, perm);
    detail::apply_permutation_in_place(values, perm);

    return Unsorter(std::move(perm));
}
