#pragma once

#include <type_traits>

namespace util::meta
{
    template <typename T>
    struct Type {};

    template <typename... Ts>
    struct TypeList {};

    template <typename T, typename... Ts>
    struct AreAllTheSame
    {
        static constexpr bool value = (std::is_same_v<T, Ts> && ...);
    };

    template <typename T, typename... Ts>
    struct IsContained
    {
        static constexpr bool value = (std::is_same_v<T, Ts> || ...);
    };

    template <typename T, typename... Ts>
    struct FirstInPack
    {
        using type = T;
    };
}

template <typename T>
struct remove_cvref
{
    using type = std::remove_cv_t<std::remove_reference_t<T>>;
};

template <typename T>
using remove_cvref_t = typename remove_cvref<T>::type;
