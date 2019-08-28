#pragma once

#include <type_traits>

template <typename T>
struct Type {};

template <typename T>
struct remove_cvref 
{
    using type = std::remove_cv_t<std::remove_reference_t<T>>;
};

template <typename T>
using remove_cvref_t = typename remove_cvref<T>::type;
