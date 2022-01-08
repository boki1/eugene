#pragma once

#include <functional>
#include <optional>

namespace internal {

template<typename T>
using optional_ref = std::optional<std::reference_wrapper<T>>;

template<typename T>
using optional_cref = std::optional<std::reference_wrapper<const T>>;

}// namespace internal
