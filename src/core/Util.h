#pragma once

#include <functional>
#include <limits>
#include <optional>

#include <fmt/format.h>

namespace internal {

template<typename T>
using optional_ref = std::optional<std::reference_wrapper<T>>;

template<typename T>
using optional_cref = std::optional<std::reference_wrapper<const T>>;

/*
 * Same as std::lower_bound, but used for primitive types instead of collections.
 * Requires a comparator function since there is no other way to know what we are
 * looking for. Returns the biggest possible value which conforms to `predicate`.
 *
 * The type T is required to be a type which has std::numeric_limits<> specialization.
 */
template<typename T, typename F>
constexpr std::optional<T> binsearch_primitive(T low, T high, F fun) {
	std::optional<T> best;
	T curr;

	while (low <= high) {
		curr = low + ((high - low) / 2);

		if (int rc = fun(curr, low, high); rc <= 0) {
			low = curr + 1;
			best = curr;
		} else if (rc > 0) {
			high = curr - 1;
		} else
			return curr;
	}

	if (best) {
		assert(best.value() > std::numeric_limits<T>::min());
		return std::make_optional(best.value());
	}

	return {};
}

}// namespace internal
