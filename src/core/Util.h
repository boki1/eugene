#pragma once

#include <functional>
#include <limits>
#include <optional>

#include <fmt/format.h>

namespace internal {

/// std::optional for mutable references
template<typename T>
using optional_ref = std::optional<std::reference_wrapper<T>>;

/// std::optional for const references
template<typename T>
using optional_cref = std::optional<std::reference_wrapper<const T>>;

/// Same as std::lower_bound, but used for primitive types instead of collections.
/// Requires a comparator function since there is no other way to know what we are
/// looking for. Returns the biggest possible value which conforms to `predicate`.
///
/// The type T is required to be a type which has std::numeric_limits<> specialization.
template<typename T, typename F>
constexpr std::optional<T> binsearch_primitive(T low, T high, F fun) {
	std::optional<T> best;
	T curr;
	const T initial = low;

	while (low <= high) {
		curr = low + ((high - low) / 2);

		if (int rc = fun(curr, low, high); rc <= 0) {
			low = curr + 1;
			if (best.value_or(initial) < curr)
				best = curr;
		} else if (rc > 0) {
			high = curr - 1;
		} else
			return curr;
	}

	return best;
}

/// Splits a vector into two parts: leaving elements [0; pivot] and returning
/// a vector with (pivot; target.size())
template<typename T>
constexpr std::vector<T> break_at_index(std::vector<T> &target, uint32_t pivot) {
	assert(target.size() >= 2);

	std::vector<T> second;
	second.reserve(target.size() - pivot);

	std::move(target.begin() + pivot,
	          target.end(),
	          std::back_inserter(second));
	target.resize(pivot);
	return second;
}

/// Concatenate two std::vectors
template<typename T>
[[maybe_unused]] constexpr std::vector<T> vector_extend(std::vector<T> &vec1, const std::vector<T> &vec2) {
	vec1.reserve(vec1.size() + vec2.size());
	vec1.insert(vec1.end(), vec2.begin(), vec2.end());
	return vec1;
};

}// namespace internal
