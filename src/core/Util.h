#pragma once

#include <functional>
#include <limits>
#include <optional>
#include <random>

#include <nop/structure.h>

#include <cppcoro/generator.hpp>
#include <fmt/core.h>
#include <fmt/format.h>

namespace internal {

///
/// Used inside 'core/storage'
///

static constexpr unsigned long long operator""_MB(const unsigned long long x) { return x * (1 << 20); }

static constexpr unsigned long long operator""_KB(const unsigned long long x) { return x * (1 << 10); }

static constexpr unsigned long long operator""_B(const unsigned long long x) { return x; }

///
/// Used inside 'core'
///

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

template<typename T, typename V>
[[nodiscard]] constexpr bool collection_contains(const T &collection, V item) {
	return std::find(collection.cbegin(), collection.cend(), item) != collection.cend();
}

// Merge ranges based on fun
[[maybe_unused]] void merge_many(std::ranges::range auto self, std::ranges::range auto diff, auto fun) {
	auto self_begin = std::cbegin(self);
	auto diff_begin = std::cbegin(diff);
	while (self_begin < std::cend(self) && diff_begin < std::cend(diff)) {
		const auto use_self = *self_begin < *diff_begin;
		std::size_t idx = use_self
		        ? std::distance(std::cbegin(self), self_begin++)
		        : std::distance(std::cbegin(diff), diff_begin++);
		fun(use_self, idx);
	}
	while (diff_begin < std::cend(diff))
		fun(false, std::distance(std::cbegin(diff), diff_begin++));
	while (self_begin < std::cend(self))
		fun(true, std::distance(std::cbegin(self), self_begin++));
}

/// Pop and return the last element of a collection
template<typename T, typename Ts>
[[maybe_unused]] T consume_back(Ts &ts) {
	if constexpr (requires { ts.top(); }) {
		const auto t = ts.top();
		ts.pop();
		return t;
	} else if constexpr (requires { ts.back(); }) {
		const auto t = ts.back();
		ts.pop_back();
		return t;
	}
}

#define UNREACHABLE \
	abort();

#define UNIMPLEMENTED \
	abort();

#define DO_NOTHING \
	do {       \
	} while (0);

[[nodiscard]] constexpr auto round_upwards(auto a, auto b) {
	return (a / b) + (a % b > 0);
}

///
/// Used  primarily in unit tests
///

/// String with limit of 10 characters in size - small string.
class smallstr {
	static constexpr std::size_t SMALL_LIMIT = 10;
	char m_str[SMALL_LIMIT];

	NOP_STRUCTURE(smallstr, m_str);

public:
	constexpr smallstr() = default;

	smallstr(std::string &&s) {
		const auto bytes_to_copy = std::min(s.size(), SMALL_LIMIT);
		for (std::size_t i = 0; i < bytes_to_copy; ++i)
			m_str[i] = s.at(i);
	}

	constexpr auto operator<=>(const smallstr &) const noexcept = default;

	friend std::ostream &operator<<(std::ostream &os, const smallstr &str) {
		os << static_cast<const char *>(str.m_str);
		return os;
	}

	constexpr const char *str() const noexcept { return m_str; }

	static constexpr std::size_t small_limit() noexcept { return SMALL_LIMIT; }
};

/// Person class used for testing purposes.
struct person {
	smallstr name;
	int age{};
	smallstr email;

	auto operator<=>(const person &) const noexcept = default;

	NOP_STRUCTURE(person, name, age, email);
};

/// Random random_item generators
/// Return a random value of type <T>
template<typename T>
T random_item();

static int g_i = 0;

template<>
int random_item<int>() {
	return g_i++;
	//	static std::random_device dev;
	//	static std::mt19937 rng(dev());
	//	static std::uniform_int_distribution<std::mt19937::result_type> dist(1, 10000000);
	//	return dist(rng);
}

template<>
float random_item<float>() {
	return static_cast<float>(random_item<int>()) / static_cast<float>(random_item<int>());
}

template<>
bool random_item<bool>() {
	return random_item<int>() % 2 == 0;
}

template<>
std::string random_item<std::string>() {
	static std::random_device dev;
	static std::mt19937 rng(dev());
	static std::uniform_int_distribution<std::mt19937::result_type> dist10(1, 10);

	static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	static std::uniform_int_distribution<std::mt19937::result_type> dist_alphanum(0, sizeof(alphanum) - 1);

	auto len = dist10(rng);
	std::string str;
	str.reserve(len);
	for (auto i = 0ul; i < len; ++i)
		str += alphanum[dist_alphanum(rng)];
	return str;
}

template<>
smallstr random_item<smallstr>() {
	static std::random_device dev;
	static std::mt19937 rng(dev());
	static std::uniform_int_distribution<std::mt19937::result_type> dist10(1, 10);

	static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	static std::uniform_int_distribution<std::mt19937::result_type> dist_alphanum(0, sizeof(alphanum) - 1);

	std::string str;
	str.reserve(smallstr::small_limit());
	for (std::size_t i = 0; i < smallstr::small_limit(); ++i)
		str += alphanum[dist_alphanum(rng)];
	return smallstr{std::move(str)};
}

template<>
person random_item<person>() {
	return person{
	        .name = random_item<smallstr>(),
	        .age = random_item<int>(),
	        .email = random_item<smallstr>()};
}

template<typename T>
std::vector<T> n_random_items(const std::size_t n) {
	std::vector<T> vec;
	vec.reserve(n);
	for (auto i = 0ul; i < n; ++i)
		vec.push_back(random_item<T>());
	return vec;
}

}// namespace internal

/// Formatters should remain outside of 'internal' namespace.

template<>
struct fmt::formatter<internal::smallstr> {
	template<typename ParseContext>
	constexpr auto parse(ParseContext &ctx) {
		return ctx.begin();
	}

	template<typename FormatContext>
	auto format(const internal::smallstr &s, FormatContext &ctx) {
		return fmt::format_to(ctx.out(), "{}", std::string(s.str()));
	}
};

template<>
struct fmt::formatter<internal::person> {
	template<typename ParseContext>
	constexpr auto parse(ParseContext &ctx) {
		return ctx.begin();
	}

	template<typename FormatContext>
	auto format(const internal::person &p, FormatContext &ctx) {
		return fmt::format_to(ctx.out(), "person{{ .name='{}', .age={}, .email='{}' }}", p.name, p.age, p.email);
	}
};
