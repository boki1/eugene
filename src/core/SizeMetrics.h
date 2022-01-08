#pragma once

namespace internal::storage {

static constexpr unsigned long long operator""_MB(const unsigned long long x) { return x * 1000 * 1000; }

static constexpr unsigned long long operator""_KB(const unsigned long long x) { return x * 1000; }

static constexpr unsigned long long operator""_B(const unsigned long long x) { return x; }

}// namespace internal::storage
