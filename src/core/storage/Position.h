#pragma once

#include <string>

namespace internal::storage {

class Position {
public:
	Position() = default;
	Position(const Position &) = default;

	Position& operator=(const Position &) = default;

	/*
	 * Intentionally implicit
	 */
	Position(long pos) : m_pos{pos}, m_isset{true} {}
	operator long() { return m_pos; }

	static auto poison() { return Position(); }

	[[nodiscard]] auto get() const noexcept { return m_pos; }
	[[nodiscard]] bool is_set() const noexcept { return m_isset; }

	void set(long pos) noexcept {
		m_pos = pos;
		m_isset = true;
	}

	bool operator==(const Position &rhs) const noexcept { return m_pos == rhs.m_pos; }
	bool operator!=(const Position &rhs) const noexcept { return m_pos != rhs.m_pos; }
	bool operator==(long rhs) const noexcept { return m_pos == rhs; }

	[[nodiscard]] std::string str() const noexcept { return std::to_string(m_pos); }

private:
	long m_pos{0x41CEBEEF};
	bool m_isset{false};
};

}// namespace internal::storage