#pragma once

#include <nop/serializer.h>
#include <nop/structure.h>

#include <fmt/format.h>
#include <fmt/core.h>

#include <functional>
#include <string>

namespace internal::storage {

class Position {
	friend std::hash<Position>;

public:
	constexpr Position() = default;
	constexpr Position(const Position &) = default;

	constexpr Position &operator=(const Position &) = default;

	/*
	 * Intentionally implicit
	 */
	constexpr Position(int pos) : m_pos{pos}, m_isset{true} {}
	constexpr operator int() { return m_pos; }

	constexpr static auto poison() { return Position(); }

	[[nodiscard]] constexpr auto get() const noexcept { return m_pos; }
	[[nodiscard]] constexpr bool is_set() const noexcept { return m_isset; }

	constexpr void set(long pos) noexcept {
		m_pos = pos;
		m_isset = true;
	}

	constexpr bool operator==(const Position &rhs) const noexcept { return m_pos == rhs.m_pos; }
	constexpr bool operator!=(const Position &rhs) const noexcept { return m_pos != rhs.m_pos; }
	constexpr bool operator==(long rhs) const noexcept { return m_pos == rhs; }

	friend std::ostream &operator<<(std::ostream &os, const Position &pos) {
		os << "Position { .pos = " << pos.get() << ", .is_set = " << std::boolalpha << pos.is_set() << " }";
		return os;
	}

private:
	long m_pos{0x41CEBEEF};
	bool m_isset{false};

	NOP_STRUCTURE(Position, m_pos, m_isset);
};

}// namespace internal::storage

template<>
struct fmt::formatter<internal::storage::Position> {
	template<typename ParseContext>
	constexpr auto parse(ParseContext &ctx) {
		return ctx.begin();
	}

	template<typename FormatContext>
	auto format(const internal::storage::Position &p, FormatContext &ctx) {
		return fmt::format_to(ctx.out(), "Position {{ .pos='{:#08X}', .is_set={} }}", p.get(), p.is_set());
	}
};

/*
 * Implement std::hash<> for Position
 */
namespace std {
template<>
struct hash<internal::storage::Position> {
	size_t operator()(const internal::storage::Position pos) const noexcept {
		return std::hash<decltype(pos.m_pos)>{}(pos.m_pos);
	}
};
}// namespace std
