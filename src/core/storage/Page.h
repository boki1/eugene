#pragma once

#include <array>
#include <cassert>
#include <concepts>
#include <fstream>
#include <optional>
#include <string_view>

#include <core/storage/Position.h>

namespace internal::storage {

class Pager;

//!
//! Single "addressable" unit used by the Pager
//!
class Page {
	static inline constexpr int32_t SIZE = 1 << 14;

	friend Pager;

public:
	explicit Page(std::array<uint8_t, Page::SIZE> &&data, bool used = true)
	    : m_data{std::move(data)}, m_used{used} {}

	Page(const Page &) = default;
	Page(Page &&) = default;

	Page &operator=(const Page &) noexcept = default;
	Page &operator=(Page &&) noexcept = default;

	using data_iterator = std::array<uint8_t, Page::SIZE>::iterator;
	using data_citerator = std::array<uint8_t, Page::SIZE>::const_iterator;

	static Page empty() noexcept {
		std::array<uint8_t, Page::SIZE> a{0};
		return Page{std::move(a), false};
	}

	void write(Position pos, int8_t d) noexcept {
		if (pos + 1 > (long) size())
			return;
		*(raw() + pos) = d;
		m_dirty = true;
	}

	void write(Position pos, data_citerator data_begin, data_citerator data_end) noexcept {
		auto numbytes = std::distance(data_begin, data_end);
		if (pos + numbytes > size())
			return;
		for (unsigned i = 0; i < numbytes; ++i)
			*(raw() + pos + i) = *(data_begin + i);
		m_dirty = true;
	}

	std::optional<uint8_t> read(Position pos) noexcept {
		if (pos + 1 > (long) size())
			return {};
		return *(raw() + pos);
	}

	std::optional<std::pair<data_citerator, data_citerator>> read(Position pos, uint32_t numbytes) noexcept {
		if (pos + numbytes > size())
			return std::nullopt;
		auto it_begin = m_data.cbegin() + pos;
		auto it_end = m_data.cbegin() + pos + numbytes;
		return std::make_pair(it_begin, it_end);
	}

	[[nodiscard]] auto operator<=>(const Page &) const = default;

public:
	[[nodiscard]] uint8_t *raw() noexcept { return m_data.data(); }

	[[nodiscard]] const uint8_t *raw() const noexcept { return m_data.data(); }

	[[nodiscard]] std::array<uint8_t, Page::SIZE> &get() noexcept { return m_data; }

	[[nodiscard]] bool used() const noexcept { return m_used; }

	[[nodiscard]] bool dirty() const noexcept { return m_dirty; }

	[[nodiscard]] static constexpr uint32_t size() noexcept { return Page::SIZE; }

private:
	std::array<uint8_t, Page::SIZE> m_data;
	bool m_used = false;
	bool m_dirty = false;
};

//!
//! "Storage driver"
//! Responsible for managing low-level storage access
//! a.k.a the one doing the actual read()'s and write()'s
class Pager {
public:
	explicit Pager(std::string_view fname)
	    : m_cursor{0},
	      m_file{fname.data(), std::ios::in | std::ios::out | std::ios::trunc},
	      m_filename{fname} {
		assert(m_file);
	}

	Position sync(const Page &page, std::optional<Position> pos_opt = {}) noexcept {
		Position sync_pos = pos_opt.value_or(m_cursor);
		m_file.seekp((long) sync_pos);
		m_file.write(reinterpret_cast<const char *>(page.raw()), Page::size());
		if (!pos_opt)
			m_cursor += Page::size();
		return sync_pos;
	}

	Page &fetch(Page &page, Position pos) noexcept {
		m_file.seekp((long) pos);
		m_file.read(reinterpret_cast<char *>(&page), Page::size());
		return page;
	}

	Position alloc() noexcept {
		auto tmp = m_cursor;
		m_cursor += Page::size();
		return tmp;
	}

private:
	long m_cursor;
	std::fstream m_file;
	std::string_view m_filename;
};

}// namespace internal::storage
