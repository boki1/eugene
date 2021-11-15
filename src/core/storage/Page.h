#pragma once

#include <array>
#include <fstream>
#include <optional>
#include <span>
#include <string_view>
#include <cassert>

#include <core/storage/Position.h>

namespace internal::storage {

static constexpr int32_t PAGE_SIZE = 1 << 14;

class Page {
public:
	explicit Page(std::array<uint8_t, PAGE_SIZE> &&data)
	    : m_data{std::move(data)} {}

	void write_byte_at(Position pos, int32_t data) noexcept {
		if (pos > (long) (PAGE_SIZE - sizeof(uint32_t)))
			return;
		for (int i = 0; i < 4; ++i)
			m_data[pos + i] = (data | (0xff << i));
	}

	std::optional<uint32_t> read_byte_from(Position pos) noexcept {
		if (pos > (long) (PAGE_SIZE - sizeof(uint32_t)))
			return {};

		uint8_t *bytes = m_data.data() + pos;
		return bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24);
	}

	std::optional<std::span<uint8_t>> read_bytes_from(Position pos, uint32_t size) noexcept {
		if (pos > (long) (PAGE_SIZE - sizeof(uint32_t) * size))
			return {};
		return std::span{m_data.begin() + pos, size};
	}

	[[nodiscard]] const uint8_t *data() const noexcept { return m_data.data(); }

	[[nodiscard]] auto operator<=>(const Page&) const = default;

private:
	std::array<uint8_t, PAGE_SIZE> m_data;
};

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
		m_file.write(reinterpret_cast<const char *>(page.data()), PAGE_SIZE);
		if (!pos_opt)
			m_cursor += PAGE_SIZE;
		return sync_pos;
	}

	Page &fetch(Page &page, Position pos) noexcept {
		m_file.seekp((long) pos);
		m_file.read(reinterpret_cast<char *>(&page), PAGE_SIZE);
		return page;
	}

private:
	long m_cursor;
	std::fstream m_file;
	std::string_view m_filename;
};

}// namespace internal::storage
