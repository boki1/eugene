#pragma once

#include <nop/base/serializer.h>
#include <nop/utility/buffer_reader.h>
#include <nop/utility/buffer_writer.h>
#include <sstream>
#include <utility>
#include <vector>

#include <nop/serializer.h>
#include <nop/status.h>
#include <nop/structure.h>
#include <nop/types/variant.h>
#include <nop/utility/die.h>
#include <nop/utility/stream_reader.h>
#include <nop/utility/stream_writer.h>

#include <fmt/core.h>

#include <core/Config.h>
#include <core/Util.h>
#include <core/storage/Pager.h>

namespace internal::storage {

struct BadIndVector : std::runtime_error {
	explicit BadIndVector(std::string_view msg) : std::runtime_error{fmt::format("Eugene: Bad indirection vector {}", msg.data())} {}
};

struct Slot {
	storage::Position pos = 0;
	std::size_t size = 0;

	NOP_STRUCTURE(Slot, pos, size);
};

using SlotId = std::size_t;

template<EugeneConfig Config>
class IndirectionVector {
	using Val = typename Config::Val;
	using RealVal = typename Config::RealVal;
	using PagerType = typename Config::PagerType;
public:
	enum class ActionOnConstruction { Load,
		                          DoNotLoad };

	template<typename... Args>
	explicit IndirectionVector(std::string_view identifier = "/tmp/eu-btree", ActionOnConstruction action = ActionOnConstruction::Load, Args &&...args)
	    : m_identifier{fmt::format("{}-indvector", identifier)}, m_slot_pager{new PagerType(fmt::format("{}-header", m_identifier), std::forward<Args>(args)...)} {
		// clang-format off
		switch (action) {
			break; case ActionOnConstruction::Load: load();
			break; case ActionOnConstruction::DoNotLoad: DO_NOTHING;
		}
		// clang-format on
	}

	IndirectionVector(const IndirectionVector &) = default;
	IndirectionVector &operator=(const IndirectionVector &) = default;

	///
	/// Operations API
	///

	/// Load indirection vector from persistent storage
	void load() {
		if constexpr (requires { m_slot_pager->load(); })
			m_slot_pager->load();
		nop::Deserializer<nop::StreamReader<std::ifstream>> deserializer{header_name().data()};
		auto backup = *this;
		if (!deserializer.Read(this)) {
			*this = backup;//< In case of erroneous read, recover saved version.
			throw BadRead(" - deserializer fails reading indirection vector");
		}
	}

	/// Store indirection vector from persistent storage
	void save() {
		if constexpr (requires { m_slot_pager->save(); })
			m_slot_pager->save();
		nop::Serializer<nop::StreamWriter<std::ofstream>> serializer{header_name().data(), std::ios::trunc};
		if (!serializer.Write(*this))
			throw BadWrite(" - serializer fails writing indirection vector");
	}

	/// Set dyn value in a slot
	SlotId set_to_slot(const RealVal &val, const auto sz) {
		const auto val_pos = m_slot_pager->alloc_inner(sz);

		fmt::print("val = '{}' (sz = {}, @slot_id = {})\n", val, sz, val_pos);

		nop::Serializer<nop::StreamWriter<std::stringstream>> serializer;
		if (!serializer.Write(val))
			throw BadWrite(fmt::format(" - serializer fails writing val_data for slot", val));
		const auto str = serializer.writer().stream().str();
		std::vector<uint8_t> val_data{str.cbegin(), str.cend()};

		fmt::print("    Serialized form = '{}'\n", str);
		nop::Deserializer<nop::StreamReader<std::stringstream>> deserializer{str};
		RealVal _rv;
		deserializer.Read(&_rv);
		fmt::print("    tryout deserialization yields '{}'\n", _rv);

		m_slot_pager->place_inner(val_pos, val_data);
		m_slots.emplace_back(val_pos, sz);
		return static_cast<SlotId>(m_slots.size() - 1);
	}

	/// Update dyn value in an existing slot
	void replace_in_slot(const SlotId n, const RealVal &new_val, const auto new_val_sz) {
		if (n >= m_slots.size())
			throw BadRead(fmt::format(" - trying to access slot = {} but out of bounds", n));
		const auto &slot = m_slots.at(n);

		auto new_val_pos = m_slot_pager->alloc_inner(new_val_sz);
		m_slot_pager->free_inner(slot.pos, slot.size);

		fmt::print("new_val = '{}' (sz = {}, @slot_id = {})\n", new_val, new_val_sz, n);

		std::vector<uint8_t> new_val_data;
		new_val_data.resize(new_val_sz);
		nop::Serializer<nop::BufferWriter> serializer{new_val_data.data(), new_val_sz};
		if (!serializer.Write(new_val))
			throw BadWrite(fmt::format(" - serializer fails updating with new_val for slot", n));

		m_slot_pager->place_inner(new_val_pos, new_val_data);

		m_slots[n] = Slot{.pos=new_val_pos, .size=new_val_sz};
	}

	/// Free up a slot
	void remove_slot(const SlotId n) {
		if (n >= m_slots.size())
			throw BadRead(fmt::format(" - trying to access slot = {} but out of bounds", n));
		const auto &slot = m_slots.at(n);

		m_slot_pager->free_inner(slot.pos, slot.size);
		m_slots.erase(m_slots.cbegin() + n);
	}

	/// Read a dyn value from slot
	[[nodiscard]] RealVal get_from_slot(const SlotId n) {
		if (n >= m_slots.size())
			throw BadRead(fmt::format(" - trying to access slot = {} but out of bounds", n));
		const auto &slot = m_slots.at(n);
		auto val_data = m_slot_pager->get_inner(slot.pos, slot.size);

		RealVal val;
		nop::Deserializer<nop::BufferReader> deserializer{val_data.data(), val_data.size()};
		if (!deserializer.Read(&val))
			throw BadRead(fmt::format(" - deserializer fails reading val_data for slot", n));
		return val;
	}

	///
	/// Properties
	///

	[[nodiscard]] std::string_view header_name() const noexcept { return m_identifier; }

private:
	std::vector<Slot> m_slots;
	std::string m_identifier;
	std::shared_ptr<PagerType> m_slot_pager;

	NOP_STRUCTURE(IndirectionVector, m_slots, m_identifier);
};

}// namespace internal::storage
