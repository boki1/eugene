#pragma once

#include <nop/base/serializer.h>
#include <nop/utility/buffer_reader.h>
#include <nop/utility/buffer_writer.h>
#include <utility>
#include <vector>

#include <nop/base/vector.h>
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
	storage::Position pos;
	std::size_t size;
};

using SlotId = std::size_t;

template<EugeneConfig Config>
class IndirectionVector {
	using Val = typename Config::Val;
	using RealVal = typename Config::RealVal;

	using PagerType = typename Config::PagerType;

	static_assert(std::same_as<Val, SlotId>);
public:
	///
	/// Properties
	///

	[[nodiscard]] std::string_view header_name() const noexcept { return m_identifier; }

	///
	/// Operations API
	///

	/// Acquires a value based on its slot id
	[[nodiscard]] Val get_from_slot(const SlotId n) {
		if (n >= m_slots.size())
			throw BadRead(fmt::format(" - trying to access slot = {} but out of bounds", n));
		const auto &slot = m_slots.at(n);

		auto val_data = m_slot_pager.get_inner(slot.pos, slot.size);

		Val val;
		nop::Deserializer<nop::BufferReader> deserializer{val_data.data()};
		if (!deserializer.Read(&val))
			throw BadRead(fmt::format(" - deserializer fails reading val_data for slot", n));
		return val;
	}

	/// Allocates and sets new slot and returns its id
	Val set_to_slot(const RealVal &val, const auto sz = sizeof(Val)) {
		const auto val_pos = m_slot_pager.alloc_inner(sz);

		std::vector<uint8_t> val_data;
		val_data.reserve(sz);
		nop::Serializer<nop::BufferWriter> serializer{val_data.data()};
		if (!serializer.Write(&val))
			throw BadWrite(fmt::format(" - serializer fails writing val_data for slot", val));

		m_slot_pager.place_inner(val_pos, val_data);
		m_slots.emplace_back(Slot{
		        .pos = val_pos,
		        .size = sz});
		return m_slots.size() - 1;
	}

	/// Frees up allocated slot and associated inner space
	void remove_slot(const SlotId n) {
		if (n >= m_slots.size())
			throw BadRead(fmt::format(" - trying to access slot = {} but out of bounds", n));
		const auto &slot = m_slots.at(n);

		m_slot_pager.free_inner(slot.pos, slot.size);
		m_slots.erase(m_slots.cbegin() + n);
	}

	/// Replaces value of slot
	void replace_in_slot(const SlotId n, const Val &new_val, const auto new_val_sz = sizeof(Val)) {
		if (n >= m_slots.size())
			throw BadRead(fmt::format(" - trying to access slot = {} but out of bounds", n));
		const auto &slot = m_slots.at(n);

		auto new_val_pos = m_slot_pager.alloc_inner(new_val_sz);
		m_slot_pager.free_inner(slot.pos, slot.size);

		std::vector<uint8_t> new_val_data;
		new_val_data.reserve(new_val_sz);
		nop::Serializer<nop::BufferWriter> serializer{new_val_data.data()};
		if (!serializer.Write(&new_val))
			throw BadWrite(fmt::format(" - serializer fails updating with new_val for slot", n));

		m_slot_pager.place_inner(new_val_pos, new_val_data);

		m_slots[n] = Slot {
			.pos = new_val_pos,
			.size = new_val_sz
		};
	}

	void save() {
		if constexpr (requires { m_slot_pager->save(); }) {
			m_slot_pager.save();
		}

		nop::Serializer<nop::StreamWriter<std::ofstream>> serializer{header_name().data(), std::ios::trunc};
		if (!serializer.Write(*this))
			throw BadWrite(" - serializer fails writing indirection vector");
	}

	void load() {
		if constexpr (requires { m_slot_pager->load(); }) {
			m_slot_pager.load();
		}

		nop::Deserializer<nop::StreamReader<std::ifstream>> deserializer{header_name().data()};
		if (!deserializer.Read(this))
			throw BadRead(" - deserializer fails reading indirection vector");
	}

	enum class ActionOnConstruction { Load,
		                          DoNotLoad };

	template<typename... Args>
	explicit IndirectionVector(std::string_view identifier, ActionOnConstruction action = ActionOnConstruction::Load, Args &&...args)
	    : m_identifier{fmt::format("{}-indvector", identifier)}, m_slot_pager{fmt::format("{}-header", m_identifier).data(), std::forward<Args>(args)...} {
		// clang-format off
		switch (action) {
			break; case ActionOnConstruction::Load: load();
			break; case ActionOnConstruction::DoNotLoad: DO_NOTHING;
		}
		// clang-format on
	}

private:
	std::vector<Slot> m_slots;
	std::string_view m_identifier;
	PagerType m_slot_pager;

	NOP_STRUCTURE(IndirectionVector, m_identifier, m_slots);
};

}// namespace internal
