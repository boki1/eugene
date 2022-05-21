#pragma once

#include <string_view>
#include <variant>

#include <core/server/detail/Storage.h>
#include <core/storage/btree/Btree.h>
#include <core/Config.h>

template<typename KEY, typename Value>
class Storage {
	struct ExampleAgentConfig : internal::Config {
		using Key = std::string;
		using RealVal = std::string;
		using Ref = std::string;
		static inline constexpr bool DYN_ENTRIES = true;
	};

protected:
	internal::storage::btree::Btree<ExampleAgentConfig> m_storage;
	using BtreeType = internal::storage::btree::Btree<ExampleAgentConfig>;

public:
	virtual void set(const KEY &key, const Value &value) {
		if (auto res = m_storage.insert(key, value);
			std::holds_alternative<typename BtreeType::InsertedNothing>(res)) {
			throw std::invalid_argument("Information already exists");
		}
	}

	virtual Value get(const KEY &key) {
		if (auto res = m_storage.get(key); res)
			return *res;
		throw std::invalid_argument("Can't find such key");
	}

	virtual void remove(const KEY &key) {
		if (auto res = m_storage.remove(key);
			std::holds_alternative<typename BtreeType::RemovedNothing>(res)) {
			throw std::invalid_argument("Can't find such key");
		}
	};

	Storage() = default;
	virtual ~Storage() {
		m_storage.save();
	};
};