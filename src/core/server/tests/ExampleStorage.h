#pragma once

#include <string_view>
#include <variant>

#include <detail/Storage.h>

class ExampleStorage : Storage {
	struct ExampleAgentConfig : DefaultConfig {
		using Key = int;
		using Val = char[256];
		using RealVal = char[256];
		using Ref = int;
	};

	~ExampleStorage() {
		m_storage.save();
	};

protected:
	Btree <ExampleAgentConfig> m_storage;
	using BtreeType = Btree<ExampleAgentConfig>;

public:
	ExampleStorage() = default;

	void set(int key, char value[]) override {
		if (auto res = m_storage.insert(key, value);
			std::holds_alternative<BtreeType::InsertionReturnMark::InsertedNothing>(res)) {
			throw std::invalid_argument("User already exists");
		}
	}

	char *get(int key) override {
		if (auto res = m_storage.get(key); res)
			return *res;
		throw std::invalid_argument("Can't find such key");
	}

	void remove(int key) override {
		if (auto res = m_storage.remove(key);
			std::holds_alternative<BtreeType::RemovalReturnMark::RemovedNothing>(res)) {
			throw std::invalid_argument("Can't find such key");
		}
	}
};
