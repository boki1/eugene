#pragma once

#include <string_view>
#include <variant>

#include <core/server/detail/Storage.h>
#include <core/storage/btree/Btree.cpp>
#include <core/Config.h>

class ExampleStorage : Storage<std::string, std::string> {
	struct ExampleAgentConfig : Config {
		using Key = std::string;
		using Val = std::string;
		using RealVal = char[256];
		using Ref = int;
	};

	~ExampleStorage() {
		m_storage.save();
	};

protected:
	Btree<ExampleAgentConfig> m_storage;
	using BtreeType = Btree<ExampleAgentConfig>;

public:
	ExampleStorage() = default;

	void set(std::string key, std::string value) override {
		if (auto res = m_storage.insert(key, value);
			std::holds_alternative<typename BtreeType::InsertionReturnMark::InsertedNothing>(res)) {
			throw std::invalid_argument("User already exists");
		}
	}

	std::string get(std::string key) override {
		if (auto res = m_storage.get(key); res)
			return *res;
		throw std::invalid_argument("Can't find such key");
	}

	void remove(std::string key) override {
		if (auto res = m_storage.remove(key);
			std::holds_alternative<typename BtreeType::RemovalReturnMark::RemovedNothing>(res)) {
			throw std::invalid_argument("Can't find such key");
		}
	}
};
