#pragma once

#include <string_view>
#include <variant>

#include <detail/CredentialsStorage.h>

class UserRepository : CredentialsStorage {
	struct AuthenticationAgentConfig : DefaultConfig {
		using Key = char[256];
		using Val = char[256];
		using RealVal = char[256];
		using Ref = char[256];
	};

	~UserRepository() {
		m_storage.save();
	};

protected:
	Btree <AuthenticationAgentConfig> m_storage;
	using BtreeType = Btree<AuthenticationAgentConfig>;

public:
	UserRepository() = default;

	bool authenticate(std::string username, std::string password) override {
		if (auto pass = m_storage.get(username.begin()); pass) {
			return *pass == password;
		}
		return false;
	}

	void load(std::string_view username, std::string_view password) override {
		if (auto res = m_storage.insert(username.begin(), password.begin());
			std::holds_alternative<BtreeType::InsertionReturnMark::InsertedNothing>(res)) {
			throw std::invalid_argument("User already exists");
		}
	}
};
