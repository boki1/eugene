#pragma once

#include <core/Config.h>
#include <core/server/detail/CredentialsStorage.h>
#include <string_view>
#include <variant>

class UserRepository : CredentialsStorage {
	struct AuthenticationAgentConfig : internal::Config {
		using Key = std::string;
		using Val = std::string;
		using RealVal = std::string;
		using Ref = std::string;
		static inline constexpr bool DYN_ENTRIES = true;
	};

	~UserRepository() {
		m_storage.save();
	};

protected:
	Btree<AuthenticationAgentConfig> m_storage;
	using BtreeType = Btree<AuthenticationAgentConfig>;

public:
	UserRepository() = default;

	bool authenticate(Credentials creds) override {
		if (auto pass = m_storage.get(creds.username.begin()); pass) {
			return *pass == creds.password;
		}
		return false;
	}

	void load(Credentials creds) override {
		if (auto res = m_storage.insert(creds.username.begin(), creds.password.begin());
			std::holds_alternative<typename BtreeType::InsertionReturnMark::InsertedNothing>(res)) {
			throw std::invalid_argument("User already exists");
		}
	}
};
