#pragma once

#include <core/Config.h>
#include <core/server/detail/CredentialsStorage.h>
#include <string_view>
#include <variant>

class UserRepository : public CredentialsStorage {
	struct AuthenticationAgentConfig : internal::Config {
		using Key = std::string;
		using RealVal = std::string;
		using Ref = std::string;
		static inline constexpr bool DYN_ENTRIES = true;
	};

protected:
	internal::storage::btree::Btree<AuthenticationAgentConfig> m_storage;
	using BtreeType = internal::storage::btree::Btree<AuthenticationAgentConfig>;

public:
	UserRepository() = default;

	~UserRepository() override {
		m_storage.save();
	};

	[[nodiscard]] bool authenticate(Credentials creds) override {
		if (auto pass = m_storage.get(creds.username); pass) {
			return *pass == creds.password;
		}
		return false;
	}

	void load(Credentials creds) override {
		if (auto res = m_storage.insert(creds.username, creds.password);
			std::holds_alternative<typename BtreeType::InsertedNothing>(res)) {
			throw std::invalid_argument("User already exists");
		}
	}
};
