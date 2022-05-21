#pragma once

#include <string_view>
#include <variant>

#include <core/server/detail/Credentials.h>
#include <core/storage/btree/Btree.h>
#include <core/Config.h>
#include <core/server/detail/CredentialsStorage.h>

class CredentialsStorage {
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
	CredentialsStorage() = default;

	virtual ~CredentialsStorage() {
		m_storage.save();
	};

	[[nodiscard]] virtual bool authenticate(const Credentials &creds) {
		if (auto pass = m_storage.get(creds.username); pass) {
			return *pass == creds.password;
		}
		return false;
	}

	virtual void load(const Credentials &creds) {
		if (auto res = m_storage.insert(creds.username, creds.password);
			std::holds_alternative<typename BtreeType::InsertedNothing>(res)) {
			throw std::invalid_argument("User already exists");
		}
	}
};
