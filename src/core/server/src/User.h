#pragma once

#include <string_view>

class AuthenticationAgent {
	struct AuthenticationEntry {
		std::uint64_t id;
		std::uint8_t passwd[256];
	};

	struct AuthenticationAgentConfig : DefaultConfig {
		using Key = std::uint64_t;
		using Val = std::uint8_t[256];
		using Ref = std::uint8_t[256];
	};

	Btree<AuthenticationAgentConfig> m_storage;

public:
	virtual ~AuthenticationAgent() = default;
	virtual bool authenticate(std::string_view username, std::string_view password) = 0;
};

class User {
	LocalStorage

public:
	void store() noexcept const;
	void load() noexcept const;
};