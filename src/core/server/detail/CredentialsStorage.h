#pragma once

#include <string_view>
#include <variant>

#include <core/server/detail/Credentials.h>
#include <core/storage/btree/Btree.h>

class CredentialsStorage {
public:
	[[nodiscard]] virtual bool authenticate(Credentials creds) = 0;
	virtual void load(Credentials creds) = 0;

	virtual ~CredentialsStorage() = default;
};
