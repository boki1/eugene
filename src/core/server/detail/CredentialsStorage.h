#pragma once

#include <string_view>
#include <variant>

#include <detail/Credentials.h>
#include <../storage/btree/Btree.h>

class CredentialsStorage {
public:
	virtual bool authenticate(Credentials creds) = 0;
	virtual void load(Credentials creds) = 0;
};
