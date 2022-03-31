#pragma once

#include <string_view>
#include <variant>

#include <../storage/btree/Btree.h>

class CredentialsStorage {
public:
	virtual bool authenticate(std::string_view username, std::string_view password) = 0;
	virtual void load(const std::string username, const std::string password) = 0;
};
