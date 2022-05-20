#pragma once

#include <iostream>

#include <cpprest/asyncrt_utils.h>
#include <cpprest/containerstream.h>
#include <cpprest/filestream.h>
#include <cpprest/http_headers.h>
#include <cpprest/http_listener.h>
#include <cpprest/json.h>
#include <cpprest/producerconsumerstream.h>
#include <cpprest/uri.h>

#include <core/server/detail/Credentials.h>

using namespace web;
using namespace http;
using namespace utility;
using namespace http::experimental::listener;

class CredentialsDecoder {
public:
	CredentialsDecoder() = default;

	static Credentials decode(const http_headers &headers) {
		auto auth_header = headers.find(U("authorization"));
		std::vector<unsigned char> creds = utility::conversions
		::from_base64(auth_header->second
			              .substr(6, auth_header->second.size()));

		//	The creds are in the form of username:password
		//	stored in vector like "u" "s" "e" "r" "n" "a" "m" "e" ":" "p" "a" "s" "s" "w" "o" "r" "d"
		const std::string username{creds.begin(), std::find(creds.begin(), creds.end(), ':')};
		const std::string password{std::find(creds.begin(), creds.end(), ':') + 1, creds.end()};

		std::size_t pass_hash = std::hash<std::string>{}(password);
		return Credentials{username, std::to_string(pass_hash)};
	}

	static bool is_valid(const http_headers &headers) {
		auto auth_header = headers.find(U("authorization"));
		if (auth_header == headers.end()) {
			return false;
		}

		auto auth_value = auth_header->second;
		if (auth_value.substr(0, 6) != U("Basic ")) {
			return false;
		}

		return true;
	}
	~CredentialsDecoder() = default;
};
