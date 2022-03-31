#pragma once

#include <iostream>

#include <cpprest/http_listener.h>
#include <cpprest/http_headers.h>
#include <cpprest/json.h>
#include <cpprest/uri.h>
#include <cpprest/asyncrt_utils.h>
#include <cpprest/json.h>
#include <cpprest/filestream.h>
#include <cpprest/containerstream.h>
#include <cpprest/producerconsumerstream.h>

using namespace web;
using namespace http;
using namespace utility;
using namespace http::experimental::listener;

struct credentials {
	const std::string username;
	const std::string password;
};

class CredentialsDecoder {
public:
	CredentialsDecoder() = default;
	CredentialsDecoder(const http_headers &credentials) : m_headers(credentials) {}

	static credentials decode(const http_headers &headers) {
		auto auth_header = headers.find(U("authorization"));
		std::vector<unsigned char> credentials = utility::conversions::from_base64
			(auth_header->second.substr(6, auth_header->second.size()));

		//	The credentials are in the form of username:password
		//	stored in vector like "u" "s" "e" "r" "n" "a" "m" "e" ":" "p" "a" "s" "s" "w" "o" "r" "d"
		const std::string username{credentials.begin(), std::find(credentials.begin(), credentials.end(), ':')};
		const std::string password{std::find(credentials.begin(), credentials.end(), ':') + 1, credentials.end()};

		std::size_t pass_hash = std::hash<std::string>{}(password);
		return creds{username, std::to_string(pass_hash)};
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

private:
	const http_headers &m_headers;

	~CredentialsDecoder() = default;
};
