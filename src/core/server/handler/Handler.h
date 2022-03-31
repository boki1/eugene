#pragma once

#include <iostream>
#include <fstream>
#include <set>
#include <vector>

#include <cpprest/http_listener.h>
#include <cpprest/json.h>
#include <cpprest/uri.h>
#include <cpprest/asyncrt_utils.h>
#include <cpprest/json.h>
#include <cpprest/filestream.h>
#include <cpprest/containerstream.h>
#include <cpprest/producerconsumerstream.h>

#include <detail/CredentialsStorage.h>
#include <detail/CredentialsDecoder.h>
#include <detail/Storage.h>
#include <../Logger.h>

#define TRACE(msg)            ucout << msg
#define CHECK_ENDPOINT(path, string) \
    if (path == string &&                          \
	this.m_credentials_decoder.is_valid(request.headers()) && \
	this.m_user_credentials.authenticate(m_credentials_decoder.decode(request.headers())))


using namespace web;
using namespace http;
using namespace utility;
using namespace http::experimental::listener;

class Handler {
public:
	explicit Handler(const utility::string_t &, CredentialsStorage &, Storage &);
	virtual ~Handler() noexcept = default;

	pplx::task<void> open() { return m_listener.open(); }
	pplx::task<void> close() { return m_listener.close(); }

protected:

private:
	http_listener m_listener;
	CredentialsStorage &m_user_credentials;


	using FuncHandleRequest = const std::function<void(json::value const &, json::value &)>;

	using pimpl_credentials = CredentialsDecoder;
	std::unique_ptr<pimpl_credentials> m_credentials_decoder;

	Storage m_storage;


	[[maybe_unused]] void handle_error(const pplx::task<void> &);
	void handle_request(std::string_view,
	                    const http_request &, FuncHandleRequest &);
	void handle_get(const http_request &);
	void handle_put(const http_request &);
	void handle_post(const http_request &);
	void handle_delete(const http_request &);
};
