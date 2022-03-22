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

#define TRACE(msg)            ucout << msg
#define TRACE_ACTION(a, k, v) ucout << (a) << " (" << (k) << ", " << (v) << ")\n"
#define CHECK_ENDPOINT(path, string, authenticate) \
    if (path == string && authenticate(request.headers()))

using namespace web;
using namespace http;
using namespace utility;
using namespace http::experimental::listener;

class handler {
public:
	explicit handler(const utility::string_t &);
	virtual ~handler() noexcept = default;

	pplx::task<void> open() { return m_listener.open(); }
	pplx::task<void> close() { return m_listener.close(); }

protected:

private:
	http_listener m_listener;
	std::map<std::string, std::string> dictionary;
	using FuncHandleRequest = const std::function<void(json::value const &, json::value &)>;

	[[maybe_unused]] void handle_error(const pplx::task<void> &);
	void handle_request(std::string_view endpoint,
	                    const http_request &, FuncHandleRequest &);
	void handle_get(const http_request &);
	void handle_put(const http_request &);
	void handle_post(const http_request &);
	void handle_delete(const http_request &);
};
