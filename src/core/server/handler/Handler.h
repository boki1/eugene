#pragma once

#include <iostream>
#include <fstream>
#include <set>
#include <vector>

#include <core/server/detail/CredentialsStorage.h>
#include <core/server/detail/CredentialsDecoder.h>
#include <core/server/detail/Storage.h>
#include <core/Logger.h>
#include <core/server/tests/UserRepository.h>
#include <core/server/tests/ExampleStorage.h>

#define TRACE(msg)            ucout << msg
#define CHECK_ENDPOINT(path, string) \
    if (path == string &&                          \
    m_credentials_decoder->is_valid(request.headers()) && \
    m_user_credentials->authenticate(m_credentials_decoder->decode(request.headers())))

using namespace web;
using namespace http;
using namespace utility;
using namespace http::experimental::listener;

template<typename Key, typename Value>
class Handler {
public:
	explicit Handler(const utility::string_t &url,
	                 UserRepository &&user_credentials,
	                 ExampleStorage &&storage) : m_listener(url),
	                                             m_user_credentials(std::make_unique<UserRepository>(user_credentials)),
	                                             m_storage(std::make_unique<ExampleStorage>(storage)) {
		m_listener.support(
			methods::GET,
			[this](auto &&initial_param) {
			  handle_get(std::forward<decltype(initial_param)>(initial_param));
			});
		m_listener.support(
			methods::PUT,
			[this](auto &&initial_param) {
			  handle_put(std::forward<decltype(initial_param)>(initial_param));
			});
		m_listener.support(
			methods::POST,
			[this](auto &&initial_param) {
			  handle_post(std::forward<decltype(initial_param)>(initial_param));
			});
		m_listener.support(
			methods::DEL,
			[this](auto &&initial_param) {
			  handle_delete(std::forward<decltype(initial_param)>(initial_param));
			});
	}
	virtual ~Handler() noexcept = default;

	pplx::task<void> open() { return m_listener.open(); }
	pplx::task<void> close() { return m_listener.close(); }

protected:

private:
	http_listener m_listener;
	std::unique_ptr<UserRepository> m_user_credentials;

	std::unique_ptr<ExampleStorage> m_storage;

	using pimpl_credentials = CredentialsDecoder;
	std::unique_ptr<pimpl_credentials> m_credentials_decoder;

	using FuncHandleRequest = const std::function<void(json::value const &, json::value &)>;

	[[maybe_unused]] void handle_error(const pplx::task<void> &task) {
		try {
			task.get();
		} catch (std::exception &e) {
			Logger::the([ex = e.what()](spdlog::logger logger) {
			  logger.log(spdlog::level::err,
			             fmt::runtime(R"(Backend failure with error "{0}")"), ex);
			});
		}
	}

	void handle_request(std::string_view endpoint,
	                    const http_request &request, FuncHandleRequest &action) {
		auto path = request.relative_uri().path();
		CHECK_ENDPOINT(path, endpoint) {
			auto answer = json::value::object();
			request
				.extract_json()
				.then([&answer, &action, &request](const pplx::task<json::value> &task) {
				  try {
					  auto const &jvalue = task.get();

					  if (!jvalue.is_null()) {
						  action(jvalue, answer);
					  }
				  } catch (http_exception const &e) {
					  ucout << e.what() << std::endl;
					  Logger::the([request_type = request.method(), path = request.relative_uri().path(), ex = e.what()]
						              (spdlog::logger logger) {
						logger.log(spdlog::level::info,
						           fmt::runtime(R"(Backend "{0}" failure of "{1}" with exception "{2}")"),
						           request_type, path, ex);
					  });
					  request.reply(status_codes::InternalError,
					                json::value::string(e.what()));
					  return;
				  }
				})
				.wait();

			Logger::the([request_type = request.method(), path](spdlog::logger logger) {
			  logger.log(spdlog::level::info,
			             fmt::runtime(R"(Backend "{0}" success of "{1}")"),
			             request_type, path);
			});
			request.reply(status_codes::OK, answer);
			return;
		}
		try {
			m_credentials_decoder->decode(request.headers());
			if (!m_credentials_decoder->is_valid(request.headers()) ||
				!m_user_credentials->authenticate(m_credentials_decoder->decode(request.headers()))) {
				Logger::the([request_type = request.method(), path = request.relative_uri().path()](spdlog::logger logger) {
				  logger.log(spdlog::level::err,
				             fmt::runtime(R"(Backend "{0}" failure of "{1}", user unauthorized)"),
				             request_type, path);
				});
				request.reply(status_codes::Unauthorized);
				return;
			}
		} catch (std::exception &e) {
			Logger::the([request_type = request.method(), path = request.relative_uri().path()](spdlog::logger logger) {
			  logger.log(spdlog::level::err,
			             fmt::runtime(R"(Backend "{0}" failure of "{1}", user unauthorized)"),
			             request_type, path);
			});
			request.reply(status_codes::Unauthorized);
			return;
		}
		Logger::the([request_type = request.method(), path = request.relative_uri().path()](spdlog::logger logger) {
		  logger.log(spdlog::level::err,
		             fmt::runtime(R"(Backend "{0}" failure for "{1}", probably endpoint doesn't exist)"),
		             request_type, path);
		});

		request.reply(status_codes::NotFound);
	}

	void handle_get(const http_request &request) {
		TRACE("\nhandle GET\n");

		//	json send something like
		//	[obj1, obj2]
		auto path = request.relative_uri().path();
		handle_request(
			"/eugene",
			request,
			[this, request](json::value const &jvalue, json::value &answer) {
			  for (auto const &key : jvalue.as_array()) {
				  try {
					  answer[key.as_string()] = json::value::string(
						  this->m_storage->get(key.as_string()));
				  } catch (std::invalid_argument const &e) {
					  Logger::the([request_type = request.method(), key = key.as_string()](spdlog::logger logger) {
						logger.log(spdlog::level::info,
						           fmt::runtime(R"(Backend "{0}" failure "{1}" can't get value)"),
						           request_type, key);
					  });
					  request.reply(status_codes::NoContent);
					  return;
				  }
			  }
			});
	}

	void handle_put(const http_request &request) {
		TRACE("\nhandle PUT\n");

		handle_request(
			"/eugene",
			request,
			[this, request](json::value const &jvalue, json::value &) {
			  for (auto const &pair : jvalue.as_object()) {
				  auto key = pair.first;
				  auto value = pair.second;

				  try {
					  this->m_storage->set(key, value.as_string());
				  }
				  catch (std::invalid_argument const &e) {
					  Logger::the([request_type = request.method(), key](spdlog::logger logger) {
						logger.log(spdlog::level::info,
						           fmt::runtime(R"(Backend "{0}" failure "{1}" already exists)"),
						           request_type, key);
					  });
					  request.reply(status_codes::NoContent);
					  return;
				  }
			  }
			});
	}

	void handle_post(const http_request &request) {
		TRACE("\nhandle POST\n");

		auto path = request.relative_uri().path();
		if (path == "/eugene/register" && m_credentials_decoder->is_valid(request.headers())) {
			Credentials creds = m_credentials_decoder->decode(request.headers());
			try {
				m_user_credentials->load(Credentials{creds.username, creds.password});

				Logger::the([request_type = request.method(), username = creds.username](spdlog::logger logger) {
				  logger.log(spdlog::level::info,
				             fmt::runtime(R"(Backend "{0}" user "{1}" registered successfully)"),
				             request_type, username);
				});
				request.reply(status_codes::OK);
				return;
			}
			catch (std::invalid_argument &e) {
				Logger::the([request_type = request.method(), username = creds.username](spdlog::logger logger) {
				  logger.log(spdlog::level::err,
				             fmt::runtime(R"(Backend "{0}" user "{1}" already exists)"),
				             request_type, username);
				});
				request.reply(status_codes::Found,
				              json::value::string(e.what()));
				return;
			}
		}
		Logger::the([request_type = request.method(), path = request.relative_uri().path()](spdlog::logger logger) {
		  logger.log(spdlog::level::err,
		             fmt::runtime(R"(Backend "{0}" user authentication failed for "{1}", probably endpoint doesn't exist)"),
		             request_type, path);
		});

		request.reply(status_codes::NotFound);
	}

	void handle_delete(const http_request &request) {
		TRACE("\nhandle DEL\n");

		auto path = request.relative_uri().path();
		handle_request(
			"/eugene",
			request,
			[this, request](json::value const &jvalue, json::value &) {
			  for (auto const &key : jvalue.as_array()) {
				  try {
					  this->m_storage->remove(key.as_string());
				  } catch (std::invalid_argument const &e) {
					  Logger::the([request_type = request.method(), key = key.as_string()](spdlog::logger logger) {
					    logger.log(spdlog::level::info,
					               fmt::runtime(R"(Backend "{0}" failure "{1}" can't delete value)"),
					               request_type, key);
					  });
					  request.reply(status_codes::Conflict);
					  return;
				  }
			  }
			});
	}
};
