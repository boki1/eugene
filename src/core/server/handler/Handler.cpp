#include <handler/Handler.h>

Handler::Handler(const utility::string_t &url,
                 CredentialsStorage &user_credentials,
                 Storage &storage) : m_listener(url),
                                     m_user_credentials(user_credentials),
                                     m_storage(storage) {
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

[[maybe_unused]] void Handler::handle_error(const pplx::task<void> &task) {
	try {
		task.get();
	}
	catch (std::exception &e) {
		Logger::the().log(spdlog::level::err,
		                  R"(Backend failure of with error "{0}")", e.what());
	}
}

void Handler::handle_request(std::string_view endpoint,
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
			  }
			  catch (http_exception const &e) {
				  ucout << e.what() << std::endl;
				  Logger::the().log(spdlog::level::info,
				                    R"(Backend failure of "{0}" with exception "{1}")",
				                    request.relative_uri().path(), e.what());
				  request.reply(status_codes::InternalError,
				                json::value::string(e.what()));
				  return;
			  }
			})
			.wait();

		request.reply(status_codes::OK, answer);
		return;
	}
	if (!m_credentials_decoder.decode(request.headers()) ||
		!m_credentials_decoder.is_valid(request.headers())) {
		request.reply(status_codes::Unauthorized);
		return;
	}
	request.reply(status_codes::NotFound);
}

void Handler::handle_get(const http_request &request) {
	TRACE("\nhandle GET\n");

//	json send something like
//	{
//		<key for indexing>",
//		<key for indexing>",
//		...
//	}
	auto path = request.relative_uri().path();
	handle_request(
		"/eugene",
		request,
		[this](json::value const &jvalue, json::value &answer) {
		  for (auto const &key : jvalue.as_array()) {
			  answer[std::to_string(key)] = json::value::string(
				  this->m_storage.get(key)
			  );
		  }
		});
}

void Handler::handle_post(const http_request &request) {
	TRACE("\nhandle POST\n");

	auto path = request.relative_uri().path();
	handle_request(
		"/eugene/register",
		request,
		[this](json::value const &jvalue, json::value &answer) {
		  credentials creds = m_credentials_decoder.decode(request);
		  this->m_user_credentials.load(creds.username, creds.password);
		}
	);
}

void Handler::handle_put(const http_request &request) {
	TRACE("\nhandle PUT\n");

	handle_request(
		"/eugene",
		request,
		[this](json::value const &jvalue, json::value &answer) {
		  for (auto const &pair : jvalue.as_object()) {
			  auto key = pair.first;
			  auto value = pair.second;

			  this->m_storage.set(key, value);
		  }
		});
}

void Handler::handle_delete(const http_request &request) {
	TRACE("\nhandle DEL\n");

	auto path = request.relative_uri().path();
	handle_request(
		"/eugene",
		request,
		[this](json::value const &jvalue, json::value &answer) {
		  for (auto const &key : jvalue.as_array()) {
			  this->m_storage.remove(key);
		  }
		});
}
