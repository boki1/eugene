#include <handler/handler.h>

handler::handler(const utility::string_t &url) : m_listener(url) {
	m_listener.support(
		methods::GET,
		[this](auto &&PH1) {
		  handle_get(std::forward<decltype(PH1)>(PH1));
		});
	m_listener.support(
		methods::PUT,
		[this](auto &&PH1) {
		  handle_put(std::forward<decltype(PH1)>(PH1));
		});
	m_listener.support(
		methods::POST,
		[this](auto &&PH1) {
		  handle_post(std::forward<decltype(PH1)>(PH1));
		});
	m_listener.support(
		methods::DEL,
		[this](auto &&PH1) {
		  handle_delete(std::forward<decltype(PH1)>(PH1));
		});

}

[[maybe_unused]] void handler::handle_error(const pplx::task<void> &task) {
	try {
		task.get();
	}
	catch (...) {
		// TODO: Log the error
	}
}

// TODO: remove this
void display_json(
	json::value const &jvalue,
	utility::string_t const &prefix) {
	ucout << prefix << jvalue.serialize() << std::endl;
}

bool authenticate(const http_headers &headers) {
	auto auth_header = headers.find(U("authorization"));
	std::vector<unsigned char> credentials = utility::conversions::from_base64
		(auth_header->second.substr(6, auth_header->second.size()));

	std::string username{credentials.begin(), std::find(credentials.begin(), credentials.end(), ':')};
	std::string password{std::find(credentials.begin(), credentials.end(), ':') + 1, credentials.end()};


	return true;
}

void handler::handle_request(std::string_view endpoint,
                             const http_request &request, FuncHandleRequest &action) {
	auto path = request.relative_uri().path();
	CHECK_ENDPOINT(path, endpoint, authenticate) {
		auto answer = json::value::object();
		request
			.extract_json()
			.then([&answer, &action](const pplx::task<json::value> &task) {
			  try {
				  auto const &jvalue = task.get();
				  display_json(jvalue, "R: ");

				  if (!jvalue.is_null()) {
					  action(jvalue, answer);
				  }
			  }
			  catch (http_exception const &e) {
				  ucout << e.what() << std::endl;
			  }
			})
			.wait();

		display_json(answer, "S: ");

		request.reply(status_codes::OK, answer);
	}
	request.reply(status_codes::NotFound);
}

void handler::handle_get(const http_request &request) {
	TRACE("\nhandle GET\n");

	auto path = request.relative_uri().path();
	CHECK_ENDPOINT(path, "/eugene", authenticate) {
		auto answer = json::value::object();

		for (auto const &p : dictionary) {
			answer[p.first] = json::value::string(p.second);
		}
		display_json(json::value::null(), "R: ");
		display_json(answer, "S: ");

		request.reply(status_codes::OK, answer);
		return;
	}
	request.reply(status_codes::NotFound);
}

void handler::handle_post(const http_request &request) {
	TRACE("\nhandle POST\n");

	auto path = request.relative_uri().path();
	handle_request(
		"/eugene/register",
		request,
		[this](json::value const &jvalue, json::value &answer) {
		  for (auto const &pair : jvalue.as_object()) {
			  if (pair.second.is_string()) {
//					  TODO: use the b-tree implementation
			  }
		  }
		});
}

void handler::handle_put(const http_request &request) {
	TRACE("\nhandle PUT\n");

	handle_request(
		"/eugene",
		request,
		[this](json::value const &jvalue, json::value &answer) {
		  for (auto const &pair : jvalue.as_object()) {
			  if (pair.second.is_string()) {
				  auto key = pair.first;
				  auto value = pair.second.as_string();

				  if (dictionary.find(key) == dictionary.end()) {
					  TRACE_ACTION("added", key, value);
					  answer[key] = json::value::string("<put>");
				  } else {
					  TRACE_ACTION("updated", key, value);
					  answer[key] = json::value::string("<updated>");
				  }

				  dictionary[key] = value;
			  }
		  }
		});
}

void handler::handle_delete(const http_request &request) {
	TRACE("\nhandle DEL\n");

	auto path = request.relative_uri().path();
	handle_request(
		"/eugene",
		request,
		[this](json::value const &jvalue, json::value &answer) {
		  std::set<utility::string_t> keys;
		  for (auto const &pair : jvalue.as_array()) {
			  if (pair.is_string()) {
				  auto key = pair.as_string();

				  auto pos = dictionary.find(key);
				  if (pos == dictionary.end()) {
					  answer[key] = json::value::string("<failed>");
				  } else {
					  TRACE_ACTION("deleted", pos->first, pos->second);
					  answer[key] = json::value::string("<deleted>");
					  keys.insert(key);
				  }
			  }
		  }

		  for (auto const &key : keys)
			  dictionary.erase(key);
		});
}