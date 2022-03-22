#include <iostream>
#include <memory>

#include <handler/handler.h>

using namespace web;
using namespace http;
using namespace utility;
using namespace http::experimental::listener;

std::unique_ptr<handler> g_httpHandler;

void on_initialize(const string_t &address) {
	uri_builder uri(address);

	auto addr = uri.to_uri().to_string();
	g_httpHandler = std::make_unique<handler>(addr);
	g_httpHandler->open().wait();

	ucout << utility::string_t(U("Listening for requests at: ")) << addr << std::endl;
}

void on_shutdown() {
	g_httpHandler->close().wait();
}

int main(int argc, char *argv[]) {
	utility::string_t port = U("8080");
	if (argc == 2) {
		port = argv[1];
	}

	utility::string_t address = U("http://127.0.0.1:");
	address.append(port);

	on_initialize(address);
	std::cout << "Press ENTER to exit" << std::endl;

	std::string line;
	std::getline(std::cin, line);

	on_shutdown();
	return 0;
}
