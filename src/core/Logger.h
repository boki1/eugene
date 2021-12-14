#pragma once

#include <memory>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

class Logger {
public:
	static spdlog::logger &the() {
		static Logger instance;
		return *instance._logger;
	}
private:
	std::shared_ptr<spdlog::logger> _logger = spdlog::basic_logger_mt("logger",
	                                                                  "logs.txt", false);
	Logger() {
		spdlog::flush_every(std::chrono::seconds(5));
	};
public:
	Logger(Logger const &) = delete;
	void operator=(Logger const &) = delete;
};
