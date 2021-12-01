#ifndef EUGENE_SRC_EUGENE_API_LOGGER_H
#define EUGENE_SRC_EUGENE_API_LOGGER_H

#include "spdlog/spdlog.h"
#include <spdlog/sinks/basic_file_sink.h>
#include <memory>

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

#endif
