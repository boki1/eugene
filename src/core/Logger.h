#pragma once

#include <memory>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>

/// @brief Singleton and thread-safe
class Logger {
public:
	/// @brief Singleton instance
	/// @return Logger instance
	static spdlog::logger &the() {
		static Logger instance;
		return *instance._logger;
	}
private:
	long max_size = 1048576 * 5; //!< 5MB max size of the log file
	long max_files = 1; //!< 2 files to keep in rotation

	std::shared_ptr<spdlog::logger> _logger =
		spdlog::rotating_logger_mt(
			"logger", "logs.txt", max_size, max_files
		); //!< logger instance

	/// @brief Constructor
	Logger() = default;
public:
	Logger(Logger const &) = delete;
	void operator=(Logger const &) = delete;
};
