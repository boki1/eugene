#include <iostream>
#include <random>
#include <map>

#include <catch2/catch.hpp>
#include <fmt/core.h>

#include <core/storage/Storage.h>
#include <core/storage/btree/Btree.h>
using namespace internal::storage::btree;

template<typename T>
T item();

template<>
int item<int>() {
	static std::random_device dev;
	static std::mt19937 rng(dev());
	static std::uniform_int_distribution<std::mt19937::result_type> dist(1, 10000000);
	return dist(rng);
}

template<>
double item<double>() {
	return static_cast<double>(item<int>()) / static_cast<double>(item<int>());
}

template<>
std::string item<std::string>() {
	static std::random_device dev;
	static std::mt19937 rng(dev());
	static std::uniform_int_distribution<std::mt19937::result_type> dist10(1, 10);

	static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	static std::uniform_int_distribution<std::mt19937::result_type> dist_alphanum(0, sizeof(alphanum) - 1);

	auto len = dist10(rng);
	std::string str;
	str.reserve(len);
	for (auto i = 0ul; i < len; ++i)
		str += alphanum[dist_alphanum(rng)];
	return str;
}

TEST_CASE("Btree operations", "[btree]") {
	Btree bpt("/tmp/eu-btree-pgcache_name");
	REQUIRE(bpt.contains(42) == false);
	REQUIRE(bpt.get(42).has_value() == false);

	bpt.put(42, 1);
	REQUIRE(bpt.root().leaf().m_keys[0] == 42);
	REQUIRE(bpt.root().leaf().m_vals[0] == 1);

	REQUIRE(bpt.get(42).value() == 1);

	std::map<Config::Key, Config::Val> backup;
	while (backup.size() != 200000) {
		auto key = item<int>();
		auto val = key;//item();
		bpt.put(key, val);
		backup.emplace(key, val);
	}

	int i = 1; for (const auto &[key, val] : backup) {
		REQUIRE(bpt.get(key).value() == val);
		fmt::print("-- {} -> Key '{}' found mapped to '{}'\n", i++, key, val);
	}
}

struct CustomConfigPrimitives : DefaultConfig {
	using Key = std::string;
	using Val = double;
	using Ref = std::string;

	static inline constexpr int NUM_RECORDS = 256;
	static inline constexpr int BTREE_NODE_BREAK_POINT = (NUM_RECORDS - 1) / 2;
};

TEST_CASE("Custom Config Btree Primitive Types", "[btree]") {
	Btree<CustomConfigPrimitives> btr{"/tmp/eu-btree-custom-config"};
	std::map<std::string, double> backup;

	while (backup.size() != 1000) {
		auto key = item<std::string>();
		auto val = item<double>();
		btr.put(key, val);
		backup[key] = val;
	}

	int i = 0;
	for (auto &[key, val] : backup) {
		REQUIRE(btr.get(key).value().get() == val);
		fmt::print("-- {} -> Key '{}' found mapped to '{}'\n", i++, key, val);
	}
}

struct Person {
	std::string name;
	int age;
	std::string email;

	auto operator<=>(const Person &) const noexcept = default;

	NOP_STRUCTURE(Person, name, age, email);
};

// Make Person printable
template<>
struct fmt::formatter<Person> {
	template<typename ParseContext>
	constexpr auto parse(ParseContext &ctx) {
		return ctx.begin();
	}

	template<typename FormatContext>
	auto format(const Person &p, FormatContext &ctx) {
		return fmt::format_to(ctx.out(), "Person{{ .name='{}', .age={}, .email='{}' }}", p.name, p.age, p.email);
	}
};

template<>
Person item<Person>() {
	return Person{
	        .name = item<std::string>(),
	        .age = item<int>(),
	        .email = item<std::string>()};
}

struct CustomConfigAggregate : DefaultConfig {
	using Key = std::string;
	using Val = Person;
	using Ref = std::string;
};

TEST_CASE("Custom Config Btree Aggregate Types", "[btree]") {
	Btree<CustomConfigAggregate> btr{"/tmp/eu-btree-custom-config"};
	std::map<std::string, Person> backup;

	while (backup.size() != 10000) {
		auto key = item<std::string>();
		auto val = item<Person>();
		btr.put(key, val);
		backup[key] = val;
	}

	int i = 0;
	for (auto &[key, val] : backup) {
		REQUIRE(btr.get(key).value().get() == val);
		fmt::print("-- {} -> Key '{}' found mapped to '{}'\n", i++, key, val);
	}
}
