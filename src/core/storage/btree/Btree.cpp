#include <map>
#include <random>
#include <string>
#include <vector>

#include <catch2/catch.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <nop/structure.h>

#include <core/storage/Position.h>
#include <core/storage/btree/Btree.h>
#include <core/storage/btree/BtreePrinter.h>

using namespace internal::storage::btree;
using internal::storage::Position;

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
float item<float>() {
	return static_cast<float>(item<int>()) / static_cast<float>(item<int>());
}

template<>
bool item<bool>() {
	return item<int>() % 2 == 0;
}

void truncate_file(std::string_view fname) {
	std::ofstream of{std::string{fname}, std::ios::trunc};
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
	truncate_file("/tmp/eu-btree-ops");
	Btree bpt("/tmp/eu-btree-ops");
	REQUIRE(bpt.contains(42) == false);
	REQUIRE(bpt.get(42).has_value() == false);

	fmt::print("NUM_RECORDS_LEAF = {}\n", bpt.num_records_leaf());
	fmt::print("NUM_RECORDS_BRANCH = {}\n", bpt.num_records_branch());

	static const std::size_t limit = 1000000;

	std::map<Config::Key, Config::Val> backup;
	while (backup.size() != limit) {
		auto key = item<int>();
		auto val = item<int>();
		bpt.put(key, val);
		backup.emplace(key, val);
	}

	util::BtreePrinter{bpt, "/tmp/eu-btree-ops-printed"}();

	REQUIRE(bpt.size() == limit);

	std::size_t i = 1;
	for (const auto &[key, val] : backup) {
		fmt::print(" [{}]: '{}' => '{}'\n", i++, key, val);
		REQUIRE(bpt.get(key).value() == val);
	}
}

TEST_CASE("Header operations", "[btree]") {
	truncate_file("/tmp/eu-headerops");
	truncate_file("/tmp/eu-headerops-header");
	Btree bpt("/tmp/eu-headerops", "/tmp/eu-headerops-header");
	bpt.save_header();

	Btree bpt2("/tmp/eu-headerops", "/tmp/eu-headerops-header");
	bpt2.header().m_size = 100;

	REQUIRE(bpt2.load_header());

	REQUIRE(bpt.header() == bpt2.header());
}

TEST_CASE("Persistent Btree", "[btree]") {
	truncate_file("/tmp/eu-persistent-btree");
	truncate_file("/tmp/eu-persistent-btree-header");

	std::map<uint32_t, uint32_t> backup;

	Position rootpos;
	{
		fmt::print("V1 ---\n");
		Btree bpt("/tmp/eu-persistent-btree", "/tmp/eu-persistent-btree-header");
		while (backup.size() != 1000) {
			auto key = item<int>();
			auto val = item<int>();
			bpt.put(key, val);
			backup[key] = val;
		}

		for (auto &[key, val] : backup) {
			REQUIRE(bpt.get(key).value() == val);
			fmt::print("-- '{}' => '{}'\n", key, val);
		}

		rootpos = bpt.rootpos();
		fmt::print("Root pos: {}\n", rootpos);
		bpt.save();
		fmt::print("Btree V1 Saved\n");
	}

	{
		// fmt::print("\nV2 ---\n");
		// Btree bpt("/tmp/eu-persistent-btree", "/tmp/eu-persistent-btree-header", true);

		// REQUIRE(bpt.rootpos() == rootpos);

		/*
		for (auto &[key, val] : backup) {
			REQUIRE(bpt.get(key).value() == val);
			fmt::print("-- '{}' => '{}'\n", key, val);
		}
		*/
	}

	/*
	{
		Btree bpt("/tmp/eu-persistent-btree", "/tmp/eu-persistent-btree-header", false);

		int i = 0;
		for (auto &[key, val] : backup) {
			REQUIRE(!bpt.contains(key));
			fmt::print("-- {} -> Key '{}' found mapped to '{}'\n", i++, key, val);
		}
	}
	*/
}

struct CustomConfigPrimitives : DefaultConfig {
	using Key = char;
	using Val = char;
	using Ref = char;
};

TEST_CASE("Custom Config Btree Primitive Type", "[btree]") {
	truncate_file("/tmp/eu-btree-ops-custom-types");
	Btree<CustomConfigPrimitives> bpt("/tmp/eu-btree-ops-custom-types");
	static const std::size_t limit = 255;

	fmt::print("NUM_RECORDS_LEAF = {}\n", bpt.num_records_leaf());
	fmt::print("NUM_RECORDS_BRANCH = {}\n", bpt.num_records_branch());

	std::map<CustomConfigPrimitives::Key, CustomConfigPrimitives::Val> backup;
	char f = 0;
	while (backup.size() != limit) {
		auto key = f;  //item<float>();
		auto val = f++;//item<float>();
		bpt.put(key, val);
		backup.emplace(key, val);
	}

	util::BtreePrinter{bpt, "/tmp/eu-btree-ops-custom-types-printed"}();

	REQUIRE(bpt.size() == limit);

	std::size_t i = 1;
	for (const auto &[key, val] : backup) {
		fmt::print(" [{}]: '{}' => '{}'\n", i++, key, val);
		REQUIRE(bpt.get(key).value() == val);
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
	truncate_file("/tmp/eu-btree-custom-config");
	Btree<CustomConfigAggregate> btr{"/tmp/eu-btree-custom-config"};
	std::map<std::string, Person> backup;

	static const std::size_t limit = 100;

	while (backup.size() != limit) {
		auto key = item<std::string>();
		auto val = item<Person>();
		btr.put(key, val);
		backup[key] = val;
	}

	std::size_t i = 0;
	for (auto &[key, val] : backup) {
		REQUIRE(btr.get(key).value() == val);
		fmt::print("-- {} -> Key '{}' found mapped to '{}'\n", i++, key, val);
	}
}
