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

template<BtreeConfig Config>
auto fill_tree(Btree<Config> &bpt, std::size_t limit = 1000) {
	using Key = typename Config::Key;
	using Val = typename Config::Val;
	std::map<Key, Val> backup;
	// int i = 0;

	while (backup.size() != limit) {
		auto key = item<Key>();
		auto val = item<Val>();
		if (backup.contains(key)) {
			REQUIRE(bpt.contains(key));
			continue;
		}

		// fmt::print("Element #{} inserted.\n", ++i);

		bpt.put(key, val);
		backup.emplace(key, val);
	}

	REQUIRE(bpt.size() == limit);
	return backup;
}

template<BtreeConfig Config>
void valid_tree(const Btree<Config> &bpt, const std::map<typename Config::Key, typename Config::Val> &backup) {
	REQUIRE(bpt.size() == backup.size());
	std::size_t i = 1;
	for (const auto &[key, val] : backup) {
		fmt::print(" [{}]: '{}' => '{}'\n", i++, key, val);
		REQUIRE(bpt.get(key).value() == val);
	}
}

void truncate_file(std::string_view fname) {
	std::ofstream of{std::string{fname}, std::ios::trunc};
}

struct smallstr {
	static constexpr inline auto sz = 10;
	smallstr(std::string &&s) {
		assert(s.size() == sz);
		std::memcpy(m_str, s.data(), sz);
	}

	smallstr() = default;

	auto operator<=>(const smallstr &) const noexcept = default;

	friend std::ostream &operator<<(std::ostream &os, const smallstr &str) {
		os << static_cast<const char *>(str.m_str);
		return os;
	}

	char m_str[sz];
	NOP_STRUCTURE(smallstr, m_str);
};

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

template<>
smallstr item<smallstr>() {
	static std::random_device dev;
	static std::mt19937 rng(dev());
	static std::uniform_int_distribution<std::mt19937::result_type> dist10(1, 10);

	static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	static std::uniform_int_distribution<std::mt19937::result_type> dist_alphanum(0, sizeof(alphanum) - 1);

	std::string str;
	str.reserve(smallstr::sz);
	for (int i = 0; i < smallstr::sz; ++i)
		str += alphanum[dist_alphanum(rng)];
	return smallstr{std::move(str)};
}

TEST_CASE("Btree operations", "[btree]") {
	truncate_file("/tmp/eu-btree-ops");
	Btree bpt("/tmp/eu-btree-ops");
	REQUIRE(bpt.contains(42) == false);
	REQUIRE(bpt.get(42).has_value() == false);

	fmt::print("NUM_RECORDS_LEAF = {}\n", bpt.num_records_leaf());
	fmt::print("NUM_RECORDS_BRANCH = {}\n", bpt.num_records_branch());

	static const std::size_t limit = 1000;
	auto backup = fill_tree(bpt, limit);

	util::BtreePrinter{bpt, "/tmp/eu-btree-ops-printed"}();
	valid_tree(bpt, backup);
}

TEST_CASE("Header operations", "[btree]") {
	truncate_file("/tmp/eu-headerops");
	truncate_file("/tmp/eu-headerops-header");
	Btree bpt("/tmp/eu-headerops", "/tmp/eu-headerops-header");
	bpt.save();

	Btree bpt2("/tmp/eu-headerops", "/tmp/eu-headerops-header");
	bpt2.header().size() = 100;
	bpt2.header().depth() = 1;

	bpt2.load();

	REQUIRE(bpt.header() == bpt2.header());
}

TEST_CASE("Persistent tree", "[btree]") {
	truncate_file("/tmp/eu-persistent-btree");
	truncate_file("/tmp/eu-persistent-btree-header");

	std::map<typename DefaultConfig::Key, typename DefaultConfig::Val> backup;
	Position rootpos;
	{
		fmt::print(" --- Btree #1\n");
		Btree bpt("/tmp/eu-persistent-btree", "/tmp/eu-persistent-btree-header");
		backup = fill_tree(bpt, 1000);
		valid_tree(bpt, backup);

		bpt.save();
		rootpos = bpt.rootpos();
		fmt::print(" --- Btree #1 saved\n");
	}

	{
		fmt::print(" --- Btree #2\n");
		Btree bpt("/tmp/eu-persistent-btree", "/tmp/eu-persistent-btree-header", true);

		REQUIRE(bpt.rootpos() == rootpos);
		valid_tree(bpt, backup);
	}

	{
		fmt::print(" --- Btree #3\n");
		Btree bpt("/tmp/eu-persistent-btree", "/tmp/eu-persistent-btree-header", false);
		REQUIRE(bpt.size() == 0);
		for (auto &[key, _] : backup)
			REQUIRE(!bpt.contains(key));
	}
}

struct CustomConfigPrimitives : DefaultConfig {
	using Key = double;
	using Val = long long;
	using Ref = double;
};

TEST_CASE("Custom Config Btree Primitive Type", "[btree]") {
	truncate_file("/tmp/eu-btree-ops-custom-types");
	Btree<CustomConfigPrimitives> bpt("/tmp/eu-btree-ops-custom-types");
	static const std::size_t limit = 1000;

	fmt::print("NUM_RECORDS_LEAF = {}\n", bpt.num_records_leaf());
	fmt::print("NUM_RECORDS_BRANCH = {}\n", bpt.num_records_branch());

	std::map<CustomConfigPrimitives::Key, CustomConfigPrimitives::Val> backup;
	while (backup.size() != limit) {
		double key = item<float>();
		long long val = item<int>();
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
	smallstr name;
	int age{};
	smallstr email;

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
struct fmt::formatter<smallstr> {
	template<typename ParseContext>
	constexpr auto parse(ParseContext &ctx) {
		return ctx.begin();
	}

	template<typename FormatContext>
	auto format(const smallstr &s, FormatContext &ctx) {
		return fmt::format_to(ctx.out(), "{}", fmt::join(s.m_str, ""));
	}
};

template<>
Person item<Person>() {
	return Person{
	        .name = item<smallstr>(),
	        .age = item<int>(),
	        .email = item<smallstr>()};
}

struct CustomConfigAggregate : DefaultConfig {
	using Key = smallstr;
	using Val = Person;
	using Ref = smallstr;
};

TEST_CASE("Custom Config Btree Aggregate Type", "[btree]") {
	truncate_file("/tmp/eu-btree-custom-config");
	Btree<CustomConfigAggregate> bpt{"/tmp/eu-btree-custom-config"};
	bpt.NUM_RECORDS_LEAF = 83;

	fmt::print("NUM_RECORDS_LEAF = {}\n", bpt.num_records_leaf());
	fmt::print("NUM_RECORDS_BRANCH = {}\n", bpt.num_records_branch());

	static const std::size_t limit = 1000;
	auto backup = fill_tree(bpt, limit);

	util::BtreePrinter{bpt, "/tmp/eu-btree-aggregatetype-printed"}();
	valid_tree(bpt, backup);
}
