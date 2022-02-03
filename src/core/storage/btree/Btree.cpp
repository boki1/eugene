#include <map>
#include <random>
#include <string>
#include <variant>
#include <vector>

#include <catch2/catch.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <nop/structure.h>

#include <core/storage/btree/Btree.h>
#include <core/storage/btree/BtreePrinter.h>

using namespace internal::storage::btree;
using internal::storage::Position;

#undef EU_PRINTING_TESTS

template<typename T>
T item();

template<>
int item<int>() {
	static int i = 0;
	return i++;

	//	static std::random_device dev;
	//	static std::mt19937 rng(dev());
	//	static std::uniform_int_distribution<std::mt19937::result_type> dist(1, 10000000);
	//	return dist(rng);
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
auto fill_tree(Btree<Config> &bpt, const std::size_t limit = 1000) {
	using Key = typename Config::Key;
	using Val = typename Config::Val;
	std::map<Key, Val> backup;
#ifdef EU_PRINTING_TESTS
	int i = 0;
#endif

	while (backup.size() != limit) {
		auto key = item<Key>();
		auto val = item<Val>();
		if (backup.contains(key)) {
			REQUIRE(bpt.contains(key));
			continue;
		}

#ifdef EU_PRINTING_TESTS
		fmt::print("Element #{} inserted.\n", ++i);
#endif

		bpt.insert(key, val);
		backup.emplace(key, val);
	}

	REQUIRE(bpt.size() == limit);
	return backup;
}

template<BtreeConfig Config>
void valid_tree(Btree<Config> &bpt, const std::map<typename Config::Key, typename Config::Val> &backup) {
	REQUIRE(bpt.size() == backup.size());
#ifdef EU_PRINTING_TESTS
	std::size_t i = 1;
#endif
	for (const auto &[key, val] : backup) {
#ifdef EU_PRINTING_TESTS
		fmt::print(" [{}]: '{}' => '{}'\n", i++, key, val);
#endif
		REQUIRE(bpt.get(key).value() == val);
	}
}

void truncate_file(std::string_view fname) {
	std::ofstream of{std::string{fname}, std::ios::trunc};
}

struct smallstr {
	static constexpr auto sz = 10;
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

struct TreeFourConfig : DefaultConfig {
	/// Nodes will have 2 elements in leaves and 3 in branches
	static inline constexpr int BRANCHING_FACTOR_LEAF = 4;
	static inline constexpr int BRANCHING_FACTOR_BRANCH = 4;
};

TEST_CASE("Btree operations", "[btree]") {
	SECTION("Empty tree") {
		truncate_file("/tmp/eu-btree-ops");
		Btree bpt("/tmp/eu-btree-ops");
		REQUIRE(bpt.contains(42) == false);
		REQUIRE(bpt.get(42).has_value() == false);
		REQUIRE(std::holds_alternative<RemovedNothing>(bpt.remove(42)));

		static const std::size_t limit = 1000;
		auto backup = fill_tree(bpt, limit);
	}

	SECTION("Insertion") {
		truncate_file("/tmp/eu-btree-ops");
		Btree bpt("/tmp/eu-btree-ops");
		static const std::size_t limit = 1000;
		auto backup = fill_tree(bpt, limit);

#ifdef EU_PRINTING_TESTS
		util::BtreePrinter{bpt, "/tmp/eu-btree-ops-printed"}();
#endif
		valid_tree(bpt, backup);
	}

	SECTION("Removal") {
		SECTION("Simple remove") {
			truncate_file("/tmp/eu-btree-ops");
			Btree bpt("/tmp/eu-btree-ops");
			for (int key = 0; key < bpt.max_num_records_leaf(); ++key)
				bpt.insert(key, key);

			for (int key = 0; key < bpt.max_num_records_leaf() / 2; ++key) {
				const auto removed = bpt.remove(key);
				REQUIRE(std::holds_alternative<RemovedVal<Config::Val>>(removed));
				REQUIRE(std::get<RemovedVal<Config::Val>>(removed).val == key);
			}
		}
		/*
		SECTION("Remove with single borrow from sibling leaf") {
			truncate_file("/tmp/eu-btree-ops");
			Btree<TreeFourConfig> treefour("/tmp/eu-btree-ops");
			treefour.insert(4, 4);
			treefour.insert(2, 2);
			treefour.insert(5, 5);
			treefour.insert(6, 6);
			treefour.insert(7, 7);

			util::BtreePrinter{treefour, "/tmp/eu-btree-ops-remove-with-borrow-1"}();

			// This should trigger a borrow from left node, meaning that. See the contents of '/tmp/eu-btree-ops-remove-with-borrow' for visual aid
			const auto removed_val = treefour.remove(2);
			REQUIRE(std::holds_alternative<RemovedVal<Config::Val>>(removed_val));
			REQUIRE(std::get<RemovedVal<Config::Val>>(removed_val).val == 2);

			util::BtreePrinter{treefour, "/tmp/eu-btree-ops-remove-with-borrow-2"}();

			Btree<TreeFourConfig> treefour2("/tmp/eu-btree-ops");
			treefour2.insert(4, 4);
			treefour2.insert(0, 0);
			treefour2.insert(6, 6);
			treefour2.insert(1, 1);
			treefour2.insert(5, 5);
			util::BtreePrinter{treefour2, "/tmp/eu-btree-ops-remove-with-borrow-3"}();

			const auto removed_val2 = treefour2.remove(6);
			REQUIRE(std::holds_alternative<RemovedVal<Config::Val>>(removed_val2));
			REQUIRE(std::get<RemovedVal<Config::Val>>(removed_val2).val == 6);

			util::BtreePrinter{treefour2, "/tmp/eu-btree-ops-remove-with-borrow-4"}();
		}

		SECTION("Remove with single borrow from sibling branch") {
			truncate_file("/tmp/eu-btree-ops");
			Btree<TreeFourConfig> treefour("/tmp/eu-btree-ops");
			treefour.insert(11, 11);
			treefour.insert(12, 12);
			treefour.insert(1, 1);
			treefour.insert(9, 9);
			treefour.insert(14, 14);
			treefour.insert(15, 15);
			treefour.insert(10, 10);
			treefour.insert(4, 4);

			util::BtreePrinter{treefour, "/tmp/eu-btree-ops-remove-with-borrow-5"}();

			const auto removed_val = treefour.remove(10);
			REQUIRE(std::holds_alternative<RemovedVal<Config::Val>>(removed_val));
			REQUIRE(std::get<RemovedVal<Config::Val>>(removed_val).val == 10);

			util::BtreePrinter{treefour, "/tmp/eu-btree-ops-remove-with-borrow-6"}();
		}

		SECTION("Remove with simple merge in leaves") {
			truncate_file("/tmp/eu-btree-ops");
			Btree<TreeFourConfig> treefour("/tmp/eu-btree-ops");
			treefour.insert(27, 27);
			treefour.insert(10, 10);
			treefour.insert(42, 42);
			treefour.insert(62, 62);
			treefour.insert(76, 76);
			treefour.insert(83, 83);

			util::BtreePrinter{treefour, "/tmp/eu-btree-ops-remove-with-merge-1"}();
			const auto removed = treefour.remove(62);
			REQUIRE(std::holds_alternative<RemovedVal<Config::Val>>(removed));
			REQUIRE(std::get<RemovedVal<Config::Val>>(removed).val == 62);
			util::BtreePrinter{treefour, "/tmp/eu-btree-ops-remove-with-merge-2"}();
		}

		SECTION("Remove with merge") {
			truncate_file("/tmp/eu-btree-ops");
			Btree<TreeFourConfig> treefour("/tmp/eu-btree-ops");
			auto backup = fill_tree(treefour, 1000);

			for (int i = 0; i < 500; ++i) {
				auto random_key = [&] {
					std::random_device dev;
					std::mt19937_64 rng(dev());

					std::uniform_int_distribution<size_t> dist(0, backup.size() - 1);
					auto random_pair = backup.begin();
					std::advance(random_pair, dist(rng));
					return random_pair->first;
				}();
				const auto removed = treefour.remove(random_key);
				REQUIRE(std::holds_alternative<RemovedVal<Config::Val>>(removed));
				REQUIRE(std::get<RemovedVal<Config::Val>>(removed).val == backup.at(random_key));
				backup.erase(random_key);
			}
		}
		*/
	}

	SECTION("Update") {
		truncate_file("/tmp/eu-btree-ops");
		Btree bpt("/tmp/eu-btree-ops");
		static const std::size_t limit = 1000;
		auto backup = fill_tree(bpt, limit);

		/// Perform at most 100 updates and check
		for (int i = 0; i < 100; ++i) {
			if (item<bool>())
				continue;
			const auto key = item<int>();
			REQUIRE(backup.contains(key) == bpt.contains(key));
			if (!backup.contains(key))
				continue;
			bpt.update(key, key - 1);
			backup[key] = key - 1;
		}

		valid_tree(bpt, backup);
	}
}

TEST_CASE("Header operations", "[btree]") {
	truncate_file("/tmp/eu-headerops");
	truncate_file("/tmp/eu-headerops-header");
	Btree bpt("/tmp/eu-headerops");
	bpt.save();

	Btree bpt2("/tmp/eu-headerops");
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
#ifdef EU_PRINTING_TESTS
		fmt::print(" --- Btree #1 start\n");
#endif
		Btree bpt("/tmp/eu-persistent-btree");
		backup = fill_tree(bpt, 1000);
		valid_tree(bpt, backup);

		bpt.save();
		rootpos = bpt.rootpos();
#ifdef EU_PRINTING_TESTS
		fmt::print(" --- Btree #1 saved\n");
#endif
	}

	{
#ifdef EU_PRINTING_TESTS
		fmt::print(" --- Btree #2 start\n");
#endif
		Btree bpt("/tmp/eu-persistent-btree", true);

		REQUIRE(bpt.rootpos() == rootpos);
		valid_tree(bpt, backup);
#ifdef EU_PRINTING_TESTS
		fmt::print(" --- Btree #2 end (no save)\n");
#endif
	}

	{
#ifdef EU_PRINTING_TESTS
		fmt::print(" --- Btree #3 start\n");
#endif
		Btree bpt("/tmp/eu-persistent-btree");
		REQUIRE(bpt.size() == 0);
		for (auto &[key, _] : backup)
			REQUIRE(!bpt.contains(key));
#ifdef EU_PRINTING_TESTS
		fmt::print(" --- Btree #3 end (no save)\n");
#endif
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

	std::map<CustomConfigPrimitives::Key, CustomConfigPrimitives::Val> backup;
	while (backup.size() != limit) {
		double key = item<float>();
		long long val = item<int>();
		bpt.insert(key, val);
		backup.emplace(key, val);
	}

	util::BtreePrinter{bpt, "/tmp/eu-btree-ops-custom-types-printed"}();

	REQUIRE(bpt.size() == limit);

#ifdef EU_PRINTING_TESTS
	std::size_t i = 1;
#endif

	for (const auto &[key, val] : backup) {
#ifdef EU_PRINTING_TESTS
		fmt::print(" [{}]: '{}' => '{}'\n", i++, key, val);
#endif
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

	static const std::size_t limit = 1000;
	auto backup = fill_tree(bpt, limit);

	util::BtreePrinter{bpt, "/tmp/eu-btree-aggregatetype-printed"}();
	valid_tree(bpt, backup);
}
