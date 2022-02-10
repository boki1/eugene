#include <map>
#include <ranges>
#include <string>
#include <variant>
#include <vector>

#include <catch2/catch.hpp>

#include <core/Util.h>
#include <core/storage/btree/Btree.h>
#include <core/storage/btree/BtreePrinter.h>

using namespace internal;
using namespace internal::storage::btree;
using internal::storage::Position;
using Bt = Btree<DefaultConfig>;

///
/// Various tree configuration used in the tests below
///

struct CustomConfigAggregate : DefaultConfig {
	using Key = smallstr;
	using Val = person;
	using Ref = smallstr;

	static inline constexpr int BRANCHING_FACTOR_LEAF = 83;
	static inline constexpr int BRANCHING_FACTOR_BRANCH = 84;
};

struct TreeFourConfig : DefaultConfig {
	/// Nodes will have 2 elements in leaves and 3 in branches
	static inline constexpr int BRANCHING_FACTOR_LEAF = 4;
	static inline constexpr int BRANCHING_FACTOR_BRANCH = 4;
};

struct CustomConfigPrimitives : DefaultConfig {
	using Key = double;
	using Val = long long;
	using Ref = double;
};

/// Enable debug printing in test suite
static constexpr bool EU_PRINTING_TESTS = false;

///
/// Utility functions
///

template<BtreeConfig Config>
auto fill_tree_with_random_items(Btree<Config> &bpt, const std::size_t limit = 1000) {
	using Key = typename Config::Key;
	using Val = typename Config::Val;
	std::map<Key, Val> backup;
	[[maybe_unused]] int i = 0;

	while (backup.size() != limit) {
		auto key = random_item<Key>();
		auto val = random_item<Val>();
		if (backup.contains(key)) {
			REQUIRE(bpt.contains(key));
			continue;
		}

		if (backup.size() == static_cast<unsigned long>(bpt.max_num_records_leaf())) {

		}

		bpt.insert(key, val);
		backup.emplace(key, val);
	}

	REQUIRE(bpt.size() == limit);
	return backup;
}

template<BtreeConfig Config>
void check_for_tree_backup_mismatch(Btree<Config> &bpt, const std::map<typename Config::Key, typename Config::Val> &backup) {
	REQUIRE(bpt.size() == backup.size());
	[[maybe_unused]] std::size_t i = 1;
	for (const auto &[key, val] : backup) {
//		if constexpr (EU_PRINTING_TESTS)
			fmt::print(" [{}]: '{}' => '{}'\n", i++, key, val);
		REQUIRE(bpt.get(key).value() == val);
	}
}

///
/// Unit tests
///

TEST_CASE("Prepare storage files") {
	/// Dummy test case
	std::ofstream{"/tmp/eu-btree-ops", std::ios::trunc};
	std::ofstream{"/tmp/eu-btree-ops-queries", std::ios::trunc};
	std::ofstream{"/tmp/eu-btree-custom-config", std::ios::trunc};
	std::ofstream{"/tmp/eu-btree-ops-custom-types", std::ios::trunc};
	std::ofstream{"/tmp/eu-persistent-btree", std::ios::trunc};
	std::ofstream{"/tmp/eu-persistent-btree-header", std::ios::trunc};
	std::ofstream{"/tmp/eu-headerops", std::ios::trunc};
	std::ofstream{"/tmp/eu-headerops-header", std::ios::trunc};
}

TEST_CASE("Btree operations", "[btree]") {
	SECTION("Empty tree") {
		Bt bpt("/tmp/eu-btree-ops");

		REQUIRE(bpt.contains(42) == false);
		REQUIRE(bpt.get(42).has_value() == false);
		REQUIRE(std::holds_alternative<Bt::RemovedNothing>(bpt.remove(42)));

		REQUIRE(bpt.sanity_check());
	}

	SECTION("Insertion") {
		Bt bpt("/tmp/eu-btree-ops");
		static const std::size_t limit = 983;
		auto backup = fill_tree_with_random_items(bpt, limit);

		if constexpr (EU_PRINTING_TESTS) {
			util::BtreePrinter{bpt, "/tmp/eu-btree-ops-printed"}();
		}

		check_for_tree_backup_mismatch(bpt, backup);
	}

	SECTION("Removal") {
		SECTION("Simple remove") {
			Bt bpt("/tmp/eu-btree-ops");
			for (int key = 0; key < bpt.max_num_records_leaf(); ++key)
				bpt.insert(key, key);

			for (int key = 0; key < bpt.max_num_records_leaf() / 2; ++key) {
				const auto removed = bpt.remove(key);
				REQUIRE(std::holds_alternative<Bt::RemovedVal>(removed));
				REQUIRE(std::get<Bt::RemovedVal>(removed).val == key);
			}
		}
		/*
		SECTION("Remove with single borrow from sibling leaf") {
			Btree<TreeFourConfig> treefour("/tmp/eu-btree-ops");
			treefour.insert(4, 4);
			treefour.insert(2, 2);
			treefour.insert(5, 5);
			treefour.insert(6, 6);
			treefour.insert(7, 7);

			util::BtreePrinter{treefour, "/tmp/eu-btree-ops-remove-with-borrow-1"}();

			// This should trigger a borrow from left node, meaning that. See the contents of '/tmp/eu-btree-ops-remove-with-borrow' for visual aid
			const auto removed_val = treefour.remove(2);
			REQUIRE(std::holds_alternative<RemovedVal>(removed_val));
			REQUIRE(std::get<RemovedVal>(removed_val).val == 2);

			util::BtreePrinter{treefour, "/tmp/eu-btree-ops-remove-with-borrow-2"}();

			Btree<TreeFourConfig> treefour2("/tmp/eu-btree-ops");
			treefour2.insert(4, 4);
			treefour2.insert(0, 0);
			treefour2.insert(6, 6);
			treefour2.insert(1, 1);
			treefour2.insert(5, 5);
			util::BtreePrinter{treefour2, "/tmp/eu-btree-ops-remove-with-borrow-3"}();

			const auto removed_val2 = treefour2.remove(6);
			REQUIRE(std::holds_alternative<RemovedVal>(removed_val2));
			REQUIRE(std::get<RemovedVal>(removed_val2).val == 6);

			util::BtreePrinter{treefour2, "/tmp/eu-btree-ops-remove-with-borrow-4"}();
		}

		SECTION("Remove with single borrow from sibling branch") {
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
			REQUIRE(std::holds_alternative<RemovedVal>(removed_val));
			REQUIRE(std::get<RemovedVal>(removed_val).val == 10);

			util::BtreePrinter{treefour, "/tmp/eu-btree-ops-remove-with-borrow-6"}();
		}

		SECTION("Remove with simple merge in leaves") {
			Btree<TreeFourConfig> treefour("/tmp/eu-btree-ops");
			treefour.insert(27, 27);
			treefour.insert(10, 10);
			treefour.insert(42, 42);
			treefour.insert(62, 62);
			treefour.insert(76, 76);
			treefour.insert(83, 83);

			util::BtreePrinter{treefour, "/tmp/eu-btree-ops-remove-with-merge-1"}();
			const auto removed = treefour.remove(62);
			REQUIRE(std::holds_alternative<RemovedVal>(removed));
			REQUIRE(std::get<RemovedVal>(removed).val == 62);
			util::BtreePrinter{treefour, "/tmp/eu-btree-ops-remove-with-merge-2"}();
		}

		SECTION("Remove with merge") {
			Btree<TreeFourConfig> treefour("/tmp/eu-btree-ops");
			auto backup = fill_tree_with_random_items(treefour, 1000);

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
				REQUIRE(std::holds_alternative<RemovedVa>(removed));
				REQUIRE(std::get<RemovedVal>(removed).val == backup.at(random_key));
				backup.erase(random_key);
			}
		}
		*/
	}

	SECTION("Update") {
		Bt bpt("/tmp/eu-btree-ops");
		static const std::size_t limit = 1000;
		auto backup = fill_tree_with_random_items(bpt, limit);

		for (int i = 0; i < 100; ++i) {
			if (random_item<bool>())
				continue;
			const auto key = random_item<int>();
			REQUIRE(backup.contains(key) == bpt.contains(key));
			if (!backup.contains(key))
				continue;
			const auto old_val = *bpt.get(key);
			bpt.update(key, key - 1);
			backup[key] = key - 1;
			if constexpr (EU_PRINTING_TESTS)
				fmt::print("Updating tree entry <{}, {}> to <{}, {}>\n", key, old_val, key, *bpt.get(key));
		}

		check_for_tree_backup_mismatch(bpt, backup);
	}

	SECTION("Queries") {
		Bt tree;
		Bt bpt("/tmp/eu-btree-ops-queries");
		std::vector<Bt::Entry> inserted_entries;
		const int limit = 100'000;
		for (const int item : std::ranges::views::iota(0, limit)) {
			inserted_entries.push_back(Bt::Entry{item, item});
			bpt.insert(item, item);
		}

		REQUIRE(*bpt.get_min_entry() == Bt::Entry{.key = 0, .val = 0});
		REQUIRE(*bpt.get_max_entry() == Bt::Entry{.key = limit - 1, .val = limit - 1});

		std::vector<Bt::Entry> fetched_entries;
		for (const Bt::Entry &item : bpt.get_all_entries())
			fetched_entries.push_back(item);
		REQUIRE(std::ranges::equal(fetched_entries, inserted_entries));

		std::vector<Bt::Entry> inserted_entries_with_odd_keys{inserted_entries};
		std::erase_if(inserted_entries_with_odd_keys, [](const Bt::Entry &entry) { return entry.key % 2 == 0; });

		std::vector<Bt::Entry> fetched_entries_with_odd_keys;
		for (const Bt::Entry &item : bpt.get_all_entries_filtered([](const Bt::Entry entry) { return entry.key % 2 != 0; }))
			fetched_entries_with_odd_keys.push_back(item);

		REQUIRE(std::ranges::equal(fetched_entries_with_odd_keys, inserted_entries_with_odd_keys));

		std::vector<Bt::Entry> inserted_entries_in_given_range{inserted_entries};
		std::erase_if(inserted_entries_in_given_range, [](const Bt::Entry &entry) { return entry.key < 65'900 || entry.key >= 66'000; });

		std::vector<Bt::Entry> fetched_entries_in_given_range;
		for (const Bt::Entry &item : bpt.get_all_entries_in_key_range(65'900, 66'000))
			fetched_entries_in_given_range.push_back(item);

		REQUIRE(std::ranges::equal(fetched_entries_in_given_range, inserted_entries_in_given_range));
	}
}

TEST_CASE("Header operations", "[btree]") {
	Bt bpt("/tmp/eu-headerops");
	bpt.save();

	Bt bpt2("/tmp/eu-headerops");

	for (int i = 0; i < bpt2.max_num_records_leaf() + 1; ++i)
		bpt2.insert(i, i);
	REQUIRE(bpt.header().tree_size != bpt2.header().tree_size);
	REQUIRE(bpt.header().tree_depth != bpt2.header().tree_depth);

	bpt2.load();
	REQUIRE(bpt.header().tree_size == bpt2.header().tree_size);
	REQUIRE(bpt.header().tree_depth == bpt2.header().tree_depth);
}

TEST_CASE("Persistent tree", "[btree]") {
	std::map<typename DefaultConfig::Key, typename DefaultConfig::Val> backup;
	Position rootpos;
	/*SECTION("Tree #1") */ {
		if constexpr (EU_PRINTING_TESTS)
			fmt::print(" --- Btree #1 start\n");
		Bt bpt("/tmp/eu-persistent-btree", ActionOnConstruction::Bare);
		backup = fill_tree_with_random_items(bpt, 1000);
		check_for_tree_backup_mismatch(bpt, backup);

		bpt.save();
		rootpos = bpt.rootpos();
		if constexpr (EU_PRINTING_TESTS)
			fmt::print(" --- Btree #1 saved\n");
	}
	/*SECTION("Tree #2") */ {
		if constexpr (EU_PRINTING_TESTS)
			fmt::print(" --- Btree #2 start\n");

		Bt bpt("/tmp/eu-persistent-btree", ActionOnConstruction::Load);

		REQUIRE(bpt.rootpos() == rootpos);
		check_for_tree_backup_mismatch(bpt, backup);
		if constexpr (EU_PRINTING_TESTS)
			fmt::print(" --- Btree #2 end (no save)\n");
	}
	/*SECTION("Tree #3") */ {
		if constexpr (EU_PRINTING_TESTS)
			fmt::print(" --- Btree #3 start\n");
		Bt bpt("/tmp/eu-persistent-btree", ActionOnConstruction::Bare);
		REQUIRE(bpt.size() == 0);
		for (auto &[key, _] : backup)
			REQUIRE(!bpt.contains(key));
		if constexpr (EU_PRINTING_TESTS)
			fmt::print(" --- Btree #3 end (no save)\n");
	}
}

TEST_CASE("Custom Config Btree Primitive Type", "[btree]") {
	Btree<CustomConfigPrimitives> bpt("/tmp/eu-btree-ops-custom-types");
	static const std::size_t limit = 1000;

	std::map<CustomConfigPrimitives::Key, CustomConfigPrimitives::Val> backup;
	while (backup.size() != limit) {
		double key = random_item<float>();
		long long val = random_item<int>();
		bpt.insert(key, val);
		backup.emplace(key, val);
	}

	util::BtreePrinter{bpt, "/tmp/eu-btree-ops-custom-types-printed"}();

	REQUIRE(bpt.size() == limit);

	[[maybe_unused]] std::size_t i = 1;

	for (const auto &[key, val] : backup) {
		if constexpr (EU_PRINTING_TESTS)
			fmt::print(" [{}]: '{}' => '{}'\n", i++, key, val);
		REQUIRE(bpt.get(key).value() == val);
	}
}

TEST_CASE("Custom Config Btree Aggregate Type", "[btree]") {
	Btree<CustomConfigAggregate> bpt{"/tmp/eu-btree-custom-config"};

	static const std::size_t limit = 1000;
	auto backup = fill_tree_with_random_items(bpt, limit);

	util::BtreePrinter{bpt, "/tmp/eu-btree-aggregatetype-printed"}();
	check_for_tree_backup_mismatch(bpt, backup);
}
