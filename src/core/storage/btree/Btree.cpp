#include <filesystem>
#include <map>
#include <ranges>
#include <string>
#include <variant>
#include <vector>

namespace fs = std::filesystem;

#include <catch2/catch.hpp>

#include <core/Util.h>
#include <core/storage/btree/Btree.h>
#include <core/storage/btree/BtreePrinter.h>

using namespace internal;
using namespace internal::storage::btree;
using internal::storage::Position;
using Bt = Btree<Config>;
using Nod = Node<Config>;
using Metadata = Nod::Metadata;
using Branch = Nod::Branch;
using Leaf = Nod::Leaf;

///
/// Various tree configuration used in the tests below
///

struct SmallstrToPerson : Config {
	using Key = smallstr;
	using Val = person;
	using RealVal = person;
	using Ref = smallstr;

	static inline constexpr int BRANCHING_FACTOR_LEAF = 83;
	static inline constexpr int BRANCHING_FACTOR_BRANCH = 84;
};

/// 2-3 tree: https://en.wikipedia.org/wiki/2-3_tree
struct Tree23 : Config {
	BTREE_OF_ORDER(4);
};

struct DoubleToLong : Config {
	using Key = float;
	using Val = int;
	using RealVal = int;
	using Ref = float;
};

struct IntToString : Config {
	using Key = int;
	using RealVal = std::string;

	static inline constexpr bool DYN_ENTRIES = true;
};

struct DoubleToVectorOfStrings : Config {
	using Key = double;
	using RealVal = std::vector<std::string>;

	static inline constexpr bool DYN_ENTRIES = true;
};

///
/// Utility functions
///

template<EugeneConfig C>
auto fill_tree_with_random_items(Btree<C> &bpt, const std::size_t limit = 1000) {
	using Key = typename C::Key;
	using RealVal = typename C::RealVal;
	std::map<Key, RealVal> backup;
	[[maybe_unused]] int i = 0;

	while (backup.size() != limit) {
		auto key = random_item<Key>();
		auto val = random_item<RealVal>();
		if (backup.contains(key)) {
			REQUIRE(bpt.contains(key));
			continue;
		}

		bpt.insert(key, val);
		backup.emplace(key, val);
	}

	return backup;
}

template<EugeneConfig C>
void check_for_tree_backup_mismatch(Btree<C> &bpt, const std::map<typename C::Key, typename C::RealVal> &backup) {
	REQUIRE(bpt.size() == backup.size());
	[[maybe_unused]] std::size_t i = 1;
	for (const auto &[key, val] : backup) {
		if constexpr (NDEBUG)
			fmt::print(" [{}]: '{}' => '{}'\n", i++, key, val);
		REQUIRE(bpt.get(key).value() == val);
	}
}

///
/// Unit tests
///

TEST_CASE("Btree operations", "[btree]") {
	fs::create_directories("/tmp/eugene-tests/btree-operations");

	SECTION("Create tree") {
		Bt bpt("/tmp/eugene-tests/btree-operations/empty-tree");
		REQUIRE(bpt.contains(42) == false);
		REQUIRE(bpt.get(42).has_value() == false);
		REQUIRE(std::holds_alternative<Bt::RemovedNothing>(bpt.remove(42)));
		REQUIRE(bpt.sanity_check());

		Bt mem_bpt("in-memory-tree-#1", ActionOnConstruction::InMemoryOnly);
		REQUIRE(mem_bpt.contains(42) == false);
		REQUIRE(mem_bpt.get(42).has_value() == false);
		REQUIRE(std::holds_alternative<Bt::RemovedNothing>(mem_bpt.remove(42)));
		REQUIRE(mem_bpt.sanity_check());
	}
	SECTION("Insertion") {
		SECTION("Insertion without rebalancing") {
			Bt bpt("/tmp/eugene-tests/btree-operations/insertion-without-rebalancing");
			fmt::print("Bt::max_num_records_leaf() = {}\n", bpt.max_num_records_leaf());
			static const std::size_t limit = bpt.max_num_records_leaf();
			auto backup = fill_tree_with_random_items(bpt, limit);

			if constexpr (NDEBUG)
				util::BtreePrinter{bpt, "/tmp/eugene-tests/btree-operations/insertion-without-rebalancing-printed"}();

			check_for_tree_backup_mismatch(bpt, backup);
		}
		SECTION("Difficult insertion") {
			Btree<Tree23> bpt("/tmp/eugene-tests/btree-operations/difficult-insertion");
			[[maybe_unused]] static const std::size_t limit = 10;
			fmt::print("keys-in-leaf: [{}]; [{}]\n", bpt.min_num_records_leaf(), bpt.max_num_records_leaf());
			fmt::print("keys-in-branch: [{}]; [{}]\n", bpt.min_num_records_branch(), bpt.max_num_records_branch());
			auto backup = fill_tree_with_random_items(bpt, limit);

			if constexpr (NDEBUG)
				util::BtreePrinter{bpt, "/tmp/eugene-tests/btree-operations/difficult-insertion-printed"}();

			check_for_tree_backup_mismatch(bpt, backup);
			const auto root_node = bpt.root();
			fmt::print("root_node.num_filled() = {}\n", root_node.num_filled());
		}
	}
	SECTION("Removal") {
		SECTION("Removal without rebalancing") {
			Btree<Tree23> bpt("/tmp/eugene-tests/btree-operations/removal-without-rebalancing");
			for (int key = 0; key < bpt.max_num_records_leaf(); ++key)
				bpt.insert(key, key);

			for (int key = 0; key < bpt.max_num_records_leaf() / 2; ++key) {
				const auto removed = bpt.remove(key);
				REQUIRE(std::holds_alternative<Btree<Tree23>::RemovedVal>(removed));
				REQUIRE(std::get<Btree<Tree23>::RemovedVal>(removed).val == key);
			}
		}
		SECTION("Difficult removal") {
			Btree<Tree23> bpt("/tmp/eugene-tests/btree-operations/difficult-removal");
			[[maybe_unused]] static const std::size_t limit = 10;
			auto backup = fill_tree_with_random_items(bpt, limit);

			util::BtreePrinter{bpt, "/tmp/eugene-tests/btree-operations/difficult-removal-printed-1"}();

			for (auto i = 0ul; i < limit; ++i) {
				const auto random_key = random_key_of_map(backup);
				const auto removed = bpt.remove(random_key);
				REQUIRE(std::holds_alternative<Btree<Tree23>::RemovedVal>(removed));
				REQUIRE(std::get<Btree<Tree23>::RemovedVal>(removed).val == backup.at(random_key));
				backup.erase(random_key);
			}

			REQUIRE(bpt.empty());

			util::BtreePrinter{bpt, "/tmp/eugene-tests/btree-operations/difficult-removal-printed-2"}();
		}
	}
	SECTION("Update") {
		Bt bpt("/tmp/eugene-tests/btree-operations/update");
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
			if constexpr (NDEBUG)
				fmt::print("Updating tree entry <{}, {}> to <{}, {}>\n", key, old_val, key, *bpt.get(key));
		}

		check_for_tree_backup_mismatch(bpt, backup);
	}
	SECTION("Queries") {
		Bt bpt("/tmp/eugene-tests/btree-operations/queries");
		std::vector<Bt::Entry> inserted_entries;
		const int limit = 1000;
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

TEST_CASE("Btree bulk operations", "[btree]") {
	fs::create_directories("/tmp/eugene-tests/btree-bulk-insertion");
	auto e = [](const auto &k) { return Btree<Tree23>::Entry{.key = k, .val = k}; };

	SECTION("Empty tree") {
		Btree<Tree23> bpt("/tmp/eugene-tests/btree-bulk-insertion/insert-many-empty");

		static const std::size_t limit = 10;
		std::vector<Btree<Tree23>::Entry> entries_to_insert;
		entries_to_insert.reserve(limit);
		std::generate_n(std::back_inserter(entries_to_insert), limit, [i = 0]() mutable { return Btree<Tree23>::Entry{.key = i, .val = i++}; });

		const auto res = bpt.insert_many(entries_to_insert);

		for (auto i = 0ul; i < limit; ++i) {
			REQUIRE(std::holds_alternative<Btree<Tree23>::InsertedEntry>(res.at(i)));
			REQUIRE(*bpt.get(entries_to_insert[i].key) == entries_to_insert[i].val);
		}

		util::BtreePrinter{bpt, "/tmp/eugene-tests/btree-bulk-insertion/insert-many-empty-printed"}();
	}

	SECTION("Simple bulk insertion without rebalancing") {
		Btree<Tree23> bpt("/tmp/eugene-tests/btree-bulk-insertion/insert-many-without-rebalancing");
		REQUIRE(bpt.size() == 0);
		bpt.insert_many(std::vector<Btree<Tree23>::Entry>{e(7), e(8), e(10), e(28), e(31), e(48), e(50), e(51)});
		REQUIRE(bpt.size() == 8);
		bpt.insert_many(std::vector<Btree<Tree23>::Entry>{e(13), e(15), e(16), e(17), e(18), e(19), e(20), e(23)});
		REQUIRE(bpt.size() == 16);

		std::vector<Btree<Tree23>::Entry> expected{e(7), e(8), e(10), e(28), e(31), e(48), e(50), e(51), e(13), e(15), e(16), e(17), e(18), e(19), e(20), e(23)};
		for (const auto &entry : expected)
			REQUIRE(bpt.get(entry.key).value() == entry.val);
		REQUIRE(bpt.size() == 16);
		util::BtreePrinter{bpt, "/tmp/eugene-tests/btree-bulk-insertion/insert-many-without-rebalancing-printed"}();
	}

	SECTION("Bulk removal") {
		Btree<Tree23> bpt("/tmp/eugene-tests/btree-bulk-insertion/remove-many");
		bpt.insert_many(std::vector<Btree<Tree23>::Entry>{e(7), e(8), e(10), e(28), e(31), e(48), e(50), e(51)});
		for (const auto &entry : std::vector<Btree<Tree23>::Entry>{e(7), e(8), e(10), e(28), e(31), e(48), e(50)})
			REQUIRE(bpt.get(entry.key).value() == entry.val);
		auto marks = bpt.remove_many(std::vector<Tree23::Key>{7, 8, 10, 28, 31, 48, 50, 51});
		REQUIRE_FALSE(marks.empty());
		for (auto &[key, mark] : marks) {
			REQUIRE(std::holds_alternative<Btree<Tree23>::RemovedVal>(mark));
			REQUIRE(std::get<Btree<Tree23>::RemovedVal>(mark).val == key);
		}
		REQUIRE(bpt.empty());
	}
}

TEST_CASE("Btree persistence", "[btree]") {
	fs::create_directories("/tmp/eugene-tests/btree-persistence");

	SECTION("Loading and recovery") {
		std::map<typename Config::Key, typename Config::Val> backup;
		Position rootpos;
		{
			if constexpr (NDEBUG)
				fmt::print(" --- Btree #1 start\n");
			Bt bpt("/tmp/eugene-tests/btree-persistence/loading-and-recovery", ActionOnConstruction::Bare);
			backup = fill_tree_with_random_items(bpt, 1000);
			check_for_tree_backup_mismatch(bpt, backup);

			bpt.save();
			rootpos = bpt.rootpos();
			if constexpr (NDEBUG)
				fmt::print(" --- Btree #1 saved\n");
		}
		{
			if constexpr (NDEBUG)
				fmt::print(" --- Btree #2 start\n");

			Bt bpt("/tmp/eugene-tests/btree-persistence/loading-and-recovery", ActionOnConstruction::Load);

			REQUIRE(bpt.rootpos() == rootpos);
			check_for_tree_backup_mismatch(bpt, backup);
			if constexpr (NDEBUG)
				fmt::print(" --- Btree #2 end (no save)\n");
		}
		{
			if constexpr (NDEBUG)
				fmt::print(" --- Btree #3 start\n");
			Bt bpt("/tmp/eugene-tests/btree-persistence/loading-and-recovery", ActionOnConstruction::Bare);
			REQUIRE(bpt.size() == 0);
			for (auto &[key, _] : backup)
				REQUIRE(!bpt.contains(key));
			if constexpr (NDEBUG)
				fmt::print(" --- Btree #3 end (no save)\n");
		}
	}

	SECTION("Header operations") {
		Bt bpt("/tmp/eugene-tests/btree-persistence/header-operations");
		bpt.save();

		Bt bpt2("/tmp/eugene-tests/btree-persistence/header-operations");

		for (int i = 0; i < bpt2.max_num_records_leaf() + 1; ++i)
			bpt2.insert(i, i);
		REQUIRE(bpt.header().tree_size != bpt2.header().tree_size);
		REQUIRE(bpt.header().tree_depth != bpt2.header().tree_depth);

		bpt2.load();
		REQUIRE(bpt.header().tree_size == bpt2.header().tree_size);
		REQUIRE(bpt.header().tree_depth == bpt2.header().tree_depth);
	}
}

TEST_CASE("Btree configs") {
	fs::create_directories("/tmp/eugene-tests/btree-configs");

	SECTION("Primitive types") {
		Btree<DoubleToLong> bpt("/tmp/eugene-tests/btree-configs/primitive-types");
		static const std::size_t limit = 100;

		std::map<DoubleToLong::Key, DoubleToLong::Val> backup;
		while (backup.size() != limit) {
			double key = random_item<float>();
			long long val = random_item<int>();
			bpt.insert(key, val);
			backup.emplace(key, val);
		}

		util::BtreePrinter{bpt, "/tmp/eugene-tests/btree-configs/primitive-types-printed"}();

		REQUIRE(bpt.size() == limit);

		[[maybe_unused]] std::size_t i = 1;

		for (const auto &[key, val] : backup) {
			if constexpr (NDEBUG)
				fmt::print(" [{}]: '{}' => '{}'\n", i++, key, val);
			REQUIRE(bpt.get(key).value() == val);
		}
	}

	SECTION("User-defined types") {
		Btree<SmallstrToPerson> bpt{"/tmp/eugene-tests/btree-configs/aggregate-types"};

		static const std::size_t limit = 100;
		auto backup = fill_tree_with_random_items(bpt, limit);

		util::BtreePrinter{bpt, "/tmp/eugene-tests/btree-configs/aggregate-types-printed"}();
		check_for_tree_backup_mismatch(bpt, backup);
	}
}

TEST_CASE("Btree utils") {
	static constexpr auto BRANCH_NUM = 100;

	SECTION("Node associated") {
		Btree<Config> bpt{"/tmp/eugene-tests/btree-utils/node-associated"};

		std::vector<Position> branch_links_left(BRANCH_NUM + 1);
		std::generate(branch_links_left.begin(), branch_links_left.end(), [&] { return bpt.pager().alloc(); });

		std::vector<Position> branch_links_right(BRANCH_NUM + 1);
		std::generate(branch_links_right.begin(), branch_links_right.end(), [&] { return bpt.pager().alloc(); });

		auto branch_node_left  = Nod{Metadata(Branch(n_random_items<int>(BRANCH_NUM), std::move(branch_links_left), std::vector<LinkStatus>(BRANCH_NUM + 1, LinkStatus::Valid))), {}, Nod::RootStatus::IsInternal};
		auto branch_node_right = Nod{Metadata(Branch(n_random_items<int>(BRANCH_NUM), std::move(branch_links_right), std::vector<LinkStatus>(BRANCH_NUM + 1, LinkStatus::Valid))), {}, Nod::RootStatus::IsInternal};
		branch_node_left.branch().link_status[17] = LinkStatus::Inval;
		branch_node_left.branch().link_status[38] = LinkStatus::Inval;
		branch_node_left.branch().link_status[46] = LinkStatus::Inval;
		branch_node_right.branch().link_status[27] = LinkStatus::Inval;
		branch_node_right.branch().link_status[36] = LinkStatus::Inval;
		branch_node_right.branch().link_status[78] = LinkStatus::Inval;

		std::sort(branch_node_left.branch().refs.begin(), branch_node_left.branch().refs.end());
		std::sort(branch_node_right.branch().refs.begin(), branch_node_right.branch().refs.end());

		auto merged = branch_node_left.fuse_with(branch_node_right);
		const auto merge_pos = bpt.pager().alloc();
		bpt.pager().place(merge_pos, merged.make_page());

		auto link_status_cbegin = merged.branch().links.cbegin();
		auto empty_node = Nod{Metadata(Branch()), {}, Nod::RootStatus::IsInternal};
		for (auto link_cbegin = merged.branch().links.cbegin(); link_cbegin != merged.branch().links.cend(); ++link_cbegin) {
			if (*link_status_cbegin)
				bpt.pager().place(*link_cbegin, empty_node.make_page());
			++link_status_cbegin;
		}

		bpt.fix_sibling_links(merged, {0xABCD});

		std::vector<Position> sibling_links;

		std::vector<Position> parent_links{merged.branch().links};
		for (const auto link : std::vector<Position>{
		             branch_node_left.branch().links[17],
		             branch_node_left.branch().links[38],
		             branch_node_left.branch().links[46],
		             branch_node_right.branch().links[27],
		             branch_node_right.branch().links[36],
		             branch_node_right.branch().links[78]}) {
			parent_links.erase(std::find(parent_links.cbegin(), parent_links.cend(), link));
		}
		parent_links.push_back(0xABCD);
		REQUIRE(std::equal(sibling_links.cbegin(), sibling_links.cend(), parent_links.cbegin()));
	}
}

TEST_CASE("Btree dyn entries") {
	fs::create_directories("/tmp/eugene-tests/btree-dyn");

	SECTION("std::string's") {
		using Tree = Btree<IntToString>;
		auto backup = [] {
			Tree bpt{"/tmp/eugene-tests/btree-dyn/strings", ActionOnConstruction::Bare};
			auto backup = fill_tree_with_random_items(bpt, 1000);
			check_for_tree_backup_mismatch(bpt, backup);
			bpt.save();
			fmt::print("saved\n");
			return backup;
		}();

		Tree bpt{"/tmp/eugene-tests/btree-dyn/strings", ActionOnConstruction::Load};
		check_for_tree_backup_mismatch(bpt, backup);

		for (auto i = 0ul; i < 100; ++i) {
			const auto random_key = random_key_of_map(backup);
			const auto removed = bpt.remove(random_key);
			REQUIRE(std::holds_alternative<Tree::RemovedVal>(removed));
			REQUIRE(std::get<Tree::RemovedVal>(removed).val == backup.at(random_key));
			backup.erase(random_key);
		}

		check_for_tree_backup_mismatch(bpt, backup);
	}
}
