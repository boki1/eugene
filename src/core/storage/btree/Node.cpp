#include <algorithm>
#include <fstream>
#include <optional>
#include <random>
#include <ranges>
#include <unordered_map>

#include "catch2/catch.hpp"

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <core/Util.h>
#include <core/storage/Pager.h>
#include <core/storage/btree/Btree.h>

using namespace std::ranges::views;
using namespace internal::storage;
using namespace internal::storage::btree;
using namespace internal;

using Nod = Node<DefaultConfig>;
using Metadata = Nod::Metadata;
using Branch = Nod::Branch;
using Leaf = Nod::Leaf;

#define PRINT_LEAF(node) \
	fmt::print(#node" = (keys: {}, vals: {})\n", fmt::join(node.keys, " "), fmt::join(node.vals, " "))

#define PRINT_BRANCH(node) \
	fmt::print(#node" = (refs: {}, links: {})\n", fmt::join(node.refs, " "), fmt::join(node.links, " "))

namespace internal {
template<>
Nod random_item<Nod>() {
	static std::random_device dev;
	std::mt19937 rng(dev());
	std::uniform_int_distribution<std::mt19937::result_type> dist128(1, 128);

	std::vector<Position> a;
	std::vector<DefaultConfig::Key> b;
	for (std::size_t i = 0; i < dist128(rng); ++i)
		if (dist128(rng) % 2)
			a.push_back(dist128(rng));
		else
			b.push_back((int) Position(dist128(rng)));

	Metadata metadata;
	if (dist128(rng) % 2)
		metadata = Metadata(Branch(std::move(b), std::move(a), {}));
	else
		metadata = Metadata(Leaf(std::move(b), std::move(b)));

	Position p;
	if (auto pp = dist128(rng); pp < 75)
		p = pp;

	return Node(std::move(metadata), p, dist128(rng) % 2 ? Nod::RootStatus::IsInternal : Nod::RootStatus::IsRoot);
}
}// namespace internal

TEST_CASE("Prepare storage files") {
	/// Dummy test case
	std::ofstream{"/tmp/eu-persistent-nodes-pager", std::ios::trunc};
	std::ofstream{"/tmp/eu-many-persistent-nodes-pager", std::ios::trunc};
}

TEST_CASE("Node serialization", "[btree]") {
	auto node1 = Nod(Metadata(Branch({1}, {1}, {})), {}, Nod::RootStatus::IsInternal);
	auto node2 = Nod(Metadata(Leaf({2}, {2})), 13, Nod::RootStatus::IsInternal);

	auto node1_as_page = node1.make_page();
	auto node2_as_page = node2.make_page();

	auto node1_from_page = Nod::from_page(node1_as_page);
	auto node2_from_page = Nod::from_page(node2_as_page);

	REQUIRE(node1_from_page == node1);
	REQUIRE(node2_from_page == node2);
}

TEST_CASE("Persistent nodes", "[btree]") {
	Pager pr("/tmp/eu-persistent-nodes-pager");

	auto node1_pos = pr.alloc();
	auto node1 = Nod(Metadata(Branch({1}, {1}, {})), {}, Nod::RootStatus::IsInternal);
	auto node1_as_page = node1.make_page();
	pr.place(node1_pos, std::move(node1_as_page));
	auto node1_from_page = Nod::from_page(pr.get(node1_pos));
	REQUIRE(node1_from_page == node1);
	REQUIRE(node1_from_page.is_root() == false);

	auto node2_pos = pr.alloc();
	auto node2 = Nod(Metadata(Leaf({2}, {2})), 13, Nod::RootStatus::IsRoot);
	auto node2_as_page = node2.make_page();
	pr.place(node2_pos, std::move(node2_as_page));
	auto node2_from_page = Nod::from_page(pr.get(node2_pos));
	REQUIRE(node2_from_page == node2);
	REQUIRE(node2_from_page.is_root() == true);
	REQUIRE(node2_from_page.parent() == Position(13));
}

TEST_CASE("Paging with many random nodes", "[btree]") {
	Pager pr("/tmp/eu-many-persistent-nodes-pager");

	std::unordered_map<Position, Nod> nodes;
	for (std::size_t i = 0; i < 128; ++i) {
		auto node = random_item<Nod>();
		auto node_pos = pr.alloc();
		nodes[node_pos] = Node<Config>{node};
		auto node_as_page = node.make_page();
		pr.place(node_pos, std::move(node_as_page));
		auto node_from_page = Nod::from_page(pr.get(node_pos));
		REQUIRE(node_from_page == node);
	}

	auto random_node = [&] {
		static std::random_device dev;
		static std::mt19937 rng(dev());
		std::uniform_int_distribution<std::mt19937::result_type> dist(0, nodes.size() - 1);

		auto random_it = std::next(std::begin(nodes), dist(rng));
		return std::tie(random_it->first, random_it->second);
	};

	for (std::size_t i = 0; i < nodes.size(); ++i) {
		auto [pos, node] = random_node();
		auto node_from_page = Nod::from_page(pr.get(pos));
		REQUIRE(node_from_page == node);
	}
}

TEST_CASE("Split full nodes", "[btree]") {
	static constexpr auto BRANCH_NUM = 6;
	static constexpr auto LEAF_NUM = 6;

	[[maybe_unused]] static constexpr auto BRANCH_LIMIT = 5;
	[[maybe_unused]] static constexpr auto LEAF_LIMIT = 5;

	std::vector<Position> branch_links(BRANCH_NUM + 1);
	std::iota(branch_links.begin(), branch_links.end(), static_cast<Position>(0ul));

	auto branch_node = Nod{Metadata(Branch(n_random_items<int>(BRANCH_NUM), std::move(branch_links), std::vector<LinkStatus>(BRANCH_NUM + 1, LinkStatus::Valid))), {}, Nod::RootStatus::IsInternal};
	g_i = 0;
	auto leaf_node = Nod{Metadata(Leaf(n_random_items<int>(LEAF_NUM), n_random_items<int>(LEAF_NUM))), {}, Nod::RootStatus::IsInternal};
	auto &branch = branch_node.branch();
	auto &leaf = leaf_node.leaf();

	const auto branch_before_split = Nod::Branch{branch_node.branch()};
	const auto leaf_before_split = Nod::Leaf{leaf_node.leaf()};

	auto validate_branch = [](const Nod::Branch &branch_before_split, const Nod::Branch &left, const Nod::Branch &right, const std::size_t pivot_idx, const auto midkey) {
		if constexpr (NDEBUG) {
			fmt::print("branch_pivot = {}\n", pivot_idx);
			fmt::print("branch_midkey = {}\n", midkey);
			PRINT_BRANCH(branch_before_split);
			PRINT_BRANCH(left);
			PRINT_BRANCH(right);
			fmt::print("\n");
		}

		REQUIRE(midkey == branch_before_split.refs[pivot_idx]);
		REQUIRE(std::equal(branch_before_split.refs.cbegin(), branch_before_split.refs.cbegin() + pivot_idx, left.refs.cbegin()));
		REQUIRE(std::equal(branch_before_split.links.cbegin(), branch_before_split.links.cbegin() + pivot_idx + 1, left.links.cbegin()));
		REQUIRE(std::equal(branch_before_split.link_status.cbegin(), branch_before_split.link_status.cbegin() + pivot_idx + 1, left.link_status.cbegin()));
		REQUIRE(std::equal(branch_before_split.refs.cbegin() + pivot_idx + 1, branch_before_split.refs.cend(), right.refs.cbegin()));
		REQUIRE(std::equal(branch_before_split.links.cbegin() + pivot_idx + 1, branch_before_split.links.cend(), right.links.cbegin()));
		REQUIRE(std::equal(branch_before_split.link_status.cbegin() + pivot_idx + 1, branch_before_split.link_status.cend(), right.link_status.cbegin()));
	};

	auto validate_leaf = [](const Nod::Leaf &leaf_before_split, const Nod::Leaf &left, const Nod::Leaf &right, const std::size_t pivot_idx, const auto midkey) {
		if constexpr (NDEBUG) {
			fmt::print("leaf_pivot = {}\n", pivot_idx);
			fmt::print("leaf_midkey = {}\n", midkey);
			PRINT_LEAF(leaf_before_split);
			PRINT_LEAF(left);
			PRINT_LEAF(right);
			fmt::print("\n");
		}

 		REQUIRE(midkey == leaf_before_split.keys[pivot_idx]);
		REQUIRE(std::equal(leaf_before_split.keys.cbegin(), leaf_before_split.keys.cbegin() + pivot_idx + 1, left.keys.cbegin()));
		REQUIRE(std::equal(leaf_before_split.vals.cbegin(), leaf_before_split.vals.cbegin() + pivot_idx + 1, left.vals.cbegin()));
		REQUIRE(std::equal(leaf_before_split.keys.cbegin() + pivot_idx, leaf_before_split.keys.cend(), right.keys.cbegin()));
		REQUIRE(std::equal(leaf_before_split.vals.cbegin() + pivot_idx, leaf_before_split.vals.cend(), right.vals.cbegin()));
	};

	SECTION("Even distribution of entries") {
		auto [branch_midkey, branch_sib_node] = branch_node.split(BRANCH_LIMIT, SplitBias::DistributeEvenly);
		validate_branch(branch_before_split, branch, branch_sib_node.branch(), (BRANCH_LIMIT + 1) / 2, branch_midkey);

		auto [leaf_midkey, leaf_sib_node] = leaf_node.split(LEAF_LIMIT, SplitBias::DistributeEvenly);
		validate_leaf(leaf_before_split, leaf, leaf_sib_node.leaf(), (LEAF_LIMIT + 1) / 2, leaf_midkey);
	}

	SECTION("Left leaning distribution of entries") {
		auto [branch_midkey, branch_sib_node] = branch_node.split(BRANCH_LIMIT, SplitBias::LeanLeft);
		validate_branch(branch_before_split, branch, branch_sib_node.branch(), BRANCH_LIMIT - 1, branch_midkey);

		auto [leaf_midkey, leaf_sib_node] = leaf_node.split(LEAF_LIMIT, SplitBias::LeanLeft);
		validate_leaf(leaf_before_split, leaf, leaf_sib_node.leaf(), LEAF_LIMIT - 1, leaf_midkey);
	}

	SECTION("Right leaning distribution of entries") {
		auto [branch_midkey, branch_sib_node] = branch_node.split(BRANCH_LIMIT, SplitBias::LeanRight);
		validate_branch(branch_before_split, branch, branch_sib_node.branch(), std::abs(BRANCH_LIMIT - BRANCH_NUM) + 1, branch_midkey);

		auto [leaf_midkey, leaf_sib_node] = leaf_node.split(LEAF_LIMIT, SplitBias::LeanRight);
		validate_leaf(leaf_before_split, leaf, leaf_sib_node.leaf(), std::abs(LEAF_LIMIT - LEAF_NUM) + 1, leaf_midkey);
	}
}
