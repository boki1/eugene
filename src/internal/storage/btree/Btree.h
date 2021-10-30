#pragma once

#include <internal/storage/Storage.h>

#include <algorithm>    // std::min
#include <array>        // std::array
#include <cassert>      // assert
#include <cmath>        // std::ceil
#include <memory>       // std::unique_ptr
#include <optional>     // std::optional
#include <tuple>        // std::tie
#include <unordered_map>// std::unordered_map
#include <utility>      // std::move, std::pair
#include <variant>      // std::variant

template<typename T>
consteval bool PowerOf2(T num) {
	return (num & (num - 1)) == 0;
}

#define unsafe_ /* marks function as not noexcept */

template<typename T>
using optional_ref = std::optional<std::reference_wrapper<T>>;

template<typename T>
using optional_cref = std::optional<std::reference_wrapper<const T>>;

namespace internal::btree {

using namespace storage;

template<typename Config>
concept BtreeConfig = requires(Config conf) {
	typename Config::Key;
	typename Config::KeyRef;
	typename Config::Val;
	typename Config::StorageDev;

	requires std::convertible_to<decltype(conf.ApplyCompression), bool>;
	requires StorageDevice<typename Config::StorageDev>;
	requires std::unsigned_integral<decltype(conf.BtreeNodeSize)>;
}
&&PowerOf2(Config::BtreeNodeSize);

struct DefaultBtreeConfig {
	using Key = uint32_t;
	using KeyRef = Key;
	using Val = uint32_t;
	using StorageDev = DefaultStorageDev;
	static constexpr unsigned BtreeNodeSize = 1 << 10;
	static constexpr bool ApplyCompression = false;
};

namespace util {
template<BtreeConfig Config>
class BtreeYAMLPrinter;
}

template<typename Config = DefaultBtreeConfig>
requires BtreeConfig<Config>
class Btree final {
private:
	using Key = typename Config::Key;
	using KeyRef = typename Config::KeyRef;
	using Val = typename Config::Val;
	using Storage = typename Config::StorageDev;

	static constexpr bool ApplyCompression = Config::ApplyCompression;
	static constexpr uint32_t BtreeNodeSize = Config::BtreeNodeSize;

public:
	class Node;
	friend Node;

	class Iterator;
	friend Iterator;

	friend util::BtreeYAMLPrinter<Config>;

public:
	Btree() = default;

	explicit Btree(Storage &storage)
	    : m_storage{storage} {}

	void prepare_root_for_inmem() {
		auto inmem_initial_root = std::make_unique<Node>(std::move(Node::RootDefault(*this)));
		m_root = &Node::Cache::the().put(inmem_initial_root->self(), std::move(inmem_initial_root));
		assert(m_root != nullptr);
	}

public:// API
	Iterator begin() noexcept { return Iterator::begin(*this); }

	Iterator end() noexcept { return Iterator::end(*this); }

	Iterator insert(const Key &key, const Val &val) noexcept {
		Node *cur = m_root;
		while (true) {
			auto [it, next] = cur->find(key);
			if (next)
				++it;
			if (cur->is_leaf())
				return cur->insert(key, val, it);
			if (it == cur->end())
				break;

			const auto links = it->branch().links_;
			const auto pos = Position{links[it.index()]};
			cur = &Node::Cache::the().fetch(pos);
		}

		return end();
	}

	std::optional<Iterator> find(const Key &target) noexcept {
		Node *cur = m_root;
		while (true) {
			auto [it, next] = cur->find(target);
			if (cur->is_leaf()) {
				if (!next)
					break;
				return std::make_optional(it);
			}

			if (next)
				++it;

			const auto links = it->branch().links_;
			const auto pos = Position{links[it.index()]};
			cur = &Node::Cache::the().fetch(pos);
		}

		return std::nullopt;
	}

	optional_cref<Val> get(const Key &target) noexcept {
		auto maybe_it = find(target);
		if (!maybe_it)
			return std::nullopt;

		auto it = maybe_it.value();
		if (it == end())
			return std::nullopt;

		auto maybe_val = it.val();
		return maybe_val;
	}

	optional_cref<Val> operator[](const Key &target) noexcept {
		return get(target);
	}

	void erase(const Key &target) noexcept {
		(void) target;
		// Function is not yet implemented
		assert(false);
	}

public:// Accessors
	[[nodiscard]] Node *root_ptr() noexcept { return m_root; }
	[[nodiscard]] Node &root() noexcept { return *m_root; }

private:
	Position allocate_node() noexcept {
		auto pos = m_stored_end;
		m_stored_end.set(m_stored_end.get() + BtreeNodeSize);
		return pos;
	}

private:
	Storage m_storage;
	Node *m_root{nullptr};
	Position m_stored_end{0ul};
};

template<BtreeConfig Config>
class Btree<Config>::Node final {
private:
	using Bt = Btree<Config>;
	friend Bt::Iterator;

public:
	enum class Type {
		Branch,
		Leaf
	};

	struct Header {
		Position self_;
		Position prev_;
		Position next_;
		Position parent_;
		Type type_;
	};

private:
	static constexpr long _calc_num_records(auto metasize) {
		const auto leaf_size = metasize / (sizeof(Key) + sizeof(Val));
		const auto branch_size = (metasize - sizeof(Position)) / (sizeof(Position) + sizeof(KeyRef));
		return std::min(leaf_size, branch_size);
	}

	enum {
		NodeLimit = Config::BtreeNodeSize,
		MetaSize = NodeLimit - sizeof(Header),
		NumRecords = _calc_num_records(MetaSize),
		NumLinks = NumRecords + 1,
	};

	static_assert(NodeLimit > 0, "Node limit has to be a positive number.");
	static_assert(PowerOf2(NodeLimit), "Node limit has to be a power of 2.");
	static_assert(NumLinks == NumRecords + 1, "Node links must be one more than the number of records.");
	static_assert(NumLinks > 2, "Node links must be more than 2.");

public:
	struct LeafMeta {
		std::array<Key, NumRecords> keys_;
		std::array<Val, NumRecords> vals_;
	};

	struct BranchMeta {
		std::array<KeyRef, NumRecords> refs_;
		std::array<Position, NumLinks> links_;
	};

public:
	class Cache final {

	public:
		static Cache &the() noexcept {
			static Cache instance;
			return instance;
		}

		Node &fetch(Position pos) noexcept {
			assert(pos != Position::poison());

			if (!m_buffer.contains(pos)) {
				// Perform disk-io to get the Node value in the buffer
				assert(false);
			}

			return *m_buffer.at(pos).get();
		}

		Node &put(Position pos, std::unique_ptr<Node> &&inmem) noexcept {
			assert(pos != Position::poison());

			m_buffer[pos] = std::move(inmem);
			return fetch(pos);
		}

	private:
		// Guarantee that Cache cannot be instantiated from outside
		Cache() = default;

		std::unordered_map<unsigned long, std::unique_ptr<Node>> m_buffer;
	};

public:// Accessors
	[[nodiscard]] static constexpr auto num_records_per_node() { return Node::NumRecords; }
	[[nodiscard]] static constexpr auto num_links_per_branch() { return Node::NumLinks; }

	[[nodiscard]] bool is_branch() const noexcept { return m_header.type_ == Type::Branch; }
	[[nodiscard]] bool is_leaf() const noexcept { return m_header.type_ == Type::Leaf; }

	[[nodiscard]] bool is_full() const noexcept {
		assert(m_numfilled <= NumRecords);
		return m_numfilled == NumRecords;
	}
	[[nodiscard]] bool is_balanced() const noexcept { return m_numfilled < NumRecords; }
	[[nodiscard]] bool is_empty() const noexcept { return !m_numfilled; }

	[[nodiscard]] bool is_rightmost() const noexcept { return m_header.next_ == Position::poison(); }
	[[nodiscard]] bool is_leftmost() const noexcept { return m_header.prev_ == Position::poison(); }
	[[nodiscard]] bool is_root() const noexcept { return m_header.parent_ == Position::poison(); }

	[[nodiscard]] const Header &header() const noexcept { return m_header; }
	[[nodiscard]] Header &header_mut() noexcept { return m_header; }

	[[nodiscard]] LeafMeta &leaf() unsafe_ { return std::get<LeafMeta>(m_meta); }
	[[nodiscard]] BranchMeta &branch() unsafe_ { return std::get<BranchMeta>(m_meta); }

	[[nodiscard]] const LeafMeta &leaf() unsafe_ const { return std::get<LeafMeta>(m_meta); }
	[[nodiscard]] const BranchMeta &branch() unsafe_ const { return std::get<BranchMeta>(m_meta); }

	[[nodiscard]] auto range() const noexcept {
		if (is_branch())
			return std::make_pair(branch().refs_.begin(), branch().refs_.end());
		return std::make_pair(leaf().keys_.begin(), leaf().keys_.end());
	}

	[[nodiscard]] const Key *raw() const noexcept {
		if (is_branch())
			return branch().refs_.data();
		return leaf().keys_.data();
	}

	[[nodiscard]] const Key &at(long idx) const noexcept {
		assert(m_numfilled > idx);
		const auto &key_at_idx = raw()[idx];
		return key_at_idx;
	}

	[[nodiscard]] const Key &first() const noexcept { return at(0); }
	[[nodiscard]] const Key &last() const noexcept { return at(m_numfilled - 1); }

	[[nodiscard]] Iterator begin() noexcept { return Iterator{*this, 0, *m_tree}; }
	[[nodiscard]] Iterator end() noexcept { return Iterator{*this, m_numfilled, *m_tree}; }

	[[nodiscard]] Iterator begin() const noexcept { return Iterator{*this, 0, *m_tree}; }
	[[nodiscard]] Iterator end() const noexcept { return Iterator{*this, m_numfilled, m_tree}; }

	void set_numfilled(long numfilled) noexcept { m_numfilled = numfilled; }
	[[nodiscard]] long numfilled() const noexcept { return m_numfilled; }

	void set_self(Position self) noexcept { m_header.self_ = self; }
	[[nodiscard]] auto self() const noexcept { return m_header.self_; }

	void set_prev(Position prev) noexcept { m_header.prev_ = prev; }
	[[nodiscard]] auto prev() const noexcept { return m_header.prev_; }

	void set_next(Position next) noexcept { m_header.next_ = next; }
	[[nodiscard]] auto next() const noexcept { return m_header.next_; }

	void set_parent(Position parent) noexcept { m_header.parent_ = parent; }
	[[nodiscard]] auto parent() const noexcept { return m_header.parent_; }

	auto &tree() noexcept { return /* non null */ *m_tree; }

public:// Constructors
	enum class ConstructionStorageFlag {
		STORE,
		DONT_STORE
	};

	explicit Node(Bt &bt, Header header, ConstructionStorageFlag flag)
	    : m_header{std::move(header)}, m_tree{&bt} {
		if (is_branch())
			m_meta = BranchMeta{};
		else
			m_meta = LeafMeta{};

		if (flag == ConstructionStorageFlag::STORE)
			set_self(m_tree->allocate_node());
	}

	Node(const Node &) = default;

	Node(Node &&) = default;

	static Node RootDefault(Bt &bt) {
		auto head = Header{
		        bt.allocate_node(), Position::poison(), Position::poison(), Position::poison(), Type::Leaf};
		return Node(bt, head, ConstructionStorageFlag::STORE);
	}

	Node &operator=(const Node &rhs) noexcept = default;

	Node &operator=(Node &&rhs) noexcept = default;

	bool operator<=>(const Node &rhs) const noexcept = default;

private:// Helpers
	/*
	 * Rebalancing is right-biased.
	 *
	 * This means that in the overflowed node stay the first half of numbers (with smaller values)
	 * whereas the second one stores the higher part. The smallest value of the right node is copied
	 * (or referenced) in the parent node.
	 */
	Iterator spill_right(LeafMeta &asleaf, long pivot_idx) noexcept {
		const auto sibling_pos = m_tree->allocate_node();
		const auto sibling_head = Header{
		        sibling_pos,
		        self(),
		        Position::poison(),
		        parent(),
		        Type::Leaf};

		set_next(sibling_pos);

		auto sibling_ptr = std::make_unique<Node>(tree(), sibling_head, ConstructionStorageFlag::STORE);

		std::copy(asleaf.keys_.begin() + pivot_idx, asleaf.keys_.end(), sibling_ptr->leaf().keys_.begin());
		std::copy(asleaf.vals_.begin() + pivot_idx, asleaf.vals_.end(), sibling_ptr->leaf().vals_.begin());
		sibling_ptr->set_numfilled(NumRecords - pivot_idx);

		// Last step: Give ownership of the newly created node to the cache
		Node::Cache::the().put(sibling_pos, std::move(sibling_ptr));

		return Iterator{*this, pivot_idx, *m_tree};
	}

	Iterator raise_to_parent(Iterator origin) noexcept {
		Node &node = origin.node_mut();

		if (node.is_root()) {
			const auto new_root_pos = m_tree->allocate_node();
			auto new_root = std::make_unique<Node>(
			        *node.m_tree,
			        Header{new_root_pos, Position::poison(), Position::poison(), Position::poison(), Type::Branch},
			        ConstructionStorageFlag::STORE);
			const KeyRef &ref = origin.key().value();
			new_root->branch().refs_[0] = ref;
			new_root->m_numfilled = 1;
			new_root->branch().links_[0] = self();
			new_root->branch().links_[1] = next();

			m_tree->m_root = &Cache::the().put(new_root_pos, std::move(new_root));
			return Iterator{*m_tree->m_root, 0, *m_tree};
		}

		return m_tree->end();
	}

public:// API
	/// @brief This insert routine relies on us knowing where exactly the
	/// data should reside in it -- i.e `it`.
	Iterator insert(const Key &key, const Val &val, Iterator it) {
		const auto idx = it.index();
		auto &as_leaf = leaf();
		as_leaf.keys_[idx] = key;
		as_leaf.vals_[idx] = val;
		++m_numfilled;

		if (is_balanced())
			return it;

		// Not ok. Perform rebalancing policy.
		// That is - regular B-tree for now.

		const uint32_t pivot_idx = std::ceil(NumRecords / 2);
		Iterator pivot = spill_right(leaf(), pivot_idx);
		Iterator pos = raise_to_parent(pivot);

		// Finalize rebalancing by modifying current node's meta data
		set_numfilled(pivot_idx);
		if (is_root())
			set_parent(pos.node().self());

		return pos;
	}

	std::pair<Iterator, bool> find(const Key &target) noexcept {
		auto [lo, _] = range();
		auto hi = lo + m_numfilled;
		const auto beginning = lo;

		while (lo < hi) {
			auto mid = lo;
			std::advance(mid, std::distance(lo, hi) / 2);
			if (target > *mid)
				lo = mid + 1;
			else
				hi = mid;
		}

		long idx = std::distance(beginning, hi);
		auto it = Iterator{*this, idx, *m_tree};
		return std::make_pair(it, *hi == target && m_numfilled > 0);
	}

	Iterator insert(const Key &key, const Val &val) noexcept {
		auto [it, found] = find(key);
		if (found)
			return m_tree->end();
		return insert(key, val, it);
	}

private:
	Header m_header;
	Bt *m_tree;

	std::variant<LeafMeta, BranchMeta> m_meta;
	long m_numfilled{0};
};

template<BtreeConfig Config>
class Btree<Config>::Iterator final {
public:// Iterator traits
	using difference_type = long;
	using value_type = Node;
	using pointer = Node *;
	using reference = Node &;
	using iterator_category = std::bidirectional_iterator_tag;

private:
	using Val = Config::Val;
	using Key = Config::Key;

public:// Constructor
	Iterator() = default;

	Iterator(const Iterator &) noexcept = default;

	/// Tree::begin iterator
	explicit Iterator(Btree &tree) : m_node{nullptr}, m_index{0}, m_tree{&tree} {
		Node *cur = tree.root_ptr();
		while (cur->is_branch()) {
			const auto &branch_meta = cur->branch();

			assert(cur->numfilled() > 0);
			const Position last_link = branch_meta.links_[0];

			cur = &Node::Cache::the().fetch(last_link);
		}

		m_node = cur;
	}

	/// Iterator to a specific location in the tree
	Iterator(Node &node, long index, Btree &tree)
	    : m_node{&node}, m_index{index}, m_tree{&tree} {}

	static Iterator begin(Btree &tree) { return Iterator{tree}; }

	static Iterator end(Btree &tree) {
		Node *cur = tree.root_ptr();
		while (cur->is_branch()) {
			const auto &branch_meta = cur->branch();

			assert(cur->numfilled() > 0);
			const Position last_link = branch_meta.links_[cur->numfilled() - 1];

			cur = &Node::Cache::the().fetch(last_link);
		}

		return Iterator{*cur, cur->numfilled(), tree};
	}

public:
	~Iterator() noexcept = default;

	Iterator &operator=(const Iterator &) = default;

public:// Properties
	optional_cref<Val> val() const noexcept {
		assert(m_node->numfilled() > m_index);
		assert(m_tree && m_node && m_index >= 0);
		if (m_node->is_branch())
			return std::nullopt;
		const auto &leaf_meta = m_node->leaf();
		return std::make_optional(std::cref(leaf_meta.vals_[m_index]));
	}

	optional_cref<Key> key() const noexcept {
		assert(m_node->numfilled() > m_index);
		assert(m_tree && m_node && m_index >= 0);

		return std::make_optional(std::cref(m_node->at(m_index)));
	}

	const Node &node() const noexcept { return *m_node; }

	Node &node_mut() noexcept { return *m_node; }

	long index() const noexcept { return m_index; }

private:
	static optional_cref<Node> parent_of_node(const Node &node) {
		auto p = node.parent();
		if (p.is_set())
			return std::make_optional(std::cref(Node::Cache::the().fetch(p)));
		return std::nullopt;
	}

	/// @brief Get iterator to the value following the one that the current
	/// iterator points to
	///
	///       | 3 6 |
	///      /   |   \
  	///  |1 2| |4 5| |7 8|
	///
	/// For example, if the this points to 3 in the root node, the value
	/// following it resides in the child node - 4.
	std::optional<Iterator> next_() const noexcept {
		// Case 1: Get follower in a child
		const long numfilled = m_node->numfilled();
		if (m_node->is_branch() && m_index < numfilled) {
			const Position link_pos = m_node->branch().links_[m_index + 1];
			Node &link_node = Node::Cache::the().fetch(link_pos);
			return std::make_optional(Iterator{link_node, 0, *m_tree});
		}

		// Case 2: Get follower in the current node
		if (m_index < numfilled - 1 || m_node->is_rightmost())
			return std::make_optional(Iterator{*m_node, m_index + 1, *m_tree});

		assert(m_index == numfilled);

		auto first_more_than = [&](const Key &origin_ref) -> optional_cref<Node> {
			auto parent_opt = parent_of_node(*m_node);
			while (parent_opt.has_value()) {
				const Node &parent = parent_opt.value().get();
				if (origin_ref > parent.last()) {
					parent_opt = parent_of_node(parent);
					continue;
				}

				return parent;
			}
			return std::nullopt;
		};

		// Case 3: Get follower in parent (or the parent's parent)
		const auto origin_ref = m_node->at(m_index);
		const auto parent_opt = first_more_than(origin_ref);
		if (!parent_opt)
			return std::nullopt;

		const Node &parent = parent_opt.value();

		for (long idx = 0; idx < parent.numfilled(); ++idx)
			if (origin_ref < parent.at(idx))
				return std::make_optional(Iterator{
				        const_cast<Node &>(parent),
				        idx,
				        *m_tree});

		assert(false);
		return std::nullopt;
	}

	std::optional<Iterator> prev_() const noexcept {
		// Case 1: Get predecessor in a child
		if (m_node->is_branch() && m_index > 0) {
			const Position link_pos = m_node->branch().links_[m_index - 1];
			Node &link_node = Node::Cache::the().fetch(link_pos);
			return std::make_optional(
			        Iterator{link_node, link_node.numfilled(), *m_tree});
		}

		// Case 2: Get predecessor in the current node
		if (m_index > 0)
			return std::make_optional(Iterator{*m_node, m_index - 1, *m_tree});

		assert(m_index == 0);

		// Case 3: Get predecessor in parent (or the parent's parent)
		const auto origin_ref = m_node->at(m_index);
		auto parent_opt = parent_of_node(*m_node);
		while (parent_opt.has_value()) {
			const Node &parent = parent_opt.value().get();
			if (origin_ref < parent.first()) {
				parent_opt = parent_of_node(parent);
				continue;
			}

			for (long idx = 0; idx < parent.numfilled(); ++idx)
				if (origin_ref > parent.at(idx))
					return std::make_optional(
					        Iterator{const_cast<Node &>(parent), idx, *m_tree});

			// Should be unreachable
			assert(false);
		}

		return std::nullopt;
	}

public:// Iterator operations
	reference operator*() const noexcept { return *m_node; }

	pointer operator->() const noexcept { return &(operator*()); }

	Iterator &operator++() {
		assert(m_node->numfilled() > m_index);
		assert(m_tree && m_node && m_index >= 0);
		if (const auto next_opt = next_(); next_opt)
			*this = next_opt.value();
		return *this;
	}

	Iterator operator++(int) {
		auto copy_ = *this;
		this->operator++();
		return copy_;
	}

	Iterator &operator--() {
		assert(m_node->numfilled() <= m_index);
		assert(m_tree && m_node && m_index >= 0);
		if (const auto prev_opt = prev_(); prev_opt)
			*this = prev_opt.value();
		return *this;
	}

	Iterator operator--(int) {
		auto copy_ = *this;
		this->operator--();
		return copy_;
	}

	bool operator<=>(const Iterator &rhs) const noexcept = default;

private:
	Node *m_node;
	long m_index;
	Btree<Config> *m_tree;
};

static_assert(std::bidirectional_iterator<Btree<DefaultBtreeConfig>::Iterator>);

}// namespace internal::btree
