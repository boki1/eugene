#pragma once

#include <internal/storage/Storage.h>

#include <algorithm>    // std::min
#include <array>        // std::array
#include <cassert>      // assert
#include <memory>       // std::unique_ptr
#include <optional>     // std::nullopt_t
#include <tuple>        // std::tie
#include <unordered_map>// std::unordered_map
#include <utility>      // std::move, std::pair
#include <variant>      // std::variant

template<typename T>
consteval bool PowerOf2(T num) {
	return (num & (num - 1)) == 0;
}

#define unsafe_ /* unsafe function */

template<typename T>
using optional_ref = std::optional<std::reference_wrapper<T>>;

template<typename T>
using optional_cref = std::optional<std::reference_wrapper<const T>>;

namespace internal::btree {

template<typename Config>
concept BtreeConfig = requires(Config conf) {
	typename Config::Key;
	typename Config::KeyRef;
	typename Config::Val;
	typename Config::StorageDev;

	requires std::convertible_to<decltype(conf.ApplyCompression), bool>;
	requires storage::StorageDevice<typename Config::StorageDev>;
	requires std::unsigned_integral<decltype(conf.BtreeNodeSize)>;
}
&&PowerOf2(Config::BtreeNodeSize);

struct DefaultBtreeConfig {
	using Key = uint32_t;
	using KeyRef = Key;
	using Val = uint32_t;
	using StorageDev = storage::DefaultStorageDev;
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
	using Position = storage::Position;

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

public:// Constructors
	Btree() = default;

	template<typename... Args>
	explicit Btree(Args &&...args) noexcept
	    : m_storage(std::forward<Args>(args)...) {}

public:
	~Btree() noexcept = default;

	bool operator<=>(const Btree &rhs) const noexcept {
		return m_root.operator<=>(rhs);
	}

public:// API
	Iterator begin() noexcept { return Iterator::begin(*this); }

	Iterator end() noexcept { return Iterator::end(*this); }

	Iterator insert(const Key &key, const Val &val) noexcept {
		Node &cur = m_root;
		while (true) {
			auto [it, next] = cur.find(key);
			if (next)
				++it;
			if (cur.is_leaf())
				return cur.insert(key, val, it);
			if (it == cur.end())
				break;
			cur = *it;
		}

		return end();
	}

	std::optional<Iterator> find(const Key &target) noexcept {
		Node &cur = const_cast<Node &>(m_root);
		while (true) {
			auto [it, next] = cur.find(target);
			if (cur.is_leaf()) {
				if (!next)
					break;
				return std::make_optional(it);
			}

			if (next)
				++it;

			cur = *it;
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

public:// Getters
	const Node &root() const noexcept { return m_root; }

	Node &root() noexcept { return m_root; }

private:
	Storage m_storage;
	Node m_root = Node::Root(*this, Node::Type::Leaf);
};

template<BtreeConfig Config>
class Btree<Config>::Node final {
private:
	using Bt = Btree<Config>;
	friend Bt::Iterator;

public:
	enum class Type { Branch,
		          Leaf };

	struct Header {
		storage::Position self_{storage::PositionPoison};
		storage::Position prev_{storage::PositionPoison};
		storage::Position next_{storage::PositionPoison};
		storage::Position parent_{storage::PositionPoison};
		Type type_;

		explicit Header(Type t) : type_{t} {}

		explicit Header(std::nullopt_t) {}

		Header(const Header &) = default;

		bool operator<=>(const Header &) const noexcept = default;
	};

	struct MemHeader {
		optional_ref<Node> prev_{};
		optional_ref<Node> next_{};
		optional_ref<Node> parent_{};
		Type type_;

		explicit MemHeader(Type t) : type_{t} {}

		explicit MemHeader(std::nullopt_t) {}

		MemHeader(const MemHeader &) = default;

		bool operator<=>(const MemHeader &) const noexcept = default;
	};

private:
	static consteval long _calc_num_records(auto metasize) {
		const auto leaf_size = metasize / (sizeof(Key) + sizeof(Val));
		const auto branch_size =
		        (metasize - sizeof(Position)) / (sizeof(Position) + sizeof(KeyRef));
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
	static_assert(NumLinks == NumRecords + 1,
	              "Node links must be one more than the number of records.");
	static_assert(NumLinks > 2, "Node links must be more than 2.");

public:
	using LeafKeyRecords = std::array<Key, NumRecords>;
	using LeafValRecords = std::array<Val, NumRecords>;
	using BranchRefs = std::array<KeyRef, NumRecords>;
	using BranchLinks = std::array<Position, NumLinks>;

	struct LeafMeta {
		LeafKeyRecords keys_;
		LeafValRecords vals_;

		bool operator<=>(const LeafMeta &) const noexcept = default;
	};

	struct BranchMeta {
		BranchRefs refs_;
		BranchLinks links_;

		bool operator<=>(const BranchMeta &) const noexcept = default;
	};

public:
	static consteval auto records_() { return Node::NumRecords; }

	static consteval auto links_() { return Node::NumLinks; }

public:
	static Node *fetch_node(const Position pos) noexcept {
		static std::unordered_map<Position, Node *> cache{};
		if (!cache.contains(pos)) {
			// Perform disk-io to get the Node value in "cache"
		}

		return cache.at(pos);
	}

public:// Getters
	[[nodiscard]] bool is_branch() const noexcept {
		if (m_header && m_header.value().type_ == Type::Branch)
			return true;
		if (m_mem && m_mem.value().type_ == Type::Branch)
			return true;
		return false;
	}

	[[nodiscard]] bool is_leaf() const noexcept { return !is_branch(); }

	[[nodiscard]] bool is_full() const noexcept {
		assert(m_numfilled <= NumRecords);
		return m_numfilled == NumRecords;
	}

	[[nodiscard]] bool is_ok() const noexcept { return m_numfilled < NumRecords; }

	[[nodiscard]] bool is_empty() const noexcept { return !m_numfilled; }

	[[nodiscard]] bool is_rightmost() const noexcept {
		if (m_mem.has_value()) {
			const auto &mem = m_mem.value();
			return !mem.next_.has_value();
		}
		// if (const auto &h : m_header.value(); h.has_value()) ;
		// TODO: Fetch
		return false;
	}

	[[nodiscard]] bool is_leftmost() const noexcept {
		if (m_mem.has_value()) {
			const auto &mem = m_mem.value();
			return !mem.prev_.has_value();
		}

		// if (const auto &h : m_header.value(); h.has_value()) ;
		// TODO: Fetch
		return false;
	}

	[[nodiscard]] bool is_root() const noexcept {
		if (m_mem.has_value()) {
			const auto &mem = m_mem.value();
			return !mem.parent_.has_value();
		}
		// if (const auto &h : m_header.value(); h.has_value()) ;
		// TODO: Fetch
		return false;
	}

	[[nodiscard]] optional_cref<Header> header() const noexcept {
		if (!m_header.has_value())
			return std::nullopt;
		return std::make_optional(std::cref(m_header.value()));
	}

	[[nodiscard]] optional_cref<MemHeader> memheader() const noexcept {
		if (!m_mem.has_value())
			return std::nullopt;
		return std::make_optional(std::cref(m_mem.value()));
	}

	[[nodiscard]] LeafMeta &leaf() { return std::get<LeafMeta>(m_meta); }

	[[nodiscard]] BranchMeta &branch() { return std::get<BranchMeta>(m_meta); }

	[[nodiscard]] const LeafMeta &leaf() unsafe_ const {
		return std::get<LeafMeta>(m_meta);
	}

	[[nodiscard]] const BranchMeta &branch() unsafe_ const {
		return std::get<BranchMeta>(m_meta);
	}

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

	[[nodiscard]] Iterator begin() noexcept {
		return Iterator{*this, 0, *m_tree};
	}

	[[nodiscard]] Iterator begin() const noexcept {
		return Iterator{*this, 0, *m_tree};
	}

	[[nodiscard]] Iterator end() noexcept {
		return Iterator{*this, m_numfilled, *m_tree};
	}

	[[nodiscard]] Iterator end() const noexcept {
		return Iterator{*this, m_numfilled, *m_tree};
	}

	[[nodiscard]] std::optional<storage::Position> self() const noexcept {
		if (m_header)
			return m_header.value().self_;
		return std::nullopt;
	}

	[[nodiscard]] std::optional<storage::Position> parent() const noexcept {
		if (m_header)
			return m_header.value().parent_;
		return std::nullopt;
	}

	[[nodiscard]] optional_cref<Node> memparent() const noexcept {
		if (m_mem)
			return m_mem.value().parent_;
		return std::nullopt;
	}

	void set_numfilled(long numfilled) noexcept { m_numfilled = numfilled; }

	[[nodiscard]] long numfilled() const noexcept { return m_numfilled; }

	void set_prev(Node *const mprev,
	              std::optional<storage::Position> prev = {}) noexcept {
		if (m_mem)
			m_mem.value().prev_.emplace(std::ref(*mprev));

		if (m_header && prev)
			m_header.value().prev_ = prev.value();
	}

	void set_next(Node *const mnext,
	              std::optional<storage::Position> next = {}) noexcept {
		if (m_mem)
			m_mem.value().next_.emplace(std::ref(*mnext));

		if (m_header && next)
			m_header.value().next_ = next.value();
	}

public:// Constructors
	constexpr explicit Node(Bt &bt, std::optional<MemHeader> memheader = {},
	                        std::optional<Header> header = {})
	    : m_header{std::move(header)}, m_mem{std::move(memheader)}, m_tree{&bt} {
		if (is_branch())
			m_meta = BranchMeta{};
		else
			m_meta = LeafMeta{};
	}

	Node(const Node &) = default;

	static constexpr Node InMemoryNode(Bt &bt, MemHeader &&memheader) {
		return Node{bt, std::make_optional(memheader)};
	}

	static constexpr Node StoredNode(Bt &bt, Header &&header) {
		return Node{bt, std::nullopt, std::make_optional(header)};
	}

	static constexpr Node Root(Bt &bt, Type type) {
		return Node{bt, MemHeader{type}};
	}

public:
	~Node() noexcept = default;

	Node &operator=(const Node &rhs) noexcept = default;

	bool operator<=>(const Node &rhs) const noexcept = default;

private:// Helpers
	Iterator spill_right(LeafMeta &asleaf, long middle_idx) noexcept {
		const auto pivot_idx = middle_idx + 1;
		auto sibling_ptr = std::make_shared<Node>(*m_tree, m_mem, m_header);
		Node &sibling = *sibling_ptr;
		auto &sleaf = sibling.leaf();

		std::copy(asleaf.keys_.begin() + pivot_idx, asleaf.keys_.end(),
		          sleaf.keys_.begin());
		std::copy(asleaf.vals_.begin() + pivot_idx, asleaf.vals_.end(),
		          sleaf.vals_.begin());

		sibling.set_prev(this, self());
		set_next(sibling_ptr.get(), sibling.self());

		sibling.set_numfilled(NumRecords - pivot_idx);
		set_numfilled(middle_idx);
		return Iterator{*this, middle_idx, *m_tree};
	}

	Iterator raise_to_parent(Iterator it) noexcept {
		[[maybe_unused]] Node &node = it.node_mut();

		return it;
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

		if (is_ok())
			return it;

		// Not ok. Perform rebalancing policy.
		// That is - regular B-tree for now.

		const auto middle_idx = (NumRecords + 1) / 2;
		Iterator pivot = spill_right(leaf(), middle_idx);
		Iterator pos = raise_to_parent(pivot);
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

private:
	std::optional<Header> m_header{std::nullopt};
	std::optional<MemHeader> m_mem{std::nullopt};
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
		Node &cur = tree.root();
		while (cur.is_branch()) {
			const auto &branch_meta = cur.branch();

			assert(cur.numfilled() > 0);
			const Position last_link = branch_meta.links_[0];

			cur = *Node::fetch_node(last_link);
		}

		m_node = &cur;
	}

	/// Iterator to a specific location in the tree
	Iterator(Node &node, long index, Btree &tree)
	    : m_node{&node}, m_index{index}, m_tree{&tree} {}

	static Iterator begin(Btree &tree) { return Iterator{tree}; }

	static Iterator end(Btree &tree) {
		Node &cur = tree.root();
		while (cur.is_branch()) {
			const auto &branch_meta = cur.branch();

			assert(cur.numfilled() > 0);
			const Position last_link = branch_meta.links_[cur.numfilled() - 1];

			cur = *Node::fetch_node(last_link);
		}

		return Iterator{cur, cur.numfilled(), tree};
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
		if (const auto mp = node.memparent(); mp.has_value())
			return mp;

		const auto p_opt = node.parent();
		assert(p_opt.has_value());
		[[maybe_unused]] const Position p = p_opt.value();
		// TODO: Fetch p or something
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
	/// @note As of now this is more like next_mem_()
	std::optional<Iterator> next_() const noexcept {
		// Case 1: Get follower in a child
		const long numfilled = m_node->numfilled();
		if (m_node->is_branch() && m_index < numfilled) {
			const Position link_pos = m_node->branch().links_[m_index + 1];
			Node &link_node = *Node::fetch_node(link_pos);
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
				return std::make_optional(
				        Iterator{const_cast<Node &>(parent), idx, *m_tree});

		assert(false);
		return std::nullopt;
	}

	std::optional<Iterator> prev_() const noexcept {
		// Case 1: Get predecessor in a child
		if (m_node->is_branch() && m_index > 0) {
			const Position link_pos = m_node->branch().links_[m_index - 1];
			Node &link_node = *Node::fetch_node(link_pos);
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
