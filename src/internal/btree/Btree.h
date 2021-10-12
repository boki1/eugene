#ifndef _EUGENE_BTREE_INCLUDED_
#define _EUGENE_BTREE_INCLUDED_

#include <memory>		// std::unique_ptr
#include <optional>		// std::nullopt_t
#include <array>		// std::array
#include <variant>		// std::variant
#include <algorithm>		// std::min
#include <utility>		// std::move, std::pair
#include <cassert>		// assert
#include <tuple>		// std::tie

#include <internal/storage/Storage.h>

template <typename T>
consteval bool PowerOf2(T num) {
	return (num & (num - 1)) == 0;
}

template <typename T>
using optional_ref = std::optional<std::reference_wrapper<T>>;

template <typename T>
using optional_cref = std::optional<std::reference_wrapper<const T>>;

template <typename T>
optional_ref<T> uncref(optional_cref<T> copt) noexcept {
	return std::nullopt;
}

namespace internal::btree
{

	template <typename Config>
	concept BtreeConfig = requires(Config conf) {
		typename Config::Key;
		typename Config::KeyRef;
		typename Config::Val;
		typename Config::StorageDev;

		requires std::convertible_to<decltype(conf.ApplyCompression), bool>;
		requires storage::StorageDevice<typename Config::StorageDev>;
		requires std::unsigned_integral<decltype(conf.BtreeNodeSize)>;
	} && PowerOf2(Config::BtreeNodeSize);

	struct DefaultBtreeConfig {
		using Key = uint32_t;
		using KeyRef = Key;
		using Val = uint32_t;
		using StorageDev = storage::DefaultStorageDev;
		static constexpr unsigned BtreeNodeSize = 1 << 10;
		static constexpr bool ApplyCompression = false;
	};

	template <typename Config=DefaultBtreeConfig>
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

		class Iterator;

	public: // Constructors
	    Btree() = default;

            template<typename... Args>
            explicit Btree(Args &&...args) noexcept
	    	: m_storage(std::forward<Args>(args)...)
	    {
	    }

	public: // API

	    Iterator begin() const noexcept {}
	    Iterator end() const noexcept {}

	    Iterator insert(const Key& key, const Val& val) noexcept
	    {
		Node &cur = m_root;
		while (true) {
			auto [it, found] = cur.find(key);
			if (found) {
				assert(it.key() && it.key().value() == key);
				return it;
			}
			if (cur.is_leaf())
				return cur.insert(key, val, it);
			if (it == cur.end())
				break;
			cur = *it;
		}

		return end();
	    }

	    std::optional<Iterator> find(const Key &target) noexcept
	    {
		Node &cur = const_cast<Node &>(m_root);
		while (true) {
			Iterator it;
			bool next;
			std::tie(it, next) = cur.find(target);
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

	    optional_cref<Val> get(const Key &target) const noexcept
	    {
		auto maybe_it = find(target);
		if (!maybe_it)
			return std::nullopt;

		auto it = maybe_it.value();
		if (it == end())
			return std::nullopt;

		auto maybe_val = it.val();
		return maybe_val;
	    }

	    optional_cref<Val> operator[](const Key &target) const noexcept
	    {
		    return get(target);
	    }

	    void erase(const Key &target) noexcept
	    {
		    assert(("Function is not yet implemented -- Btree::erase", false));
	    }


	public: // Getters

	    const Node& root() const noexcept {
		    return m_root;
	    }

	    Node& root() noexcept {
		    return m_root;
	    }

	private:
            Storage m_storage;
            Node m_root = Node::Root(*this, Node::Type::Leaf);
	};

	template <BtreeConfig Config>
	class Btree<Config>::Node final {
	private:
		using Bt = Btree<Config>;

	public:
		enum class Type { Branch, Leaf };

		struct Header {
			storage::Position self_{storage::PositionPoison};
			storage::Position prev_{storage::PositionPoison};
			storage::Position next_{storage::PositionPoison};
			storage::Position parent_{storage::PositionPoison};
			Type type_;

			explicit Header(Type t) : type_{t} {}
			explicit Header(std::nullopt_t) {}
			Header(const Header &) = default;
		};

		struct MemHeader {
			optional_cref<Node> prev_{};
			optional_cref<Node> next_{};
			optional_cref<Node> parent_{};
			Type type_;

			explicit MemHeader(Type t) : type_{t} {}
			explicit MemHeader(std::nullopt_t) {}
			MemHeader(const MemHeader &) = default;
		};

	private:
		static consteval unsigned long _calc_num_records(auto metasize) {
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

		using LeafKeyRecords = std::array<Key, NumRecords>;
		using LeafValRecords = std::array<Val, NumRecords>;
		using BranchRefs = std::array<KeyRef, NumRecords>;
		using BranchLinks = std::array<Position, NumLinks>;

		struct LeafMeta {
			LeafKeyRecords keys_;
			LeafValRecords vals_;
		};

		struct BranchMeta {
			BranchRefs refs_;
			BranchLinks links_;
		};

	public: // Getters

		[[nodiscard]] bool is_branch() const noexcept {
			if (m_header && m_header.value().type_ == Type::Branch) return true;
			if (m_mem && m_mem.value().type_ == Type::Branch) return true;
			return false;
		}

		[[nodiscard]] bool is_leaf() const noexcept {
			return !is_branch();
		}

		[[nodiscard]] bool is_full() const noexcept {
			assert(m_numfilled <= NumRecords);
			return m_numfilled == NumRecords;
		}

		[[nodiscard]] bool is_ok() const noexcept {
			return m_numfilled < NumRecords;
		}

		[[nodiscard]] optional_cref<Header> header() const noexcept {
			return std::make_optional(std::cref(m_header));
		}

		[[nodiscard]] optional_cref<MemHeader> memheader() const noexcept {
			return std::make_optional(std::cref(m_mem));
		}

		[[nodiscard]] LeafMeta &leaf() {
			return std::get<LeafMeta>(m_meta);
		}

		[[nodiscard]] BranchMeta &branch() {
			return std::get<BranchMeta>(m_meta);
		}

		[[nodiscard]] const LeafMeta &leaf() const {
			return std::get<LeafMeta>(m_meta);
		}

		[[nodiscard]] const BranchMeta &branch() const {
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

		[[nodiscard]] const Key &at(unsigned long idx) const {
			assert(m_numfilled > idx);
			return raw()[idx];
		}

		[[nodiscard]] Iterator begin() noexcept {
			return Iterator { *this, 0, m_tree };
		}

		[[nodiscard]] Iterator end() noexcept {
			return Iterator { *this, m_numfilled, m_tree };
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

		void set_numfilled(unsigned long numfilled) noexcept {
			m_numfilled = numfilled;
		}

		void set_prev(Node * const mprev, std::optional<storage::Position> prev = {}) noexcept {
			if (m_mem)
				m_mem.value().prev_.emplace(std::cref(*mprev));
				// m_mem.value().prev_ = std::make_optional<std::cref(*mprev)>;

			if (m_header && prev)
				m_header.value().prev_ = prev.value(); 
		}

		void set_next(Node * const mnext, std::optional<storage::Position> next = {}) noexcept {
			if (m_mem)
				m_mem.value().next_.emplace(std::cref(*mnext));

			if (m_header && next)
				m_header.value().next_ = next.value(); 
		}

	public: // Constructors


		constexpr explicit Node(Bt &bt, std::optional<MemHeader> memheader = {}, std::optional<Header> header = {})
			: m_header{std::move(header)},
			  m_mem{std::move(memheader)},
			  m_tree{bt}
		{
			if (is_branch())
				m_meta = BranchMeta{};
			else
				m_meta = LeafMeta{};
		}

		static constexpr Node InMemoryNode(Bt &bt, MemHeader &&memheader)
		{
			return Node{bt, std::make_optional(memheader)};
		}

		static constexpr Node StoredNode(Bt &bt, Header &&header)
		{
			return Node{bt, std::nullopt, std::make_optional(header)};
		}

		static constexpr Node Root(Bt &bt, Type type)
		{
			return Node{ bt, MemHeader { type } };
		}

		Node(const Node &) = default;

		Node &operator=(const Node &rhs) noexcept
		{
			m_tree = rhs.m_tree;
			m_numfilled = rhs.m_numfilled;
			m_mem = rhs.m_mem;
			m_header = rhs.m_header;
			m_meta = rhs.m_meta;
			return *this;
		}

	private: // Helpers

		Iterator make_sibling(LeafMeta &asleaf, unsigned long middle_idx) noexcept
		{
			const auto pivot_idx = middle_idx + 1;
			auto sibling_ptr = std::make_shared<Node>(
				m_tree, m_mem, m_header
			);
			Node &sibling = *sibling_ptr;
			auto &sleaf = sibling.leaf();

			std::copy(asleaf.keys_.begin() + pivot_idx, asleaf.keys_.end(), sleaf.keys_.begin());
			std::copy(asleaf.vals_.begin() + pivot_idx, asleaf.vals_.end(), sleaf.vals_.begin());

			sibling.set_prev(this, self());
			set_next(sibling_ptr.get(), sibling.self());

			sibling.set_numfilled(NumRecords - pivot_idx);
			set_numfilled(middle_idx);
		}

		Iterator raise_to_parent(Iterator it) noexcept
		{
			assert(it.node());
			Node &node = it.node().value();

			return it;
		}
		
	public: // API

		Iterator insert(const Key &key, const Val &val, Iterator it)
		{
			assert(it.key() && it.key().value() != key);

			const auto maybe_idx = it.index();
			if (!maybe_idx)
				return Iterator::end(m_tree);
			const auto idx = maybe_idx.value();

			auto &as_leaf = leaf();
			as_leaf.keys_[idx] = key;
			as_leaf.vals_[idx] = val;
			++m_numfilled;

			if (is_ok())
				return it;

			// Rebalance

			const auto middle_idx = (NumRecords + 1) / 2;
			Iterator pivot = make_sibling(leaf(), middle_idx);
			Iterator pos = raise_to_parent(pivot);
			return pos;
		}

		std::pair<Iterator, bool> find(const Key &target) noexcept
		{
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

			unsigned long idx = std::distance(beginning, hi);
			auto it = Iterator{*this, idx, m_tree};
			return std::make_pair(it, *hi == target);
		}

	private:
		std::optional<Header> m_header{ std::nullopt };
		std::optional<MemHeader> m_mem{ std::nullopt };
		Bt &m_tree;

		std::variant<LeafMeta, BranchMeta> m_meta;
		unsigned long m_numfilled { 0 };
	};

	template <BtreeConfig Config>
	class Btree<Config>::Iterator final {
	public: // Iterator traits
		using difference_type = unsigned long;
		using value_type = Node;
		using pointer = Node *;
		using reference = Node &;
		using iterator_category = std::bidirectional_iterator_tag;

	private:
		using Val = Config::Val;
		using Key = Config::Key;


	public: // Constructor

		Iterator() = default;

		Iterator(Node& node, unsigned long index, Btree &tree)
			: m_node{ std::make_optional(std::ref(node)) },
			  m_index{ std::make_optional(index) },
			  m_tree{ std::make_optional(std::ref(tree)) }
		{
		}

		explicit Iterator(Btree &tree)
			: m_tree{ std::make_optional(std::ref(tree)) }
		{
		}

		static Iterator end(Btree &tree) noexcept {
			return Iterator{ tree };
		}

		Iterator& operator=(const Iterator&) = default;

	public: // Properties

		optional_cref<Val> val() const noexcept
		{
			if (!m_node)
				return std::nullopt;

			Node &noderef = m_node.value();
			return std::nullopt;
		}

		optional_cref<Key> key() const noexcept
		{
			if (!m_node || !m_index)
				return std::nullopt;

			Node &noderef = m_node.value();
			const auto index_ = m_index.value();
			auto [begin, end] = noderef.range();
			if (std::distance(begin, end) <= index_)
				return std::nullopt;
			std::advance(begin, index_);
			return *begin;
		}

		optional_ref<Node> node() const noexcept
		{
			return m_node;
		}

		std::optional<unsigned long> index() const noexcept
		{
			return m_index;
		}

	public: // Iterator operations

		reference operator*() const
		{
			assert(m_node.has_value());
			return m_node.value();
		}

		pointer operator->() const
		{
			return &(operator*());
		}

		Iterator& operator++()
		{

		}

		Iterator operator++(int)
		{

		}

		Iterator& operator--()
		{

		}

		Iterator operator--(int)
		{

		}

		Iterator operator+(unsigned long)
		{

		}

		// bool operator<=>(const Iterator &) const noexcept = default;
		//
		bool operator==(const Iterator &rhs) const noexcept
		{

		}

	private:
		optional_ref<Node> m_node{};
		std::optional<size_t> m_index{};
		optional_ref<Btree<Config>> m_tree{};
	};

}


#endif // _EUGENE_BTREE_INCLUDED_
