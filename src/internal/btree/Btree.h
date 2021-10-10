#ifndef _EUGENE_BTREE_INCLUDED_
#define _EUGENE_BTREE_INCLUDED_

#include <memory>		// std::unique_ptr, std::weak_ptr
#include <optional>		// std::nullopt_t
#include <array>		// std::array
#include <variant>		// std::variant
#include <algorithm>		// std::min
#include <utility>		// std::move, std::pair
#include <cassert>		// assert
#include <tuple>		// std::ignore

#include <internal/storage/Storage.h>

template <typename T>
consteval bool PowerOf2(T num) {
	return (num & (num - 1)) == 0;
}

template <typename T>
using optional_ref = std::optional<std::reference_wrapper<T>>;

template <typename T>
using optional_cref = std::optional<std::reference_wrapper<const T>>;

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

	    Iterator begin() noexcept {}
	    Iterator end() noexcept {}

	    Iterator insert(const Key& key, const Val& val) noexcept
	    {
		Node &cur = m_root;
		while (true) {
			auto [it, skip] = cur.find(key);
			if (skip)
				++it;
			if (cur.is_leaf())
				return cur.insert(key, val, it);
			if (it == cur.end())
				break;
			cur = *it;
		}

		return end();
	    }

	    std::optional<Iterator> find(const Key &target) const noexcept
	    {
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
			storage::Position self_;
			storage::Position prev_;
			storage::Position next_;
			storage::Position parent_;
			Type type_;
		};

		struct MemHeader {
			optional_cref<Node> prev_{};
			optional_cref<Node> next_{};
			optional_cref<Node> parent_{};
			Type type_;
			explicit MemHeader(Type t) : type_{t} {}
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
			return m_numfilled >= NumRecords;
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

		}

		[[nodiscard]] Iterator end() noexcept {

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
		
	public: // API

		Iterator insert(const Key &key, const Val &val, Iterator it={})
		{
			return it;
		}

		std::pair<Iterator, bool> find(const Key &target) const noexcept
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

			auto idx = std::distance(beginning, hi);
			auto it = Iterator{*this, idx};
			return std::make_pair(it, *hi == target);
		}

	private:
		Bt &m_tree;
		std::optional<Header> m_header{};
		std::optional<MemHeader> m_mem{};

		std::variant<LeafMeta, BranchMeta> m_meta;
		unsigned long m_numfilled { 0 };
	};

	template <BtreeConfig Config>
	class Btree<Config>::Iterator final {
	public:
		using Val = Config::Val;
		using Key = Config::Key;

	public: // Constructor

		Iterator() = default;

		Iterator(const Node &, unsigned long)
		{
		}

		optional_cref<Val> val() const noexcept
		{
			return std::nullopt;
		}

		const Iterator operator++(int)
		{
		}

		Iterator &operator++()
		{
		}

		Node &operator*()
		{
		}

	};

}

#endif // _EUGENE_BTREE_INCLUDED_
