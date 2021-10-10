#ifndef _EUGENE_BTREE_INCLUDED_
#define _EUGENE_BTREE_INCLUDED_

#include <memory>		// std::unique_ptr
#include <optional>		// std::nullopt_t
#include <array>		// std::array
#include <variant>		// std::variant
#include <algorithm>		// std::min
#include <utility>		// std::move, std::pair
#include <cassert>		// assert

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

	public:
	    Btree() = default;

            template<typename... Args>
            explicit Btree(Args &&...args) noexcept
	    	: m_storage(std::forward<Args>(args)...)
	    {
	    }

	private:
            Storage m_storage;
            std::unique_ptr<Node> m_root {Node::RootPtr(*this, Node::Type::Leaf)};
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

		[[nodiscard]] auto range() const noexcept {
			if (is_branch())
				return std::make_pair(branch().keys_.begin(), branch().keys_end());
			return std::make_pair(leaf().keys_.begin(), leaf().keys_end());
		}

		[[nodiscard]] const Key *raw() const noexcept {
			if (is_branch())
				return branch().keys_.data();
			return leaf().keys_.data();
		}

		[[nodiscard]] const Key &at(unsigned long idx) const {
			assert(m_numfilled > idx);
			return raw()[idx];
		}

	public: // Constructors

		Node(const Node &) = default;
		Node (Node &&) noexcept = default;
		Node &operator=(const Node &) = default;
	    	Node& operator=(Node&& other) noexcept = default;

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

		static constexpr Node * const RootPtr(Bt &bt, Type type)
		{
			return new Node{ bt, MemHeader { type } };
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

	};

}

#endif // _EUGENE_BTREE_INCLUDED_
