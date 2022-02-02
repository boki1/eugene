#pragma once
#include <cassert>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <utility>

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <nop/serializer.h>
#include <nop/status.h>
#include <nop/structure.h>
#include <nop/utility/die.h>
#include <nop/utility/stream_reader.h>
#include <nop/utility/stream_writer.h>

#include <core/Util.h>
#include <core/storage/Pager.h>
#include <core/storage/btree/Config.h>
#include <core/storage/btree/Node.h>

namespace internal::storage::btree {

namespace util {
template<BtreeConfig Config = DefaultConfig>
class BtreePrinter;
}

template<BtreeConfig Config = DefaultConfig>
class Btree final {
	using Self = Btree<Config>;

	using Key = typename Config::Key;
	using Val = typename Config::Val;
	using Ref = typename Config::Ref;
	using Nod = Node<Config>;

	using PagerAllocatorPolicy = typename Config::PageAllocatorPolicy;
	using PagerEvictionPolicy = typename Config::PageEvictionPolicy;

	friend util::BtreePrinter<Config>;

public:
	//! Number of entries in branch and leaf nodes may differ
	//! Directly unwrap with `.value()` since we _want to fail at compile time_ in case their is no value which
	//! satisfies the predicates
	int NUM_LINKS_BRANCH = ::internal::binsearch_primitive(2ul, PAGE_SIZE / 2, [](auto current, auto, auto) {
		                       auto sz = nop::Encoding<Nod>::Size({typename Nod::Metadata(typename Nod::Branch(std::vector<Ref>(current), std::vector<Position>(current))), 10, true});
		                       return sz - PAGE_SIZE;
	                       }).value_or(0);
	int NUM_RECORDS_BRANCH = NUM_LINKS_BRANCH - 1;

	//! Equivalent to `m` in Knuth's definition
	//! Make sure that when a leaf is split, its contents could be distributed among the two branch nodes.
	//! Directly unwrap with `.value()` since we _want to fail at compile time_ in case their is no value which
	//! satisfies the predicates
	int _NUM_RECORDS_LEAF = ::internal::binsearch_primitive(1ul, PAGE_SIZE / 2, [](auto current, auto, auto) {
		                        return nop::Encoding<Nod>::Size({typename Nod::Metadata(typename Nod::Leaf(std::vector<Key>(current), std::vector<Val>(current))), 10, true}) - PAGE_SIZE;
	                        }).value_or(0);
	int NUM_RECORDS_LEAF = _NUM_RECORDS_LEAF - 1 >= NUM_RECORDS_BRANCH * 2
	        ? NUM_RECORDS_BRANCH * 2 - 1
	        : _NUM_RECORDS_LEAF - 1;

private:
	static inline constexpr bool APPLY_COMPRESSION = Config::APPLY_COMPRESSION;
	static inline constexpr int PAGE_CACHE_SIZE = Config::PAGE_CACHE_SIZE;

	static inline constexpr uint32_t MAGIC = 0xB75EEA41;

public:
	struct Header {
		Position m_rootpos;
		std::size_t m_size{};
		std::size_t m_depth{};
		uint32_t m_magic{MAGIC};
		uint32_t m_pgcache_size{PAGE_CACHE_SIZE};
		uint8_t m_apply_compression{APPLY_COMPRESSION};

		/*
		 * The storage locations of the tree header and the tree contents differ.
		 * This one stores the name of the file which contains the header of the tree,
		 * whereas the other name passed to the Btree constructor is the one where
		 * the actual nodes of the tree are stored.
		 */
		std::string m_content_file;

		/*
		 * We don't want to serialize this. It is used only to check whether the header
		 * we are currently possessing is valid.
		 */
		bool m_dirty{false};

		Header() = default;
		explicit Header(Position rootpos, std::size_t size, std::size_t depth, std::string_view content_file)
		    : m_rootpos{rootpos},
		      m_size{size},
		      m_depth{depth},
		      m_content_file{std::string{content_file}} {}

		[[nodiscard]] auto &rootpos() noexcept { return m_rootpos; }

		[[nodiscard]] auto &size() noexcept { return m_size; }

		[[nodiscard]] auto &depth() noexcept { return m_depth; }

		[[nodiscard]] auto &dirty() noexcept { return m_dirty; }

		auto operator<=>(const Header &) const noexcept = default;

		friend std::ostream &operator<<(std::ostream &os, const Header &h) {
			os << "Header { .rootpos = " << h.m_rootpos << ", .size =" << h.m_size << ", .depth =" << h.m_depth << " }";
			return os;
		}

		NOP_STRUCTURE(Header, m_rootpos, m_size, m_depth, m_magic, m_pgcache_size, m_apply_compression, m_content_file);
	};

private:
	[[nodiscard]] constexpr bool is_node_full(const Self::Nod &node) {
		if (node.is_branch())
			return node.is_full(NUM_RECORDS_BRANCH);
		return node.is_full(NUM_RECORDS_LEAF);
	}

	[[nodiscard]] constexpr auto node_split(Self::Nod &node) {
		if (node.is_branch())
			return node.split(NUM_RECORDS_BRANCH);
		return node.split(NUM_RECORDS_LEAF);
	}

	[[nodiscard]] constexpr std::optional<Val> search_subtree(const Self::Nod &node, const Self::Key &target_key) {
		if (node.is_branch()) {
			const auto &refs = node.branch().m_refs;
			const std::size_t index = std::lower_bound(refs.cbegin(), refs.cend(), target_key) - refs.cbegin();
			const Position pos = node.branch().m_links[index];
			const auto other = Nod::from_page(m_pager.get(pos));
			return search_subtree(other, target_key);
		}

		// assert(node.is_leaf());

		const auto &keys = node.leaf().m_keys;
		const auto &vals = node.leaf().m_vals;
		const auto it = std::lower_bound(keys.cbegin(), keys.cend(), target_key);
		if (it == keys.cend() || *it != target_key)
			return {};
		return vals[it - keys.cbegin()];
	}

	Nod make_new_root() {
		auto old_root = root();
		auto old_pos = m_rootpos;

		auto new_pos = m_pager.alloc();

		old_root.set_parent(new_pos);
		old_root.set_root(false);

		auto [midkey, sibling] = node_split(old_root);
		auto sibling_pos = m_pager.alloc();

		Nod new_root{typename Nod::Metadata(typename Nod::Branch({midkey}, {old_pos, sibling_pos})), new_pos, true};

		m_pager.place(new_pos, new_root.make_page());
		m_pager.place(old_pos, old_root.make_page());
		m_pager.place(sibling_pos, sibling.make_page());

		m_rootpos = new_pos;
		++m_depth;
		m_header.m_dirty = true;

		return new_root;
	}

public:
	[[nodiscard]] auto root() { return Nod::from_page(m_pager.get(rootpos())); }

	[[nodiscard]] auto &header() noexcept { return m_header; }

	[[nodiscard]] std::string header_name() const noexcept { return fmt::format("{}-header", m_identifier); }

	[[nodiscard]] const auto &rootpos() const noexcept { return m_rootpos; }

	[[nodiscard]] std::size_t size() noexcept { return m_size; }

	[[nodiscard]] std::size_t size() const noexcept { return m_size; }

	[[nodiscard]] bool empty() const noexcept { return size() == 0; }

	[[nodiscard]] bool empty() noexcept { return size() == 0; }

	[[nodiscard]] std::size_t depth() { return m_depth; }

	[[nodiscard]] auto num_records_leaf() const noexcept { return NUM_RECORDS_LEAF; }

	[[nodiscard]] auto num_records_branch() const noexcept { return NUM_RECORDS_BRANCH; }

public:
	/*
	 *  Operations API
	 */

	void insert(const Self::Key &key, const Self::Val &val) {
		Position currpos{rootpos()};
		Nod curr{root()};

		if (is_node_full(curr))
			curr = make_new_root();

		while (true) {
			if (curr.is_leaf()) {
				/* fmt::print(" -- Putting kv pair in leaf ... \n"); */
				auto &keys = curr.leaf().m_keys;
				auto &vals = curr.leaf().m_vals;

				const std::size_t index = std::lower_bound(keys.cbegin(), keys.cend(), key) - keys.cbegin();
				if (!keys.empty() && index < keys.size() && keys[index] == key)
					return;

				keys.insert(keys.begin() + index, key);
				vals.insert(vals.begin() + index, val);
				m_pager.place(currpos, curr.make_page());
				++m_size;
				break;
			}

			auto &refs = curr.branch().m_refs;
			auto &links = curr.branch().m_links;

			const std::size_t index = std::lower_bound(refs.cbegin(), refs.cend(), key) - refs.cbegin();
			const Position child_pos = links[index];
			auto child = Nod::from_page(m_pager.get(child_pos));

			if (!is_node_full(child)) {
				currpos = child_pos;
				curr = std::move(child);
				continue;
			}

			auto [midkey, sibling] = node_split(child);
			auto sibling_pos = m_pager.alloc();

			assert(std::find(refs.cbegin(), refs.cend(), midkey) == refs.cend());
			assert(std::find(links.cbegin(), links.cend(), sibling_pos) == links.cend());

			refs.insert(refs.begin() + index, midkey);
			links.insert(links.begin() + index + 1, sibling_pos);

			m_pager.place(sibling_pos, std::move(sibling.make_page()));
			m_pager.place(child_pos, std::move(child.make_page()));
			m_pager.place(currpos, std::move(curr.make_page()));

			if (key < midkey) {
				currpos = child_pos;
				curr = std::move(child);
			} else if (key > midkey) {
				currpos = sibling_pos;
				curr = std::move(sibling);
			}
		}
	}

	[[nodiscard]] constexpr std::optional<Val> get(const Self::Key &key) {
		return search_subtree(root(), key);
	}

	[[nodiscard]] constexpr bool contains(const Self::Key &key) {
		return search_subtree(root(), key).has_value();
	}

private:

	/// Bare intialize an empty tree
	constexpr void bare() noexcept {
		m_rootpos = m_pager.alloc();
		Nod root_initial{typename Nod::Metadata(typename Nod::Leaf({}, {})), m_rootpos, true};
		m_pager.place(m_rootpos, std::move(root_initial.make_page()));

		// Fill in initial header
		m_header.rootpos() = m_rootpos;
		m_header.size() = m_size;
		m_header.depth() = m_depth;
	}

public:
	/*
	 *  Persistence API
	 */

	constexpr void load() {
		nop::Deserializer<nop::StreamReader<std::ifstream>> deserializer{header_name()};
		if (!deserializer.Read(&m_header))
			throw BadRead();

		m_rootpos = m_header.m_rootpos;
		m_size = m_header.m_size;
		m_depth = m_header.m_depth;

		m_pager.load();
	}

	constexpr void save() {
		if (m_header.dirty()) {
			m_header.rootpos() = m_rootpos;
			m_header.size() = m_size;
			m_header.dirty() = false;
		}

		nop::Serializer<nop::StreamWriter<std::ofstream>> serializer{header_name(), std::ios::trunc};
		if (!serializer.Write(m_header))
			throw BadWrite();

		m_pager.save();
	}

public:
	explicit Btree(std::string_view identifier, bool load_from_header = false)
	    : m_pager{identifier},
	      m_identifier{identifier} {

		// static_assert(NUM_LINKS_BRANCH >= 2);
		// static_assert(NUM_RECORDS_BRANCH > 1);
		// static_assert(NUM_RECORDS_LEAF > 1);

		load_from_header ? load() : bare();
	}

private:
	Pager<PagerAllocatorPolicy, PagerEvictionPolicy> m_pager;

	const std::string_view m_identifier;

	mutable Header m_header;

	Position m_rootpos;

	std::size_t m_size{0};

	std::size_t m_depth{0};
};

}// namespace internal::storage::btree
