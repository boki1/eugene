#pragma once
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <utility>

#include <gsl/pointers>

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <nop/serializer.h>
#include <nop/status.h>
#include <nop/structure.h>
#include <nop/utility/die.h>
#include <nop/utility/stream_reader.h>
#include <nop/utility/stream_writer.h>

#include <core/Util.h>
#include <core/storage/Page.h>
#include <core/storage/PageCache.h>
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

	friend util::BtreePrinter<Config>;

	//! Reserved a random size for now. Seems to me that this would be enough :)
	static inline constexpr int BTREE_HEADER_SIZE = 128;

	static inline constexpr int BTREE_NODE_SIZE = Page::size() - BTREE_HEADER_SIZE;

	//! Equivalent to `m` in Knuth's definition
	static inline constexpr int NUM_RECORDS_LEAF = BTREE_NODE_SIZE / (sizeof(Key) + sizeof(Val));

	//! Number of entries in branch and leaf nodes may differ
	static inline constexpr int NUM_LINKS_BRANCH = BTREE_NODE_SIZE / (sizeof(Position) + sizeof(Ref));
	static inline constexpr int NUM_RECORDS_BRANCH = NUM_LINKS_BRANCH - 1;

	static inline constexpr bool APPLY_COMPRESSION = Config::APPLY_COMPRESSION;
	static inline constexpr int PAGE_CACHE_SIZE = Config::PAGE_CACHE_SIZE;

	static inline constexpr uint32_t MAGIC = 0xB75EEA41;

public:
	struct Header {
		Position m_rootpos;
		std::size_t m_size;
		uint32_t m_magic{MAGIC};
		uint32_t m_pgcache_size{PAGE_CACHE_SIZE};
		uint32_t m_btree_node_size{BTREE_NODE_SIZE};
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
		explicit Header(Position rootpos, std::size_t size, std::string_view content_file)
		    : m_rootpos{rootpos},
		      m_size{size},
		      m_content_file{std::string{content_file}} {}

		void set_rootpos(Position rootpos) noexcept { m_rootpos = rootpos; }
		void set_size(std::size_t size) noexcept { m_size = size; }
		bool &dirty() noexcept { return m_dirty; }

		auto operator<=>(const Header &) const noexcept = default;

		friend std::ostream &operator<<(std::ostream &os, const Header &h) {
			os << "Header { .rootpos = " << h.m_rootpos << ", .size =" << h.m_size << " }";
			return os;
		}

		NOP_STRUCTURE(Header, m_rootpos, m_size, m_magic, m_pgcache_size,
		              m_btree_node_size, m_apply_compression, m_content_file);
	};

private:
	[[nodiscard]] bool is_node_full(const Self::Nod &node) {
		if (node.is_branch())
			return node.is_full(NUM_RECORDS_BRANCH);
		return node.is_full(NUM_RECORDS_LEAF);
	}

	[[nodiscard]] auto node_split(Self::Nod &node) {
		if (node.is_branch())
			return node.split(NUM_RECORDS_BRANCH);
		return node.split(NUM_RECORDS_LEAF);
	}

	[[nodiscard]] std::optional<Val> search_subtree(const Self::Nod &node, const Self::Key &target_key) const noexcept {
		if (node.is_branch()) {
			const auto &refs = node.branch().m_refs;
			const std::size_t index = std::lower_bound(refs.cbegin(), refs.cend(), target_key) - refs.cbegin();
			const Position pos = node.branch().m_links[index];
			const auto other = Nod::from_page(m_pgcache.get_page(pos));
			return search_subtree(other, target_key);
		}
		assert(node.is_leaf());

		const auto &keys = node.leaf().m_keys;
		const auto &vals = node.leaf().m_vals;
		const auto it = std::lower_bound(keys.cbegin(), keys.cend(), target_key);
		if (it == keys.cend() || *it != target_key)
			return {};
		return vals[it - keys.cbegin()];
	}

	Nod make_new_root() {
		fmt::print("Making new root node\n");

		auto old_root = root();
		auto old_pos = m_rootpos;
		fmt::print("Old is @{}\n", old_pos);

		auto new_pos = m_pgcache.get_new_pos();
		fmt::print("New is @{}\n", new_pos);

		old_root.set_parent(new_pos);
		old_root.set_root(false);

		auto [midkey, sibling] = node_split(old_root);
		auto sibling_pos = m_pgcache.get_new_pos();
		fmt::print("Sibling is @{}\n", sibling_pos);

		Nod new_root{typename Nod::Metadata(typename Nod::Branch({midkey}, {old_pos, sibling_pos})), new_pos, true};

		m_pgcache.put_page(new_pos, new_root.make_page());
		m_pgcache.put_page(old_pos, old_root.make_page());
		m_pgcache.put_page(sibling_pos, sibling.make_page());
		fmt::print("Stored the 3 nodes\n");

		m_rootpos = new_pos;
		fmt::print("Updated m_rootpos to match new_pos (@{})\n", new_pos);
		fmt::print("\n");
		m_header.m_dirty = true;

		return new_root;
	}

public:
	[[nodiscard]] auto root() noexcept { return std::as_const(*this).root(); }

	[[nodiscard]] auto root() const noexcept { return Nod::from_page(m_pgcache.get_page(rootpos())); }

	[[nodiscard]] auto &header() noexcept { return m_header; }

	[[nodiscard]] const auto &rootpos() const noexcept { return m_rootpos; }

	[[nodiscard]] std::size_t size() { return m_size; }

	[[nodiscard]] auto num_records_leaf() const noexcept { return NUM_RECORDS_LEAF; }

	[[nodiscard]] auto num_records_branch() const noexcept { return NUM_RECORDS_BRANCH; }
public:
	/*
	 *  Operations API
	 */

	void put(const Self::Key &key, const Self::Val &val) {
		auto currpos{rootpos()};
		auto curr{root()};

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
				m_pgcache.put_page(currpos, curr.make_page());
				++m_size;
				break;
			}

			/* fmt::print(" -- Searching in branch ... \n"); */
			auto &refs = curr.branch().m_refs;
			auto &links = curr.branch().m_links;
			const std::size_t index = std::lower_bound(refs.cbegin(), refs.cend(), key) - refs.cbegin();
			const Position child_pos = links[index];
			auto child = Nod::from_page(m_pgcache.get_page(child_pos));

			if (!is_node_full(child)) {
				curr = std::move(child);
				currpos = child_pos;
				continue;
			}

			fmt::print("Splitting non-root node.\n");

			fmt::print("Node @{}\n", child_pos);
			auto [midkey, sibling] = node_split(child);
			fmt::print("Midkey = {}\n", midkey);

			auto child_page = child.make_page();
			assert(child == Nod::from_page(child_page));
			m_pgcache.put_page(child_pos, std::move(child_page));
			fmt::print("Child node @{}\n", child_pos);
			auto sibling_pos = m_pgcache.get_new_pos();
			auto sibling_page = sibling.make_page();
			m_pgcache.put_page(sibling_pos, std::move(sibling_page));
			assert(sibling == Nod::from_page(sibling_page));
			fmt::print("Sibling node @{}\n", sibling_pos);

			refs.insert(refs.begin() + index, midkey);
			links.insert(links.begin() + index + 1, sibling_pos);
			m_pgcache.put_page(currpos, curr.make_page());
			fmt::print("Stored midkey and sibling_pos in parent.\n");
			fmt::print("\n");

			if (key < midkey) {
				curr = std::move(child);
				currpos = child_pos;
			} else if (key > midkey) {
				curr = std::move(sibling);
				currpos = sibling_pos;
			}
		}
	}

	[[nodiscard]] std::optional<Val> get(const Self::Key &key) const noexcept {
		return search_subtree(root(), key);
	}

	[[nodiscard]] bool contains(const Self::Key &key) const noexcept {
		return search_subtree(root(), key).has_value();
	}

public:
	/*
	 *  Persistence API
	 */

	void save_header() const noexcept {
		if (m_header.dirty()) {
			/* fmt::print("Dirty header. Still at rootpos = {}, setting to {}\n", m_header.m_rootpos, m_rootpos); */
			m_header.set_rootpos(m_rootpos);
			m_header.set_size(m_size);
			m_header.dirty() = false;
		}

		nop::Serializer<nop::StreamWriter<std::ofstream>> serializer{m_header_name.data(), std::ios::trunc};
		serializer.Write(m_header) || nop::Die(std::cerr);
		/* fmt::print("Stored btree header\n"); */
		std::cout << m_header << "\n";
	}

	bool load_header() {
		nop::Deserializer<nop::StreamReader<std::ifstream>> deserializer{m_header_name.data()};
		deserializer.Read(&m_header) || nop::Die(std::cerr);
		/* fmt::print("Loaded btree header\n"); */
		std::cout << m_header << "\n";
		return true;
	}

	void load_root() {
		if (load_header()) {
			m_rootpos = m_header.m_rootpos;
			m_size = m_header.m_size;
		}
	}

	void save() const {
		save_header();
		m_pgcache.flush_all();
	}

public:
	explicit Btree(std::string_view pgcache_name,
	               std::string_view btree_header_name = "/tmp/eu-btree-header",
	               bool load = false)
	    : m_pgcache{pgcache_name, PAGE_CACHE_SIZE},
	      m_header{m_pgcache.get_new_pos(), 0, pgcache_name},
	      m_header_name{btree_header_name} {

		/* fmt::print("Constructing btree\n"); */

		if (load) {
			load_root();
		} else {
			m_rootpos = m_header.m_rootpos;
			m_pgcache.put_page(m_rootpos,
			                   Nod{typename Nod::Metadata(typename Nod::Leaf({}, {})), rootpos(), true}.make_page());
			m_size = 0;
		}
	}

private:
	mutable PageCache m_pgcache;

	mutable Header m_header;
	const std::string_view m_header_name;

	Position m_rootpos;

	std::size_t m_size;
};

}// namespace internal::storage::btree

/*
 * Why doesn't this work??
 *
template<>
template<internal::storage::btree::BtreeConfig Conf>
struct fmt::formatter<internal::storage::btree::Btree<Conf>::Header> {

	template<typename ParseContext>
	constexpr auto parse(ParseContext &ctx) {
		return ctx.begin();
	}

	template<typename FormatContext>
	auto format(const internal::storage::btree::Btree<Conf>::Header &h, FormatContext &ctx) {
		return fmt::format_to(ctx.out(), "Header {{ .rootpos='{}', .dirty={}, .records='{}' }}", h.m_rootpos, h.m_dirty, h.m_numrecords);
	}
};
*/
