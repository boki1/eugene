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

	static auto _calc_num_records_leaf() {
		std::size_t low = 2;
		std::size_t high = Page::size() / sizeof(uint8_t);

		int mid = 0;
		std::size_t size;
		std::pair<int, std::size_t> last_valid;
		while (low <= high) {
			mid = ((unsigned int) low + (unsigned int) high) >> 1;
			size = nop::Encoding<Nod>::Size({typename Nod::Metadata(typename Nod::Leaf(std::vector<Key>(mid), std::vector<Val>(mid))), {}, true});

			if (size < Page::size()) {
				low = mid + 1;
				last_valid = {mid, size};
			}
			else if (size > Page::size())
				high = mid - 1;
			else
				break;
		}
		if (size > Page::size()) {
			assert(last_valid.second <= Page::size());
			return last_valid.first;
		}
		return mid;
	}

	static auto _calc_num_links_branch() {
		std::size_t low = 2;
		std::size_t high = Page::size() / sizeof(uint8_t);

		int mid = 0;
		std::size_t size;
		std::pair<int, std::size_t> last_valid;
		while (low <= high) {
			mid = ((unsigned int) low + (unsigned int) high) >> 1;
			size = nop::Encoding<Nod>::Size({typename Nod::Metadata(typename Nod::Branch(std::vector<Ref>(mid), std::vector<Position>(mid))), {}, true});

			if (size < Page::size()) {
				low = mid + 1;
				last_valid = {mid, size};
			}
			else if (size > Page::size())
				high = mid - 1;
			else
				break;
		}
		if (size > Page::size()) {
			assert(last_valid.second <= Page::size());
			return last_valid.first;
		}
		return mid;
	}

public:
	//! Number of entries in branch and leaf nodes may differ
	int NUM_LINKS_BRANCH = _calc_num_links_branch();
	int NUM_RECORDS_BRANCH = NUM_LINKS_BRANCH - 1;

	//! Equivalent to `m` in Knuth's definition
	//! Make sure that when a leaf is split, its contents could be distributed among the two branch nodes.
	int _NUM_RECORDS_LEAF = _calc_num_records_leaf();
	int NUM_RECORDS_LEAF = _NUM_RECORDS_LEAF - 1 >= NUM_RECORDS_BRANCH * 2
	        ? NUM_RECORDS_BRANCH * 2 - 1
	        : _NUM_RECORDS_LEAF;

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
		auto old_root = root();
		auto old_pos = m_rootpos;

		auto new_pos = m_pgcache.get_new_pos();

		old_root.set_parent(new_pos);
		old_root.set_root(false);

		auto [midkey, sibling] = node_split(old_root);
		auto sibling_pos = m_pgcache.get_new_pos();

		Nod new_root{typename Nod::Metadata(typename Nod::Branch({midkey}, {old_pos, sibling_pos})), new_pos, true};

		m_pgcache.put_page(new_pos, new_root.make_page());
		m_pgcache.put_page(old_pos, old_root.make_page());
		m_pgcache.put_page(sibling_pos, sibling.make_page());

		m_rootpos = new_pos;
		++m_depth;
		m_header.m_dirty = true;

		return new_root;
	}

public:
	[[nodiscard]] auto root() noexcept { return std::as_const(*this).root(); }

	[[nodiscard]] auto root() const noexcept { return Nod::from_page(m_pgcache.get_page(rootpos())); }

	[[nodiscard]] auto &header() noexcept { return m_header; }

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

	void put(const Self::Key &key, const Self::Val &val) {
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
				m_pgcache.put_page(currpos, curr.make_page());
				++m_size;
				break;
			}

			auto &refs = curr.branch().m_refs;
			auto &links = curr.branch().m_links;

			const std::size_t index = std::lower_bound(refs.cbegin(), refs.cend(), key) - refs.cbegin();
			const Position child_pos = links[index];
			assert(child_pos.is_set());
			auto child = Nod::from_page(m_pgcache.get_page(child_pos));

			if (!is_node_full(child)) {
				currpos = child_pos;
				curr = std::move(child);
				continue;
			}

			auto [midkey, sibling] = node_split(child);
			auto sibling_pos = m_pgcache.get_new_pos();

			assert(std::find(refs.cbegin(), refs.cend(), midkey) == refs.cend());
			assert(std::find(links.cbegin(), links.cend(), sibling_pos) == links.cend());

			refs.insert(refs.begin() + index, midkey);
			links.insert(links.begin() + index + 1, sibling_pos);

			m_pgcache.put_page(sibling_pos, std::move(sibling.make_page()));
			m_pgcache.put_page(child_pos, std::move(child.make_page()));
			m_pgcache.put_page(currpos, std::move(curr.make_page()));

			if (key < midkey) {
				currpos = child_pos;
				curr = std::move(child);
			} else if (key > midkey) {
				currpos = sibling_pos;
				curr = std::move(sibling);
			}
		}
	}

	[[nodiscard]] std::optional<Val> get(const Self::Key &key) const noexcept {
		return search_subtree(root(), key);
	}

	[[nodiscard]] bool contains(const Self::Key &key) const noexcept {
		return search_subtree(root(), key).has_value();
	}

private:
	void save_header() const noexcept {
		if (m_header.dirty()) {
			m_header.rootpos() = m_rootpos;
			m_header.size() = m_size;
			m_header.dirty() = false;
		}

		nop::Serializer<nop::StreamWriter<std::ofstream>> serializer{m_header_name.data(), std::ios::trunc};
		serializer.Write(m_header) || nop::Die(std::cerr);
	}

	bool load_header() {
		nop::Deserializer<nop::StreamReader<std::ifstream>> deserializer{m_header_name.data()};
		deserializer.Read(&m_header) || nop::Die(std::cerr);
		std::cout << m_header << "\n";
		return true;
	}

	void bare_init() noexcept {
		m_rootpos = m_pgcache.get_new_pos();
		Nod root_initial{typename Nod::Metadata(typename Nod::Leaf({}, {})), m_rootpos, true};
		m_pgcache.put_page(m_rootpos, std::move(root_initial.make_page()));

		// Fill in initial header
		m_header.rootpos() = m_rootpos;
		m_header.size() = m_size;
		m_header.depth() = m_depth;
	}

public:
	/*
	 *  Persistence API
	 */

	void load() {
		auto ok = load_header();
		assert(ok);

		m_rootpos = m_header.m_rootpos;
		m_size = m_header.m_size;
		m_depth = m_header.m_depth;
	}

	void save() const {
		save_header();
		m_pgcache.flush_all();
	}

public:
	explicit Btree(std::string_view pgcache_name,
	               std::string_view btree_header_name = "/tmp/eu-btree-header",
	               bool should_load = false)
	    : m_pgcache{pgcache_name, PAGE_CACHE_SIZE},
	      m_header_name{btree_header_name} {

		assert(NUM_RECORDS_BRANCH > 1);
		assert(NUM_RECORDS_LEAF > 1);

		if (should_load)
			load();
		else
			bare_init();
	}

private:
	mutable PageCache m_pgcache;

	mutable Header m_header;
	const std::string_view m_header_name;

	Position m_rootpos;

	std::size_t m_size{0};

	std::size_t m_depth{0};
};

}// namespace internal::storage::btree
