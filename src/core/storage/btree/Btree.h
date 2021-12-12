#pragma once

#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <utility>

#include <gsl/pointers>

#include <fmt/core.h>

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

template<BtreeConfig Config = DefaultConfig>
class Btree final {
	using Self = Btree<Config>;

	using Key = typename Config::Key;
	using Val = typename Config::Val;
	using Ref = typename Config::Ref;
	using Nod = Node<Config>;

	using PosNod = std::pair<Position, Self::Nod>;

	inline static constexpr uint32_t NUM_RECORDS = Config::NUM_RECORDS;

public:
	struct Header {
		Position m_rootpos;
		uint32_t m_magic{0xB75EEA41};
		uint32_t m_numrecords{Config::NUM_RECORDS};
		uint32_t m_pgcache_size{Config::PAGE_CACHE_SIZE};
		uint32_t m_btree_node_size{Config::BTREE_NODE_SIZE};
		uint8_t m_apply_compression{Config::APPLY_COMPRESSION};

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
		explicit Header(Position rootpos, std::string_view content_file)
		    : m_rootpos{rootpos},
		      m_content_file{std::string{content_file}} {}

		void set_rootpos(Position rootpos) noexcept { m_rootpos = rootpos; }
		bool &dirty() noexcept { return m_dirty; }

		auto operator<=>(const Header &) const noexcept = default;

		friend std::ostream &operator<<(std::ostream &os, const Header &h) {
			os << "Header { .rootpos = " << h.m_rootpos << ", .numrecords = " << h.m_numrecords << " }";
			return os;
		}

		NOP_STRUCTURE(Header, m_rootpos, m_magic, m_numrecords, m_pgcache_size,
		              m_btree_node_size, m_apply_compression, m_content_file);
	};

private:
	[[nodiscard]] bool is_node_full(const Self::Nod &node) { return node.is_full(NUM_RECORDS); }

	[[nodiscard]] auto node_split(Self::Nod &node) { return node.split(NUM_RECORDS / 2); }

	[[nodiscard]] auto search_subtree(const Self::Nod &node, const Self::Key &target_key) const noexcept -> optional_cref<Val> {
		if (node.is_branch()) {
			const auto &refs = node.branch().m_refs;
			const std::size_t index = std::distance(refs.cbegin(), std::lower_bound(refs.cbegin(), refs.cend(), target_key));
			const Position pos = node.branch().m_links[index];
			const auto other = Nod::from_page(m_pgcache.get_page(pos));
			return search_subtree(other, target_key);
		}
		assert(node.is_leaf());

		const auto &keys = node.leaf().m_keys;
		const auto it = std::lower_bound(keys.cbegin(), keys.cend(), target_key);
		if (it == keys.cend() || *it != target_key)
			return {};
		return node.leaf().m_vals[std::distance(keys.cbegin(), it)];
	}

	void make_new_root() {
		auto &old_root = root();
		auto old_pos = m_rootpos;

		auto new_pos = m_pgcache.get_new_pos();

		old_root.set_parent(new_pos);
		old_root.set_root(false);

		auto [midkey, sibling] = old_root.split();
		auto sibling_pos = m_pgcache.get_new_pos();

		Nod new_root(typename Nod::Metadata(typename Nod::Branch({midkey}, {old_pos, sibling_pos})), new_pos, true);

		m_pgcache.put_page(new_pos, new_root.make_page());
		m_pgcache.put_page(old_pos, old_root.make_page());
		m_pgcache.put_page(sibling_pos, sibling.make_page());

		m_root = std::move(new_root);
		m_rootpos = new_pos;
		m_header.m_dirty = true;
	}

public:
	[[nodiscard]] auto &root() noexcept { return m_root; }
	[[nodiscard]] const auto &root() const noexcept { return m_root; }

	[[nodiscard]] auto &header() noexcept { return m_header; }

	[[nodiscard]] const auto &rootpos() const noexcept { return m_rootpos; }

public:
	/*
	 *  Operations API
	 */

	void put(const Self::Key &key, const Self::Val &val) {
		if (is_node_full(m_root))
			make_new_root();

		Nod *curr = &m_root;
		Position currpos = m_rootpos;

		while (1) {
			if (curr->is_leaf()) {
				auto &keys = curr->leaf().m_keys;
				auto &vals = curr->leaf().m_vals;
				const std::size_t index = std::distance(keys.begin(), std::lower_bound(keys.begin(), keys.end(), key));
				keys.insert(keys.begin() + index, key);
				vals.insert(vals.begin() + index, val);
				break;
			}

			auto &branch = curr->branch();
			const std::size_t index = std::distance(
			        std::lower_bound(branch.m_refs.begin(), branch.m_refs.end(), key), branch.m_refs.begin());
			const Position child_pos = branch.m_links[index];
			auto child = Nod::from_page(m_pgcache.get_page(child_pos));

			if (!child.is_full(NUM_RECORDS)) {
				curr = new Nod{std::move(child)};
				currpos = m_rootpos;
				continue;
			}

			auto [midkey, sibling] = child.split();
			m_pgcache.put_page(child_pos, child.make_page());
			auto sibling_pos = m_pgcache.get_new_pos();
			m_pgcache.put_page(sibling_pos, sibling.make_page());

			auto refs_it = branch.m_refs.begin() + index;
			branch.m_refs.insert(refs_it, midkey);
			auto links_it = branch.m_links.begin() + index;
			branch.m_links.insert(links_it, sibling_pos);
			if (key < midkey) {
				curr = new Nod{std::move(child)};
				currpos = child_pos;
			} else {
				curr = new Nod{std::move(sibling)};
				currpos = sibling_pos;
			}
		}
	}

	[[nodiscard]] auto get(const Self::Key &key) const noexcept -> optional_cref<Self::Val> {
		return search_subtree(root(), key);
	}

	[[nodiscard]] bool contains(const Self::Key &key) const noexcept {
		return search_subtree(root(), key).has_value();
	}

public:
	/*
	 *  Persistance API
	 */

	void save_header() const noexcept {
		if (m_header.dirty()) {
			fmt::print("Dirty header. Still at rootpos = {}, setting to {}\n", m_header.m_rootpos, m_rootpos);
			m_header.set_rootpos(m_rootpos);
			m_header.dirty() = false;
		}

		nop::Serializer<nop::StreamWriter<std::ofstream>> serializer{m_header_name.data(), std::ios::trunc};
		serializer.Write(m_header) || nop::Die(std::cerr);
		// fmt::print("Stored btree header\n");
		std::cout << m_header << "\n";
	}

	bool load_header() noexcept {
		nop::Deserializer<nop::StreamReader<std::ifstream>> deserializer{m_header_name.data()};
		deserializer.Read(&m_header) || nop::Die(std::cerr);
		// fmt::print("Loaded btree header\n");
		std::cout << m_header << "\n";
		return true;
	}

	void load_root() noexcept {
		if (load_header()) {
			m_rootpos = m_header.m_rootpos;

			// TODO:
			// m_root = std::move(Nod::from_page(m_pgcache.get_page(m_rootpos)));
		}
	}

	void save() const noexcept {
		save_header();
		m_pgcache.flush_all();
	}

public:
	explicit Btree(std::string_view pgcache_name,
	               std::string_view btree_header_name = "/tmp/eu-btree-header",
	               bool load = false)
	    : m_pgcache{pgcache_name, Config::PAGE_CACHE_SIZE},
	      m_header{m_pgcache.get_new_pos(), pgcache_name},
	      m_header_name{btree_header_name} {

		fmt::print("Constructing btree\n");

		if (load)
			load_root();
		else {
			m_rootpos = m_header.m_rootpos;
			m_root = Nod{typename Nod::Metadata(typename Nod::Leaf({}, {})), m_rootpos, true};
		}
	}

private:
	mutable PageCache m_pgcache;

	mutable Header m_header;
	const std::string_view m_header_name;

	Position m_rootpos;
	Nod m_root;
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
