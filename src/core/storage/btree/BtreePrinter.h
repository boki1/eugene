#pragma once

#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <core/storage/Pager.h>
#include <core/storage/btree/Btree.h>
#include <core/storage/btree/Config.h>
#include <core/storage/btree/Node.h>

namespace internal::storage::btree::util {

template<typename Collection>
static std::string join(const Collection& collection, std::string_view delim) {
	std::stringstream ss;
	auto begin = std::cbegin(collection);
	const auto end = std::cend(collection);
	while (begin < end - 1)
		ss << *begin++ << delim;
	ss << *begin;
	return ss.str();
}

template<BtreeConfig Config>
class BtreePrinter {
	using Bt = Btree<Config>;
	using Nod = Node<Config>;

public:
	explicit BtreePrinter(Bt &bt, std::string_view ofname = "/tmp/bpt_view")
	    : m_ofname{ofname},
	      m_btree{bt},
	      m_out{m_ofname, std::ios::out | std::ios::trunc} {}

	void operator()() noexcept { print(); }

	Nod node_at(Position pos) {
		return Nod::from_page(m_btree.m_pager.get(pos));
	}

	void print_node(const Nod node, unsigned level = 1) noexcept {
		const std::string indentation(level * 2, ' ');
		m_out << indentation;
		if (level > 1)
			m_out << "- ";
		if (node.is_branch() || level == 1)
			m_out << "keys: ";
		if (node.is_leaf()) {
			m_out << '(' << node.leaf().m_keys.size() << ") [" << join(node.leaf().m_keys, ", ") << "]\n";
			return;
		}

		m_out << '[' << join(node.branch().m_refs, ", ") << "]\n";
		m_out << indentation << (level > 1 ? "  " : "") << "children: \n";
		for (const auto pos : node.branch().m_links)
			print_node(node_at(pos), level + 1);
	}

	void print() noexcept {
		fmt::print("keys-in-leaves: [{}; {}]\n", m_btree.min_num_records_leaf(), m_btree.max_num_records_leaf());
		fmt::print("keys-in-branches: [{}; {}]\n", m_btree.min_num_records_branch(), m_btree.max_num_records_branch());
		m_out << "tree:\n";

		print_node(node_at(m_btree.rootpos()));
	}

private:
	std::string m_ofname;
	Bt &m_btree;
	std::ofstream m_out;
};

}// namespace internal::storage::btree::util
