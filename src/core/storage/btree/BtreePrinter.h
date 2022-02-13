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
	auto begin = std::cbegin(collection);
	const auto end = std::cend(collection);
    if (begin == end) {
        return "";
	}

	std::stringstream ss;
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
		return Nod::from_page(m_btree.m_pager->get(pos));
	}

	void print_node(const Nod node, unsigned level = 1) noexcept {
		const std::string indentation(level * 2, ' ');
		m_out << indentation;
		if (level > 1)
			m_out << "- ";
		if (node.is_branch() || level == 1)
			m_out << "keys: ";
		if (node.is_leaf()) {
			m_out << '(' << node.leaf().keys.size() << ") [" << join(node.leaf().keys, ", ") << "]\n";
			return;
		}

		m_out << '[' << join(node.branch().refs, ", ") << "]\n";
		m_out << indentation << (level > 1 ? "  " : "") << "children:\n";
		for (std::size_t i = 0; i < node.branch().links.size(); ++i)
			if (node.branch().link_status[i] == LinkStatus::Inval)
				fmt::print("- Empty\n");
			else
				print_node(node_at(node.branch().links[i]), level + 1);
	}

	void print() noexcept {
		m_out << "keys-in-leaves: [" << m_btree.min_num_records_leaf() << "; " << m_btree.max_num_records_leaf() << "]\n";
		m_out << "keys-in-branches: [" << m_btree.min_num_records_branch() << "; " << m_btree.max_num_records_branch() << "]\n";
		m_out << "tree:\n";

		print_node(node_at(m_btree.rootpos()));
	}

private:
	std::string m_ofname;
	Bt &m_btree;
	std::ofstream m_out;
};

}// namespace internal::storage::btree::util
