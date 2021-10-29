#pragma once

#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <internal/btree/Btree.h>

namespace internal::btree::util {

template<BtreeConfig Config>
class BtreeYAMLPrinter {
	using Bt = Btree<Config>;
	using Node = typename Bt::Node;

public:
	explicit BtreeYAMLPrinter(const Bt &bt, std::string ofname = "bpt.yml")
	    : m_ofname{std::move(ofname)}, m_btree{bt}, m_out{m_ofname,
	                                                      std::ios::out} {}

	void operator()() noexcept { print(); }

	template<typename InputIt>
	static std::string join(InputIt begin, const InputIt end,
	                        std::string_view delim) {
		std::stringstream ss;
		while (begin < end - 1)
			ss << *begin++ << delim;
		ss << *begin;
		return ss.str();
	}

	void print_node(const Node *node, unsigned level = 1) noexcept {
		const std::string indentation(level * 2, ' ');
		m_out << indentation;
		if (level > 1)
			m_out << "- ";
		if (node->is_branch() || level == 1)
			m_out << "keys: ";
		auto [beginning, _] = node->range();
		const auto actual_end = beginning + node->numfilled();
		m_out << '[' << join(beginning, actual_end, ", ") << "]\n";
		if (node->is_branch()) {
			m_out << indentation << "children: \n";
			for (const auto link : node->branch().links_)
				print_node(Node::fetch_node(link), level + 1);
		}
	}

	void print() noexcept {
		m_out << "keys_per_block: " << Node::records_() << '\n';
		m_out << "tree:\n";

		print_node(&m_btree.m_root);
	}

private:
	std::string m_ofname;
	const Btree<Config> &m_btree;
	std::ofstream m_out;
};

}// namespace internal::btree::util
