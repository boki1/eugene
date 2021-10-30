#pragma once

#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <internal/storage/btree/Btree.h>

namespace internal::btree::util {

template<typename InputIt>
std::string join(InputIt begin, const InputIt end,
                        std::string_view delim) {
	std::stringstream ss;
	while (begin < end - 1)
		ss << *begin++ << delim;
	ss << *begin;
	return ss.str();
}

template<BtreeConfig Config>
class BtreeYAMLPrinter {
	using Bt = Btree<Config>;
	using Node = typename Bt::Node;

public:
	explicit BtreeYAMLPrinter(const Bt &bt, std::string ofname = "bpt.yml")
	    : m_ofname{std::move(ofname)}, m_btree{bt}, m_out{m_ofname,
	                                                      std::ios::out} {}

	void operator()() noexcept { print(); }

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
			auto links = node->branch().links_;
			for (auto it = links.begin(); it != links.end() && *it != Position::poison(); ++it) {
				print_node(&Node::Cache::the().fetch(*it), level + 1);
			}
		}
	}

	void print() noexcept {
		m_out << "keys_per_block: " << Node::num_records_per_node() << '\n';
		m_out << "tree:\n";

		print_node(m_btree.m_root);
	}

private:
	std::string m_ofname;
	const Btree<Config> &m_btree;
	std::ofstream m_out;
};

}// namespace internal::btree::util
