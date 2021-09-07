/*
 * @file BTree.h
 * @brief Defines the structure of B*+ tree and specifies its API
 */
#ifndef EUGENE_BTREE_INCLUDED
#define EUGENE_BTREE_INCLUDED

#include <memory>
#include <vector>
#include <iterator>
#include <map>

using std::unique_ptr;
using std::vector;
using std::map;

namespace eugene::eugene_internal::btree {

template <unsigned Order, typename ShallowKeyType, typename KeyType, typename ValueType>
class BTree {

    class BTreeNode {
    public:
        template <typename KeyRef, typename ValRef> requires
            std::is_same<typename std::decay<ValRef>::type, ValueType>::value &&
            std::is_same<typename std::decay<KeyRef>::type, KeyType>::value
        BTreeNode(map<KeyRef, ValRef> elements) : children{} {}

    public:
        void insert_meta_pair(/* Key key, Value val */) {}
        
    private:
        void store_meta_in_leaves() {}

    private:
	    vector<unique_ptr<BTreeNode>> children;
	    vector<ShallowKeyType> key_refs;
    };

    /*
     * @brief Bidirectional iterator over the tree
     */
    class Iterator {
    public:
        /*
         * @brief By default constructs an end iterator
         */
        Iterator() = default;

        /*
         * @brief Equivalent to getting the begin iterator
         */
        explicit Iterator(const BTree &t_tree) {}

        /*
         * @brief Start from the given node of the given tree
         */
        explicit Iterator(const BTree &t_tree, const BTreeNode &t_node) {}

        Iterator(const Iterator &) = default;

        Iterator &operator=(const Iterator &) = default;

        Iterator operator++(int) {}
        Iterator& operator++() {}

        Iterator operator--(int) {}
        Iterator& operator--() {}

        BTreeNode& operator*() {}
        BTreeNode* operator->() {}

        bool operator==(const Iterator &) const { return false; }
        bool operator!=(const Iterator &) const { return false; }

        friend void swap(Iterator &, Iterator &)
        {
            using std::swap;
            // Can use std::swap() as swap(..., ...);
        }

    private:
        BTree &tree;
        BTreeNode &cur;
    };

   public :

    /*
	 * @brief Empty construction
	 */
    BTree() = default;
    
    /*
     * @brief Construction from a graph stored in file
     * @param 	infname 	filename
     */
    explicit BTree(const std::string &infname) {}

   public:
    /*
     * @brief Look for a given key in the tree
     * @param 	desired_key 	The key we are searching for
     * @ret 	Iterator to result - either referencing the found element or end
     */
    Iterator find(const KeyType &desired_key) {}

	/*
	 * @brief Insert a key-value pair into the tree
	 * @param 	key 		The key to be inserted
	 * @param 	value 		The value associated with the key
	 * @ret 	Iterator to the place where the key resides after insertion of an Err
	 */
    template <typename ValueRef>
        requires std::is_same<typename std::decay<ValueRef>::type, ValueType>::value
    Iterator insert(const KeyType &key, ValueRef &&value) {
		// Do not forget to std::forward<ValueRef>(value)
    }

	/*
	 * @brief Remove a key from the tree
	 * @param 	Iterator referencing the key that is going to be removed
	 * @ret 	[Nothing]
	 */
	void remove(Iterator it) {}

    /*
     * @brief   Get an iterator to the beginning of the tree
     * @ret     The begin iterator
     */
    Iterator begin() {}

    /*
     * @brief   Get an iterator to the end of the tree
     * @ret     The end iterator
     */
    Iterator end() {}

   private:
    unique_ptr<BTreeNode> root;
    unsigned height{0};
};

}  // namespace eugene::eugene_internal::btree

#endif	// include guard

