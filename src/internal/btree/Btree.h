#ifndef _EUGENE_BTREE_INCLUDED_
#define _EUGENE_BTREE_INCLUDED_

#include <memory>
#include <cstdint>
#include <vector>
#include <utility>
#include <pthread.h>

#include <internal/storage/Storage.h>
#include <internal/storage/Compression.h>

namespace internal::btree {

template<typename Config>
concept BtreeConfig = requires(Config conf) {
	typename Config::Key;
	typename Config::Val;
	typename Config::StorageDev;
	conf.BranchingFactor;
	conf.ApplyCompression;

	requires std::convertible_to<decltype(conf.ApplyCompression), bool>;
	requires storage::StorageDevice<typename Config::StorageDev>;
	requires std::unsigned_integral<decltype(conf.BranchingFactor)>;
};

template<BtreeConfig config>
class Btree {
	using Position = off_t;

	using Key = config::Key;
	using Val = config::Val;
	using StorageDev = config::StorageDev;
	using Storage = StorageDev;

	static constexpr decltype(config::BranchingFactor) BranchingFactor = config::BranchingFactor;
	static constexpr decltype(config::BranchingFactor) ApplyCompression = config::ApplyCompression;

	struct Node {
	  enum class Type : uint8_t {
		  Internal,
		  Leaf
	  };

	  enum Type m_type;
	  uint64_t m_size;
	  Position m_pos;
	  Position m_leftpos;
	  Position m_rightpos;
	  std::vector<Key> m_keys;
	};

	struct InMemoryNode {
	  // ?
	};

public:

	template<typename... Args>
	explicit Btree(Args&& ... args)
			: m_storage(std::forward<Args>(args)...)
	{
//		m_superpos = m_storage.superpos();
	}

	virtual ~Btree() noexcept
	{
		// todo: maybe wrap pthread_rwlock in a RAII struct, expecially if multiplatform
		int rc = pthread_rwlock_destroy(&m_lock);
	}

public:

private:
	const Storage m_storage;
	std::unique_ptr<Node> m_super{nullptr};
	Position m_superpos;
	pthread_rwlock_t m_lock{PTHREAD_RWLOCK_DEFAULT_NP};

public:
	class Iterator;
};

}

#endif