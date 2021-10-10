#ifndef _EUGENE_BTREE_INCLUDED_
#define _EUGENE_BTREE_INCLUDED_

#include <memory>		// std::unique_ptr
#include <optional>		// std::nullopt_t

#include <internal/storage/Storage.h>

template <typename T>
consteval bool PowerOf2(T num) {
	return (num & (num - 1)) == 0;
}

namespace internal::btree
{

	template <typename Config>
	concept BtreeConfig = requires(Config conf) {
		typename Config::Key;
		typename Config::KeyRef;
		typename Config::Val;
		typename Config::StorageDev;

		requires std::convertible_to<decltype(conf.ApplyCompression), bool>;
		requires storage::StorageDevice<typename Config::StorageDev>;
		requires std::unsigned_integral<decltype(conf.BtreeNodeSize)>;
	} && PowerOf2(Config::BtreeNodeSize);

	struct DefaultBtreeConfig {
		using Key = uint32_t;
		using KeyRef = Key;
		using Val = uint32_t;
		using StorageDev = storage::DefaultStorageDev;
		static constexpr unsigned BtreeNodeSize = 1 << 10;
		static constexpr bool ApplyCompression = false;
	};

	template <typename Config=DefaultBtreeConfig>
		requires BtreeConfig<Config>
	class Btree {
	private:
		using Position = storage::Position;
		    
		using Key = typename Config::Key;
		using KeyRef = typename Config::KeyRef;
		using Val = typename Config::Val;
		using Storage = typename Config::StorageDev;
		    
		static constexpr bool ApplyCompression = Config::ApplyCompression;
		static constexpr uint32_t BtreeNodeSize = Config::BtreeNodeSize;

	public:
		class Node;

		class Iterator;

	public:
	    Btree() = default;

            template<typename... Args>
            explicit Btree(Args &&...args) noexcept
	    	: m_storage(std::forward<Args>(args)...)
	    {
	    }

	private:
            Storage m_storage;
            std::unique_ptr<Node> m_root;
	};

	template <BtreeConfig Config>
	class Btree<Config>::Node {

	};

	template <BtreeConfig Config>
	class Btree<Config>::Iterator {

	};

}



#endif // _EUGENE_BTREE_INCLUDED_
