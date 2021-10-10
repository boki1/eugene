#ifndef _EUGENE_STORAGEDEVICE_INCLUDED_
#define _EUGENE_STORAGEDEVICE_INCLUDED_

#include <concepts>

#include <external/expected/Expected.h>

namespace internal::storage
{
    	using Position = unsigned long int;
    	static constexpr Position PositionPoison = 0x41CEBEEF;
    
	/*
	* TODO
	*/
	template<typename Dev>
	concept StorageDevice = true;

	struct DefaultStorageDev {

	};

	static_assert(StorageDevice<DefaultStorageDev>, "Default storage device is not a storage device!");
}

#endif

