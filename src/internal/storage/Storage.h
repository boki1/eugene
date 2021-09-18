#ifndef _EUGENE_STORAGEDEVICE_INCLUDED_
#define _EUGENE_STORAGEDEVICE_INCLUDED_

namespace internal::storage {

template<typename Dev>
concept StorageDevice = requires(Dev dev) {
	{ dev.close() } -> std::same_as<void>;
	{ dev.open() } -> std::same_as<void>;
	{ dev.read() } -> std::same_as<void>;
	{ dev.write() } -> std::same_as<void>;
	{ dev.seek() } -> std::same_as<void>;
};

}

#endif