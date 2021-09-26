#ifndef _EUGENE_COMPRESSION_INCLUDED_
#define _EUGENE_COMPRESSION_INCLUDED_

namespace internal::compression {

template<typename T>
std::vector<uint8_t> compress(const T&);

template<typename T>
T decompress(std::vector<uint8_t> v);

}

#endif