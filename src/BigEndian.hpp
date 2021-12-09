#ifndef __HEPNOS_BIG_ENDIAN_HPP
#define __HEPNOS_BIG_ENDIAN_HPP

#include <endian.h>

namespace hepnos {

template<size_t I>
struct BE_helper;

template<>
struct BE_helper<2> {
    static auto htobe(uint16_t val) {
        return htobe16(val);
    }
    static auto betoh(uint16_t val) {
        return be16toh(val);
    }
};

template<>
struct BE_helper<4> {
    static auto htobe(uint32_t val) {
        return htobe32(val);
    }
    static auto betoh(uint32_t val) {
        return be32toh(val);
    }
};

template<>
struct BE_helper<8> {
    static auto htobe(uint64_t val) {
        return htobe64(val);
    }
    static auto betoh(uint64_t val) {
        return be64toh(val);
    }
};

template<typename T>
class BE {

    T value = 0;

    public:

    BE() = default;

    BE(T val)
    : value(BE_helper<sizeof(T)>::htobe(val)) {}

    BE(const BE& other) = default;
    BE(BE&& other) = default;
    BE& operator=(const BE& other) = default;
    BE& operator=(BE&& other) = default;

    operator T() const {
        return BE_helper<sizeof(T)>::betoh(value);
    }
};

}

#endif
