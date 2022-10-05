/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos/UUID.hpp"
#include "hepnos/BigEndian.hpp"
#include <uuid/uuid.h>
#include <cstring>

namespace hepnos {

UUID::UUID() {}

std::string UUID::to_string() const {
    std::string result(32,'\0');
    uuid_unparse(data, const_cast<char*>(result.data()));
    return result;
}

void UUID::randomize() {
    uuid_generate_random(data);
}

UUID UUID::generate() {
    UUID result;
    result.randomize();
    return result;
}

UUID UUID::fromStringHash(const std::string& str) {
    uint64_t h1 = 0, h2 = 0;
    h1 = std::hash<std::string>()(str);
    for(int i = str.size(); i >= 0; --i) {
        h2 = 31*h2 + str[i];
    }
    h1 = BE_helper<8>::htobe(h1);
    h2 = BE_helper<8>::htobe(h2);
    UUID result;
    std::memcpy(result.data, &h1, sizeof(h1));
    std::memcpy(result.data+sizeof(h1), &h2, sizeof(h2));
    return result;
}

bool UUID::operator==(const UUID& other) const {
    int c = memcmp(data, other.data, sizeof(data));
    return c == 0;
}

struct UUID_as_pair {
    uint64_t a, b;
};

uint64_t UUID::hash() const {
    auto p = reinterpret_cast<const UUID_as_pair*>(data);
    return p->a ^ p->b;
}

}
