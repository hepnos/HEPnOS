#include "hepnos/UUID.hpp"
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
