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

}
