#ifndef __HEPNOS_UUID_UTIL_HPP
#define __HEPNOS_UUID_UTIL_HPP

#include <string>
#include <cstring>
#include <thallium/serialization/stl/string.hpp>
#include <boost/serialization/split_member.hpp>

namespace hepnos {

struct UUID {

    unsigned char data[16];

    public:

    UUID();
    UUID(const UUID& other) = default;
    UUID(UUID&& other) = default;
    UUID& operator=(const UUID& other) = default;
    UUID& operator=(UUID&& other) = default;

    std::string to_string() const;

    BOOST_SERIALIZATION_SPLIT_MEMBER()

    template<typename Archive>
    void save(Archive& a, const unsigned int version) const {
        (void)version;
        a & std::string(data, 16);
    }

    template<typename Archive>
    void load(Archive& a, const unsigned int version) {
        (void)version;
        std::string s;
        a & s;
        std::memcpy(data, s.data(), 16);
    }

    template<typename T>
    friend T& operator<<(T& stream, const UUID& id);

    static UUID generate();

    void randomize();

    bool operator==(const UUID& other) const;

    uint64_t hash() const;
};

template<typename T>
T& operator<<(T& stream, const UUID& id) {
    stream << (std::string)id;
    return stream;
}

}

#endif
