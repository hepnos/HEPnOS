/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_UUID_UTIL_HPP
#define __HEPNOS_UUID_UTIL_HPP

#include <string>
#include <cstring>
#include <thallium/serialization/stl/string.hpp>
#include <boost/serialization/split_member.hpp>

namespace hepnos {

/**
 * @brief UUID class (Universally Unique IDentifier).
 */
struct UUID {

    unsigned char data[16];

    public:

    /**
     * @brief Constructor, produces a zero-ed UUID.
     */
    UUID();

    /**
     * @brief Copy constructor.
     */
    UUID(const UUID& other) = default;

    /**
     * @brief Move constructor.
     */
    UUID(UUID&& other) = default;

    /**
     * @brief Copy-assignment operator.
     */
    UUID& operator=(const UUID& other) = default;

    /**
     * @brief Move-assignment operator.
     */
    UUID& operator=(UUID&& other) = default;

    /**
     * @brief Converts the UUID into a string.
     *
     * @return a readable string representation of the UUID.
     */
    std::string to_string() const;

    BOOST_SERIALIZATION_SPLIT_MEMBER()

    /**
     * @brief Serialization of a UUID into an archive.
     *
     * @tparam Archive
     * @param a
     * @param version
     */
    template<typename Archive>
    void save(Archive& a, const unsigned int version) const {
        (void)version;
        a & std::string(data, 16);
    }

    /**
     * @brief Deserialization of a UUID from an archive.
     *
     * @tparam Archive
     * @param a
     * @param version
     */
    template<typename Archive>
    void load(Archive& a, const unsigned int version) {
        (void)version;
        std::string s;
        a & s;
        std::memcpy(data, s.data(), 16);
    }

    /**
     * @brief Converts the UUID into a string and pass it
     * to the provided stream.
     */
    template<typename T>
    friend T& operator<<(T& stream, const UUID& id);

    /**
     * @brief Generates a random UUID.
     *
     * @return a random UUID.
     */
    static UUID generate();

    /**
     * @brief randomize the current UUID.
     */
    void randomize();

    /**
     * @brief Compare the UUID with another UUID.
     */
    bool operator==(const UUID& other) const;

    /**
     * @brief Computes a hash of the UUID.
     *
     * @return a uint64_t hash value.
     */
    uint64_t hash() const;
};

template<typename T>
T& operator<<(T& stream, const UUID& id) {
    stream << id.to_string();
    return stream;
}

}

#endif
