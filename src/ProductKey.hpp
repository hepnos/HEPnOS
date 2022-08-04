/*
 * (C) 2022 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRODUCT_KEY
#define __HEPNOS_PRODUCT_KEY

namespace hepnos {

struct ProductKey {

    std::string label;
    std::string type;

    ProductKey(std::string l, std::string t)
        : label(std::move(l)), type(std::move(t)) {}

    bool operator==(const ProductKey& other) const {
        return label == other.label && type == other.type;
    }

    struct hash {
        std::size_t operator()(const ProductKey& pk) const {
            static const auto hs = std::hash<std::string>();
            return hs(pk.label) ^ (~hs(pk.type));
        }
    };
};

}

#endif
