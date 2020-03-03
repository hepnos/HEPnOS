#ifndef __HEPNOS_ITEM_TYPE_HPP
#define __HEPNOS_ITEM_TYPE_HPP

namespace hepnos {

enum class ItemType : uint32_t {
    DATASET,
    RUN,
    SUBRUN,
    EVENT,
    PRODUCT
};

}

#endif
