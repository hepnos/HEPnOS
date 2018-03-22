#ifndef __HEPNOS_INPUT_TAG_H
#define __HEPNOS_INPUT_TAG_H

#include <string>

namespace hepnos {

struct InputTag {

    std::string moduleLabel;
    std::string instanceName;
    std::string processName;

    InputTag(const std::string& ml,
             const std::string& in,
             const std::string& pn)
        : moduleLabel(ml),
          instanceName(in),
          processName(pn) {}
};

}

#endif
