#ifndef __HEPNOS_DEMANGLE_H
#define __HEPNOS_DEMANGLE_H

#include <boost/core/demangle.hpp>
#include <string>

namespace hepnos {

template<typename T>
std::string demangle() {
    char const * name = typeid(T).name();    
    return boost::core::demangle(name);
}

template<typename T>
std::string demangle(T&&) {
    return demangle<T>();
}

}

#endif
