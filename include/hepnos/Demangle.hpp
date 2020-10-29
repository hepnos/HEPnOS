/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_DEMANGLE_H
#define __HEPNOS_DEMANGLE_H

#include <boost/core/demangle.hpp>
#include <string>

namespace hepnos {

/**
 * @brief Returns the name of a type.
 *
 * @tparam T Type from which to return the name.
 *
 * @return Name of type T.
 */
template<typename T>
std::string demangle() {
    char const * name = typeid(T).name();
    return boost::core::demangle(name);
}

/**
 * @brief Returns the name of type, which example
 * instance provided for type deduction.
 */
template<typename T>
std::string demangle(T&&) {
    return demangle<T>();
}

}

#endif
