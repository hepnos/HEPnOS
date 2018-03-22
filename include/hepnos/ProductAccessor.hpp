#ifndef __HEPNOS_PRODUCT_ACCESSOR_H
#define __HEPNOS_PRODUCT_ACCESSOR_H

#include <fstream>
#include <string>
#include <typeinfo>
#include <cxxabi.h>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <hepnos/InputTag.hpp>

namespace hepnos {

/**
 * This function takes an object name created by typeid(obj).name()
 * and returns a "friendly" (i.e. compiler-independent) version of
 * this name.
 */
static std::string demangle(const char* name) {
    int status = -4;
    char* res = abi::__cxa_demangle(name, NULL, NULL, &status);
    const char* const demangled_name = (status==0)?res:name;
    std::string ret_val(demangled_name);
    free(res);
    return ret_val;
}

template<typename BackendProductAccessor>
class ProductAccessor {

    private:

        BackendProductAccessor _backend;

    public:

        template<typename ... Args>
        ProductAccessor(Args&& ... args)
        : _backend(std::forward<Args>(args)...) {}

        template<typename T>
        void store(const InputTag& tag, const T& obj) {
            std::string objType = demangle(typeid(obj).name());
            std::stringstream ss;
            boost::archive::binary_oarchive oa(ss);
            oa << obj;
            std::string serialized = ss.str();
            std::vector<char> buffer(serialized.begin(), serialized.end());
            _backend.store(objType, tag, buffer);
        }

        template<typename T>
        bool load(const InputTag& tag, T& obj) {
            std::string objType = demangle(typeid(obj).name());
            std::vector<char> buffer;
            if(!_backend.load(objType, tag, buffer)) return false;
            std::string serialized(buffer.begin(), buffer.end());
            std::stringstream ss(serialized);
            boost::archive::binary_iarchive ia(ss);
            ia >> obj;
            return true;
        }
};

}
#endif
