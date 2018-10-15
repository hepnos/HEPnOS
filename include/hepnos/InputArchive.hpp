/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_INPUT_ARCHIVE_H
#define __HEPNOS_INPUT_ARCHIVE_H

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/impl/basic_binary_iprimitive.ipp>
#include <boost/archive/impl/basic_binary_iarchive.ipp>

namespace hepnos {

class DataStore;
class InputArchive;

using iarchive = boost::archive::binary_iarchive_impl<InputArchive, std::istream::char_type, std::istream::traits_type>;

/**
 * @brief This InputArchive class derives from boost's binary input archive.
 * It lets caller embed a pointer to the DataStore storing the object.
 * This is necessary to deserialize Ptr instances, for example.
 */
class InputArchive : public iarchive {

    friend class boost::archive::detail::interface_iarchive<InputArchive>;
    friend class boost::archive::basic_binary_iarchive<InputArchive>;
    friend class boost::archive::load_access;

    private:

    DataStore* m_datastore = nullptr;

    public:

    template<typename ... Args>
    InputArchive(DataStore* datastore, Args&& ... args)
    : iarchive(std::forward<Args>(args)..., 0)
    , m_datastore(datastore) {}

    DataStore* getDataStore() const {
        return m_datastore;
    }

};

}

#endif
