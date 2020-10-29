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

    DataStore m_datastore;

    public:

    /**
     * @brief Constructor.
     *
     * @tparam Args Argument types necessary for the underlying boost iarchive.
     * @param datastore DataStore.
     * @param args Arguments necessary for the parent boost iarchive.
     */
    template<typename ... Args>
    InputArchive(DataStore&& datastore, Args&& ... args)
    : iarchive(std::forward<Args>(args)..., boost::archive::archive_flags::no_header)
    , m_datastore(std::move(datastore)) {}

    /**
     * @brief Constructor.
     *
     * @tparam Args Argument types necessary for the underlying boost iarchive.
     * @param datastore DataStore.
     * @param args Arguments necessary for the parent boost iarchive.
     */
    template<typename ... Args>
    InputArchive(const DataStore& datastore, Args&& ... args)
    : iarchive(std::forward<Args>(args)..., boost::archive::archive_flags::no_header)
    , m_datastore(datastore) {}

    /**
     * @brief Returns the DataStore used when creating the InputArchive.
     */
    const DataStore& datastore() const {
        return m_datastore;
    }

};

}

#endif
