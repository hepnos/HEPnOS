/*
 * (C) The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_OUTPUT_ARCHIVE_H
#define __HEPNOS_OUTPUT_ARCHIVE_H

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/impl/basic_binary_oprimitive.ipp>
#include <boost/archive/impl/basic_binary_oarchive.ipp>

namespace hepnos {

class DataStore;
class OutputArchive;

using oarchive = boost::archive::binary_oarchive_impl<OutputArchive, std::ostream::char_type, std::ostream::traits_type>;

/**
 * @brief This OnputArchive class derives from boost's binary onput archive.
 */
class OutputArchive : public oarchive {

    friend class boost::archive::detail::interface_oarchive<OutputArchive>;
    friend class boost::archive::basic_binary_oarchive<OutputArchive>;
    friend class boost::archive::save_access;

    public:

    /**
     * @brief Constructor.
     *
     * @tparam Args Argument types necessary for the underlying boost oarchive.
     * @param datastore DataStore.
     * @param args Arguments necessary for the parent boost iarchive.
     */
    template<typename ... Args>
    OutputArchive(Args&& ... args)
    : oarchive(std::forward<Args>(args)..., boost::archive::archive_flags::no_header) {
        init(boost::archive::archive_flags::no_header);
    }

};

}

BOOST_SERIALIZATION_USE_ARRAY_OPTIMIZATION(hepnos::OutputArchive)
BOOST_SERIALIZATION_REGISTER_ARCHIVE(hepnos::OutputArchive)

#endif
