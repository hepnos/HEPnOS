/*
 * (C) The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_OUTPUT_ARCHIVE_H
#define __HEPNOS_OUTPUT_ARCHIVE_H

#include <iosfwd>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/impl/basic_binary_oprimitive.ipp>
#include <boost/archive/impl/basic_binary_oarchive.ipp>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/back_inserter.hpp>

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

class OutputSizer {

    public:

    typedef char                       char_type;
    typedef boost::iostreams::sink_tag category;

    std::streamsize write(const char_type* s, std::streamsize n) {
        (void)s;
        *m_size += n;
        return n;
    }

    size_t size() const {
        return *m_size;
    }

    OutputSizer()
    : m_size(std::make_shared<size_t>(0)) {}

    OutputSizer(const OutputSizer&) = default;
    OutputSizer(OutputSizer&&) = default;
    OutputSizer& operator=(const OutputSizer&) = default;
    OutputSizer& operator=(OutputSizer&&) = default;

    private:

    std::shared_ptr<size_t> m_size;
};

using OutputStringWrapper = boost::iostreams::back_insert_device<std::string>;
using OutputStream = boost::iostreams::stream<OutputStringWrapper>;
using OutputSizeEvaluator = boost::iostreams::stream<OutputSizer>;

}

BOOST_SERIALIZATION_USE_ARRAY_OPTIMIZATION(hepnos::OutputArchive)
BOOST_SERIALIZATION_REGISTER_ARCHIVE(hepnos::OutputArchive)

#endif
