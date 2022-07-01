/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PRIVATE_DATABASE_ADAPTOR
#define __HEPNOS_PRIVATE_DATABASE_ADAPTOR

#include <yokan/cxx/database.hpp>

namespace hepnos {

struct DatabaseAdaptor {

    DatabaseAdaptor(yokan::Database&& db)
    : m_db(std::move(db)) {}

    DatabaseAdaptor(const yokan::Database& db)
    : m_db(db) {}

#define DEFINE_ADAPTOR_FOR_FUNCTION(__fun__, __constness__) \
    template<typename ... T> \
    auto __fun__(T&&... args) __constness__ { \
        while(true) { \
            try { \
                return m_db.__fun__(std::forward<T>(args)...); \
            } catch(const yokan::Exception& ex) { \
                if(ex.code() == YOKAN_ERR_FROM_MERCURY) \
                    continue; \
                throw; \
            } \
        } \
        return m_db.__fun__(std::forward<T>(args)...); \
    }

    DEFINE_ADAPTOR_FOR_FUNCTION(get, const)
    DEFINE_ADAPTOR_FOR_FUNCTION(length, const)
    DEFINE_ADAPTOR_FOR_FUNCTION(put, const)
    DEFINE_ADAPTOR_FOR_FUNCTION(exists, const)
    DEFINE_ADAPTOR_FOR_FUNCTION(listKeysPacked, const)
    DEFINE_ADAPTOR_FOR_FUNCTION(listKeyValsPacked, const)
    DEFINE_ADAPTOR_FOR_FUNCTION(putPacked, const)
    DEFINE_ADAPTOR_FOR_FUNCTION(lengthPacked, const)
    DEFINE_ADAPTOR_FOR_FUNCTION(getPacked, const)

#undef DEFINE_ADAPTOR_FOR_FUNCTION

    private:
    yokan::Database m_db;
};

}

#endif
