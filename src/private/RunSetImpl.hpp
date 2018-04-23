#ifndef __HEPNOS_PRIVATE_RUNSET_IMPL_H
#define __HEPNOS_PRIVATE_RUNSET_IMPL_H

#include "hepnos/RunSet.hpp"
#include "hepnos/DataSet.hpp"

namespace hepnos {

class RunSet::Impl {

    public:

    DataSet* m_dataset;
    iterator m_end;

    Impl(DataSet* ds)
    : m_dataset(ds) {}
};

}

#endif
