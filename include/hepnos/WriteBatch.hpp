/*
 * (C) 2019 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_WRITE_BATCH_H
#define __HEPNOS_WRITE_BATCH_H

#include <memory>
#include <string>
#include <vector>

#include <hepnos/KeyValueContainer.hpp>
#include <hepnos/ProductID.hpp>
#include <hepnos/DataStore.hpp>
#include <hepnos/Exception.hpp>

namespace hepnos {

class DataStore;
class DataSet;
class Run;
class SubRun;
class Event;
class WriteBatchImpl;
class AsyncEngine;

class WriteBatch {

    friend class DataStore;
    friend class DataSet;
    friend class Run;
    friend class SubRun;
    friend class Event;
    friend class KeyValueContainer;

    private:

    std::unique_ptr<WriteBatchImpl> m_impl;

    public:

    WriteBatch(DataStore& ds, unsigned max_batch_size=128);
    WriteBatch(DataStore& ds, AsyncEngine& async, unsigned max_batch_size=128);
    ~WriteBatch();
    WriteBatch(const WriteBatch&) = delete;
    WriteBatch& operator=(const WriteBatch&) = delete;
    WriteBatch(WriteBatch&&) = delete;
    WriteBatch& operator=(WriteBatch&&) = delete;

};

}

#endif
