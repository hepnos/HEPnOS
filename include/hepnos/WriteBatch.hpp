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

/**
 * @brief The WriteBatch oject can be used to batch
 * operations such as creating Runs, SubRuns, and Events,
 * as well as storing products into the underlying DataStore.
 */
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

    /**
     * @brief Constructor.
     *
     * Note: the max_batch_size is not the maximum capacity of the WriteBatch.
     * The WriteBatch has virtually unlimited capacity and will only flush its
     * content when instructed to (i.e. calling flush()) or when it is destroyed.
     *
     * @param ds DataStore in which to write.
     * @param max_batch_size Maximum batch size when issuing an operation.
     */
    WriteBatch(DataStore& ds, unsigned max_batch_size=128);

    /**
     * @brief Constructor using and AsyncEngine to write the batches
     * asynchronously.
     *
     * Note: when using an AsyncEngine, the AsyncEngine will continuously
     * try to write the content of the WriteBatch into storage, issuing
     * variable-sized batches of up to max_batch_size.
     *
     * @param async AsyncEngine to use to write asynchronously.
     * @param max_batch_size Maximum batch size when issuing an operation.
     */
    WriteBatch(AsyncEngine& async, unsigned max_batch_size=128);
    
    /**
     * @brief Destructor.
     */
    ~WriteBatch();
    
    /**
     * @brief Deleted copy constructor.
     */
    WriteBatch(const WriteBatch&) = delete;
    
    /**
     * @brief Deleted move constructor.
     */
    WriteBatch& operator=(const WriteBatch&) = delete;
    
    /**
     * @brief Deleted copy-assignment operator.
     */
    WriteBatch(WriteBatch&&) = delete;
    
    /**
     * @brief Deleted move-assignment operator.
     */
    WriteBatch& operator=(WriteBatch&&) = delete;
    
    /**
     * @brief Flush the content of the WriteBatch, blocking until
     * everything is flushed.
     */
    void flush();
};

}

#endif
