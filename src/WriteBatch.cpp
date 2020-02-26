/*
 * (C) 2019 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos.hpp"
#include "WriteBatchImpl.hpp"
#include "hepnos/AsyncEngine.hpp"

namespace hepnos {

WriteBatch::WriteBatch(DataStore& datastore, unsigned max_batch_size)
: m_impl(std::make_unique<WriteBatchImpl>(datastore.m_impl, max_batch_size)) {}

WriteBatch::WriteBatch(DataStore& datastore, AsyncEngine& async, unsigned max_batch_size)
: m_impl(std::make_unique<WriteBatchImpl>(datastore.m_impl, max_batch_size, async.m_impl)) {}

WriteBatch::~WriteBatch() {}

}
