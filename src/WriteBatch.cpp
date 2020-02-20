/*
 * (C) 2019 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "hepnos.hpp"
#include "WriteBatchImpl.hpp"

namespace hepnos {

WriteBatch::WriteBatch(DataStore& datastore)
: m_impl(std::make_unique<WriteBatchImpl>(datastore.m_impl)) {}

WriteBatch::~WriteBatch() {}

}
