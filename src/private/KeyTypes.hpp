/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PRIVATE_KEY_TYPES_H
#define __PRIVATE_KEY_TYPES_H

#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <memory>

namespace hepnos {

struct DataStoreEntry {
    uint8_t m_level;
    char    m_fullname[1];

    DataStoreEntry()  = delete;
    ~DataStoreEntry() = delete;

    size_t length() const {
        return sizeof(*this) + strlen(m_fullname);
    }

    const void* raw() const {
        return static_cast<const void*>(this);
    }

    void* raw() {
        return static_cast<void*>(this);
    }
};

struct DataStoreEntryDeleter {
    void operator()(DataStoreEntry* entry) {
        free(entry);
    }
};

typedef std::unique_ptr<DataStoreEntry, DataStoreEntryDeleter> DataStoreEntryPtr;

inline DataStoreEntryPtr make_datastore_entry(uint8_t level, const std::string& name) {
    size_t s = sizeof(DataStoreEntry) + name.size();
    DataStoreEntry* entry = static_cast<DataStoreEntry*>(calloc(1,s));
    entry->m_level = level;
    strcpy(entry->m_fullname, name.c_str());
    return DataStoreEntryPtr(entry, DataStoreEntryDeleter());
}

inline DataStoreEntryPtr make_datastore_entry(uint8_t level, size_t nameSize) {
    size_t s = sizeof(DataStoreEntry) + nameSize;
    DataStoreEntry* entry = static_cast<DataStoreEntry*>(calloc(1,s));
    entry->m_level = level;
    return DataStoreEntryPtr(entry, DataStoreEntryDeleter());
}

}

#endif
