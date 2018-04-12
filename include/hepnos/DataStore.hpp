#ifndef __HEPNOS_DATA_STORE_H
#define __HEPNOS_DATA_STORE_H

#include <vector>
#include <string>
#include <memory>

namespace hepnos {

class DataSet;

class DataStore {

    friend class DataSet;

    public:

        DataStore(const std::string& configFile);

        DataStore(const DataStore&) = delete;
        DataStore(DataStore&&);
        DataStore& operator=(const DataStore&) = delete;
        DataStore& operator=(DataStore&&);
    
        ~DataStore();

        class iterator;
        class const_iterator;

        iterator find(const std::string& datasetName);

        const_iterator find(const std::string& datasetName) const;

        iterator begin();

        iterator end();

        const_iterator cbegin() const;

        const_iterator cend() const;

        iterator lower_bound(const std::string& lb);

        const_iterator lower_bound(const std::string& lb) const;

        iterator upper_bound(const std::string& ub);

        const_iterator upper_bound(const std::string& ub) const;

        DataSet createDataSet(const std::string& name);

    private:

        class Impl;
        std::unique_ptr<Impl> m_impl;

        bool load(uint8_t level, const std::string& containerName,
                  const std::string& objectName, std::vector<char>& data) const;

        bool store(uint8_t level, const std::string& containerName,
                   const std::string& objectName, const std::vector<char>& data);

        size_t nextKeys(uint8_t level, const std::string& containerName,
                    const std::string& lower,
                    std::vector<std::string>& keys, size_t maxKeys) const;
};

class DataStore::const_iterator {

    friend class DataStore::Impl;
    friend class DataStore;

    protected:

        class Impl;
        std::unique_ptr<Impl> m_impl;

        const_iterator(DataStore& ds);
        const_iterator(DataStore& ds, const DataSet& current);

    public:

        typedef const_iterator self_type;
        typedef DataSet value_type;
        typedef DataSet& reference;
        typedef DataSet* pointer;
        typedef int difference_type;
        typedef std::forward_iterator_tag iterator_category;

        virtual ~const_iterator();
        const_iterator(const const_iterator&);
        const_iterator(const_iterator&&);
        const_iterator& operator=(const const_iterator&);
        const_iterator& operator=(const_iterator&&);

        self_type operator++();
        self_type operator++(int);
        const reference operator*();
        const pointer operator->();
        bool operator==(const self_type& rhs) const;
        bool operator!=(const self_type& rhs) const;
};

class DataStore::iterator : public DataStore::const_iterator {

        friend class DataStore::Impl;
        friend class DataStore;

    private:

        iterator(DataStore& ds);
        iterator(DataStore& ds, const DataSet& current);

    public:

        typedef iterator self_type;
        typedef DataSet value_type;
        typedef DataSet& reference;
        typedef DataSet* pointer;
        typedef int difference_type;
        typedef std::forward_iterator_tag iterator_category;

        ~iterator();
        iterator(const iterator&);
        iterator(iterator&&);
        iterator& operator=(const iterator&);
        iterator& operator=(iterator&&);

        reference operator*();
        pointer operator->();
};

}

#endif
