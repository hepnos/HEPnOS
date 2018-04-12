#ifndef __HEPNOS_DATA_SET_H
#define __HEPNOS_DATA_SET_H

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <hepnos/Exception.hpp>
#include <hepnos/DataStore.hpp>

namespace hepnos {

class DataSet {

    friend class DataStore;

    private:

        DataSet();

        DataSet(DataStore& ds, uint8_t level, const std::string& fullname);

        DataSet(DataStore& ds, uint8_t level, const std::string& container, const std::string& name);

        bool storeBuffer(const std::string& key, const std::vector<char>& buffer);

        bool loadBuffer(const std::string& key, std::vector<char>& buffer) const;

        DataStore*   m_datastore;
        uint8_t      m_level;
        std::string  m_container;
        std::string  m_name;

    public:

        const std::string& name() const {
            return m_name;
        }

        const std::string container() const {
            return m_container;
        }

        std::string fullname() const {
            std::stringstream ss;
            if(m_container.size() != 0)
                ss << m_container << "/";
            ss << m_name;
            return ss.str();
        }

        DataSet next() const;

        bool valid() const;

        template<typename K, typename V>
        bool store(const K& key, const V& value) {
            std::stringstream ss_key, ss_value;
            ss_key << key;
            boost::archive::binary_oarchive oa(ss_value);
            try {
                oa << value;
            } catch(...) {
                throw Exception("Exception occured during serialization");
            }
            std::string serialized = ss_value.str();
            std::vector<char> buffer(serialized.begin(), serialized.end());
            return storeBuffer(ss_key.str(), buffer);
        }

        template<typename K, typename V>
        bool load(const K& key, V& value) const {
            std::stringstream ss_key;
            ss_key << key;
            std::vector<char> buffer;
            if(!loadBuffer(key, buffer)) {
                return false;
            }
            try {
                std::string serialized(buffer.begin(), buffer.end());
                std::stringstream ss(serialized);
                boost::archive::binary_iarchive ia(ss);
                ia >> value;
            } catch(...) {
                throw Exception("Exception occured during serialization");
            }
            return true;
        }

        bool operator==(const DataSet& other) const;
};

}

#endif
