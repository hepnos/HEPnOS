#ifndef __HEPNOS_RUN_H
#define __HEPNOS_RUN_H

#include <memory>
#include <string>
#include <hepnos/DataStore.hpp>
#include <hepnos/RunNumber.hpp>

namespace hepnos {

class RunSet;

class Run {

    private:

    friend class RunSet;
    friend class DataSet;

    class Impl;
    std::unique_ptr<Impl> m_impl;

    Run(DataStore* datastore, uint8_t level, const std::string& container, const RunNumber& run);

    public:

    Run();

    Run(const Run&);

    Run(Run&&);

    Run& operator=(const Run&);

    Run& operator=(Run&&);

    ~Run();

    Run next() const;

    bool valid() const;

    bool storeRawData(const std::string& key, const std::vector<char>& buffer);

    bool loadRawData(const std::string& key, std::vector<char>& buffer) const;

    bool operator==(const Run& other) const;

    bool operator!=(const Run& other) const;

    const RunNumber& number() const;

    const std::string& container() const;
};

}

#endif
