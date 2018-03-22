#ifndef __HEPNOS_FILE_DATASTORE_H
#define __HEPNOS_FILE_DATASTORE_H

#include <string>
#include <iterator>
#include <boost/filesystem.hpp>
#include <hepnos/FileNamespace.hpp>

namespace hepnos {

namespace fs = boost::filesystem;

class FileDataStore {

public:

    typedef FileNamespace namespace_type;
    typedef FileRun       run_type;
    typedef FileSubRun    subrun_type;
    typedef FileEvent     event_type;

    FileDataStore(const std::string path)
    : _path(path) {
        if(_path.empty()) _path = "./";
        if(_path.back() != '/') _path += std::string("/");
        fs::create_directories(_path);
        fs::create_directory(_path+std::string(".ref"));
    }

    namespace_type createNamespace(const std::string& name) {
        std::string dir = _path + name;
        if(fs::is_directory(dir)) 
            return FileNamespace(name, dir);
        fs::create_directory(dir);
        return FileNamespace(name, dir);
    }

    namespace_type openNamespace(const std::string& name) {
        std::string dir = _path + name;
        if(fs::is_directory(dir))
            return FileNamespace(name, dir);
        else
            return FileNamespace();
    }

private:

    std::string _path;
};

}

#endif
