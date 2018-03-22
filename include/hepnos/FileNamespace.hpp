#ifndef __HEPNOS_FILE_NAMESPACE_H
#define __HEPNOS_FILE_NAMESPACE_H

#include <string>
#include <iomanip>
#include <boost/filesystem.hpp>
#include <hepnos/FileObjectIterator.hpp>
#include <hepnos/FileRun.hpp>

namespace hepnos {

namespace fs = boost::filesystem;

class FileDataStore;

class FileNamespace {

    friend class FileDataStore;

    private:

        std::string _name;
        std::string _path;

        FileNamespace(const std::string& name, const std::string& dir)
        : _name(name), _path(dir+std::string("/")) {}

        FileNamespace() {}

    public:

        typedef FileObjectIterator<FileRun> iterator;

        const std::string& getName() const {
            return _name;
        }

        bool isValid() const {
            return !_name.empty();
        }

        FileRun createRun() {
            fs::directory_iterator begin(_path), end;
            size_t runNumber = std::count_if(begin, end,
                    [](const fs::directory_entry& d) {
                        return fs::is_directory(d.path());
                    });
            std::stringstream ss;
            ss << _path << std::setfill('0') << std::setw(12) << runNumber << "/";
            std::string dir = ss.str();
            fs::create_directory(dir);
            return FileRun(runNumber, dir);
        }

        FileRun openRun(std::uint64_t runNumber) {
            std::stringstream ss;
            ss << _path << std::setfill('0') << std::setw(12) << runNumber << "/";
            std::string dir = ss.str();
            if(fs::is_directory(dir))
                return FileRun(runNumber, dir);
            else
                return FileRun();
        }

        std::size_t numRuns() const {
            fs::directory_iterator begin(_path), end;
            return std::count_if(begin, end,
                    [](const fs::directory_entry& d) {
                        return fs::is_directory(d.path());
                    });
        }

        iterator begin() {
            return iterator(_path,0,numRuns());
        }

        iterator end() {
            auto n = numRuns();
            return iterator(_path,n,n);
        }
};

}
#endif
