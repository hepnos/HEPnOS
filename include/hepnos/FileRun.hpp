#ifndef __HEPNOS_FILE_RUN_H
#define __HEPNOS_FILE_RUN_H

#include <string>
#include <limits>
#include <boost/filesystem.hpp>
#include <hepnos/FileProductAccessorBackend.hpp>
#include <hepnos/ProductAccessor.hpp>
#include <hepnos/FileSubRun.hpp>
#include <hepnos/FileObjectIterator.hpp>

namespace hepnos {

namespace fs = boost::filesystem;

class FileNamespace;
template<typename T> class FileObjectIterator;

class FileRun : public ProductAccessor<FileProductAccessorBackend> {

    private:

        friend class FileNamespace;
        friend class FileObjectIterator<FileRun>;

        FileRun(std::uint64_t runNumber, const std::string& dir) 
        : ProductAccessor<FileProductAccessorBackend>(dir)
        , _runNumber(runNumber)
        , _path(dir) {}

        FileRun(const std::string& dir)
        : ProductAccessor<FileProductAccessorBackend>(dir)
        , _runNumber(0)
        , _path(dir) {
            std::size_t i,j;
            j = dir.size()-1;
            if(dir[j] == '/') j--;
            i = j;
            while(dir[i] != '/') i--;
            i += 1;
            while(dir[i] == '0') i++;
            j += 1;
            std::string runDir(&dir[i], j-i);
            if(runDir.size() > 0)
                _runNumber = std::stoi(runDir);
        }

        FileRun() 
        : ProductAccessor<FileProductAccessorBackend>("")
        , _runNumber(std::numeric_limits<std::uint64_t>::max())
        , _path("") {}

        std::uint64_t _runNumber;
        std::string _path;

    public:

        typedef FileObjectIterator<FileSubRun> iterator;

        std::uint64_t getRunNumber() const {
            return _runNumber;
        }

        bool isValid() const {
            return _runNumber != std::numeric_limits<std::uint64_t>::max();
        }

        FileSubRun createSubRun() {
            fs::directory_iterator begin(_path), end;
            size_t subRunNumber = std::count_if(begin, end,
                    [](const fs::directory_entry& d) {
                    return fs::is_directory(d.path());
                    });
            std::stringstream ss;
            ss << _path << std::setfill('0') << std::setw(12) << subRunNumber << "/";
            std::string dir = ss.str();
            fs::create_directory(dir);
            return FileSubRun(subRunNumber, dir);
        }

        FileSubRun openSubRun(std::uint64_t subRunNumber) {
            std::stringstream ss;
            ss << _path << std::setfill('0') << std::setw(12) << subRunNumber << "/";
            std::string dir = ss.str();
            if(fs::is_directory(dir))
                return FileSubRun(subRunNumber, dir);
            else
                return FileSubRun();
        }

        std::size_t numSubRuns() const {
            fs::directory_iterator begin(_path), end;
            return std::count_if(begin, end,
                    [](const fs::directory_entry& d) {
                    return fs::is_directory(d.path());
                    });
        }

        iterator begin() {
            return iterator(_path,0,numSubRuns());
        }

        iterator end() {
            auto n = numSubRuns();
            return iterator(_path,n,n);
        }
};

}
#endif
