#ifndef __HEPNOS_FILE_EVENT_H
#define __HEPNOS_FILE_EVENT_H

#include <string>
#include <limits>
#include <boost/filesystem.hpp>
#include <hepnos/FileProductAccessorBackend.hpp>
#include <hepnos/FileObjectIterator.hpp>
#include <hepnos/ProductAccessor.hpp>

namespace hepnos {

namespace fs = boost::filesystem;

class FileSubRun;
class FileDataStore;

class FileEvent : public ProductAccessor<FileProductAccessorBackend> {

    private:

        friend class FileDataStore;
        friend class FileSubRun;
        friend class FileObjectIterator<FileEvent>;

        void createRefAndSetID() {
            std::size_t h = std::hash<std::string>()(_path);
            std::string linkName;
            std::stringstream ss;
            do {
                ss.str("");
                ss << _path << "../../../../.ref/";
                ss << std::setfill('0') << std::setw(20) << h;
                h += 1;
            } while(fs::exists(ss.str()));
            _eventID = h-1;
            fs::create_symlink(_path, ss.str());
        }

        FileEvent(std::uint64_t eventNumber, const std::string& dir) 
        : ProductAccessor<FileProductAccessorBackend>(dir)
        , _eventNumber(eventNumber)
        , _path(dir) {
            createRefAndSetID();
        }

        FileEvent(const std::string& dir)
        : ProductAccessor<FileProductAccessorBackend>(dir)
        , _eventNumber(0)
        , _path(dir) {
            if(_path.back() != '/') _path += std::string("/");
            std::size_t i,j;
            j = _path.size()-1;
            if(_path[j] == '/') j--;
            i = j;
            while(_path[i] != '/') i--;
            i += 1;
            while(_path[i] == '0') i++;
            j += 1;
            std::string eventDir(&dir[i], j-i);
            if(eventDir.size() > 0)
                _eventNumber = std::stoi(eventDir);
        }

        FileEvent()
        : ProductAccessor<FileProductAccessorBackend>("")
        , _eventNumber(std::numeric_limits<std::uint64_t>::max())
        , _path("") {}

        std::uint64_t _eventNumber;
        std::string   _path;
        std::size_t   _eventID;

    public:

        std::uint64_t getEventNumber() const {
            return _eventNumber;
        }

        std::size_t getEventID() const {
            return _eventID;
        }

        bool isValid() const {
            return _eventNumber != std::numeric_limits<std::uint64_t>::max();
        }
};

}
#endif
