#ifndef __HEPNOS_FILE_SUB_RUN_H
#define __HEPNOS_FILE_SUB_RUN_H

#include <string>
#include <limits>
#include <boost/filesystem.hpp>
#include <hepnos/FileProductAccessorBackend.hpp>
#include <hepnos/ProductAccessor.hpp>
#include <hepnos/FileEvent.hpp>

namespace hepnos {

namespace fs = boost::filesystem;

class FileRun;

class FileSubRun : public ProductAccessor<FileProductAccessorBackend> {

    private:

        friend class FileRun;
        friend class FileObjectIterator<FileSubRun>;

        FileSubRun(std::uint64_t subRunNumber, const std::string& dir) 
        : ProductAccessor<FileProductAccessorBackend>(dir)
        , _subRunNumber(subRunNumber)
        , _path(dir) {}

        FileSubRun()
        : ProductAccessor<FileProductAccessorBackend>("")
        , _subRunNumber(std::numeric_limits<std::uint64_t>::max())
        , _path("") {}

        std::uint64_t _subRunNumber;
        std::string   _path;

    public:

        typedef FileObjectIterator<FileEvent> iterator;

        std::uint64_t getSubRunNumber() const {
            return _subRunNumber;
        }

        bool isValid() const {
            return _subRunNumber != std::numeric_limits<std::uint64_t>::max();
        }

        FileEvent createEvent() {
            fs::directory_iterator begin(_path), end;
            size_t eventNumber = std::count_if(begin, end,
                    [](const fs::directory_entry& d) {
                    return fs::is_directory(d.path());
                    });
            std::stringstream ss;
            ss << _path << std::setfill('0') << std::setw(12) << eventNumber << "/";
            std::string dir = ss.str();
            fs::create_directory(dir);
            return FileEvent(eventNumber, dir);
        }

        FileEvent openEvent(std::uint64_t eventNumber) {
            std::stringstream ss;
            ss << _path << std::setfill('0') << std::setw(12) << eventNumber << "/";
            std::string dir = ss.str();
            if(fs::is_directory(dir))
                return FileEvent(eventNumber, dir);
            else
                return FileEvent();
        }

        std::size_t numEvents() const {
            fs::directory_iterator begin(_path), end;
            return std::count_if(begin, end,
                    [](const fs::directory_entry& d) {
                    return fs::is_directory(d.path());
                    });
        }

        iterator begin() {
            return iterator(_path, 0, numEvents());
        }

        iterator end() {
            auto n = numEvents();
            return iterator(_path, n, n);
        }
};

}
#endif
