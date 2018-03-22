#ifndef __HEPNOS_FILE_OBJECT_ITERATOR_H
#define __HEPNOS_FILE_OBJECT_ITERATOR_H

#include <boost/filesystem.hpp>

namespace hepnos {

namespace fs = boost::filesystem;

template<typename T>
class FileObjectIterator {

    private:

        std::string   _path;
        std::uint64_t _numObjects;
        std::uint64_t _currentIdx;
        T             _current;

    public:

        FileObjectIterator(const std::string& path, std::uint64_t idx, std::uint64_t nobj)
        : _path(path), _numObjects(nobj), _currentIdx(idx) {
            if(_path.back() != '/') _path += std::string("/");
            if(_currentIdx != _numObjects) {
                std::stringstream ss;
                ss << _path << std::setfill('0') << std::setw(12) << _currentIdx << "/";
                _current = T(_currentIdx, ss.str());
            }
        }

        bool operator==(const FileObjectIterator& other) const {
            return _path == other._path
                && _currentIdx == other._currentIdx
                && _numObjects == other._numObjects;
        }

        bool operator!=(const FileObjectIterator& other) const {
            return !(*this == other);
        }

        const T& operator*() const {
            return _current;
        }

        const T* operator->() const {
            return &_current;
        }

        T* operator->() {
            return &_current;
        }

        FileObjectIterator operator++() {
            FileObjectIterator old = *this;
            _currentIdx += 1;
            if(_currentIdx != _numObjects) {
                std::stringstream ss;
                ss << _path << std::setfill('0') << std::setw(12) << _currentIdx << "/";
                _current = T(_currentIdx, ss.str());
            }
            return old;
        }

        FileObjectIterator& operator++(int) {
            _currentIdx += 1;
            if(_currentIdx != _numObjects) {
                std::stringstream ss;
                ss << _path << std::setfill('0') << std::setw(12) <<  _currentIdx << "/";
                _current = T(_currentIdx, ss.str());
            }
            return *this;
        }
};

}

#endif
