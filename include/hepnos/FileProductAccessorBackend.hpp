#ifndef __HEPNOS_FILE_PRODUCT_ACCESSOR_BACKEND_H
#define __HEPNOS_FILE_PRODUCT_ACCESSOR_BACKEND_H

#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <hepnos/InputTag.hpp>

namespace hepnos {

class FileProductAccessorBackend {
    
    private:

        std::string makeFileNameFor(const std::string& objName,
                                    const InputTag& tag) {
            std::string dot(".");
            std::string fileName = 
                objName + dot + tag.moduleLabel + dot
                + tag.instanceName + dot + tag.processName;
            if(_path.empty()) return fileName;
            else if(_path.back() == '/') return _path + fileName;
            else return _path + std::string("/") + fileName;
        }

        std::string _path;

    public:

        FileProductAccessorBackend(const std::string& path)
        : _path(path) {}

        void store(const std::string& objName,
                   const InputTag& tag,
                   const std::vector<char>& data) {
            std::string fileName = makeFileNameFor(objName, tag);
            std::ofstream outfile;
            outfile.open(fileName, std::ofstream::out | std::ofstream::trunc);
            outfile.write(data.data(), data.size());
            outfile.close();
        }

        bool load(const std::string& objName,
                  const InputTag& tag,
                  std::vector<char>& data) {
            std::string fileName = makeFileNameFor(objName, tag);
            std::ifstream infile;
            infile.open(fileName, std::ifstream::ate | std::ifstream::binary);
            if(!infile.good()) return false;
            std::size_t size = infile.tellg();
            infile.seekg(0, infile.beg);
            data.resize(size);
            infile.read(data.data(), size);
            infile.close();
            return true;
        }

        ~FileProductAccessorBackend() {}

};

}
#endif
