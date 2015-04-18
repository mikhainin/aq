
#ifndef __avroq__filehandler__
#define __avroq__filehandler__

#include <string>
#include <memory>

#include "stringbuffer.h"

namespace avro {

class FileHandle {
public:
    explicit FileHandle(const std::string &filename);
    FileHandle(const FileHandle &) = delete;
    FileHandle() = delete;

    ~FileHandle();

    std::unique_ptr<StringBuffer> mmapFile();
private:
    int fd = -1;
    int fileLength = 0;
    std::string filename;
    const char *mmappedFile = nullptr;
};

}

#endif /* defined(__avroq__filehandler__) */
