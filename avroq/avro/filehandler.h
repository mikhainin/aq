
#ifndef __avroq__filehandler__
#define __avroq__filehandler__

#include <string>

namespace avro {
class FileHandle {
public:
    explicit FileHandle(const std::string &filename);
    FileHandle(const FileHandle &) = delete;
    FileHandle() = delete;

    ~FileHandle();
private:
    int fd = -1;
};
}

#endif /* defined(__avroq__filehandler__) */
