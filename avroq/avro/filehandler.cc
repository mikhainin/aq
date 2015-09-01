#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <iostream>

#include "filehandler.h"


namespace avro {

FileException::FileException(const std::string &msg) : std::runtime_error(msg) {
}


FileHandle::FileHandle(const std::string &filename) :filename(filename) {

    struct stat st;
    stat(filename.c_str(), &st);
    fileLength = st.st_size;

    fd = open(filename.c_str(), O_RDONLY);

    if (fd < 0) {
        throw FileException(std::string("Can't open file '") + filename + "': " + strerror(errno));
    }


}

FileHandle::~FileHandle() {
    if (mmappedFile && (mmappedFile != MAP_FAILED)) {
        int res = munmap((void*)mmappedFile, (fileLength/4096 + 1) * 4096);
        assert(res == 0);
        mmappedFile = nullptr;
    }
    if (fd != -1) {
        close(fd);
    }
}

const std::string &FileHandle::fileName() const {
    return filename;
}

std::unique_ptr<StringBuffer> FileHandle::mmapFile() {

    size_t len = (fileLength/4096 + 1) * 4096;

    mmappedFile =
        (const char *)mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);

    if (mmappedFile == MAP_FAILED) {
        throw FileException("Can't mmap file '" + filename + "': " + strerror(errno));
    }

    return std::unique_ptr<StringBuffer>(new StringBuffer(mmappedFile, fileLength));

}

}