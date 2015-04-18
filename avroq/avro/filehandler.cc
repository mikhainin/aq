#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "filehandler.h"


namespace avro {

FileHandle::FileHandle(const std::string &filename) {

    struct stat st;
    stat(filename.c_str(), &st);
    fileLength = st.st_size;

    fd = open(filename.c_str(), O_RDONLY);
    // TODO: check if opened
    assert(fd > 0);


}

FileHandle::~FileHandle() {
    if (fd != -1) {
        close(fd);
    }
    if (mmappedFile && mmappedFile != MAP_FAILED) {
        munmap((void*)mmappedFile, (fileLength/4096 + 1) * 4096);
        mmappedFile = nullptr;
    }
}

std::unique_ptr<StringBuffer> FileHandle::mmapFile() {

    size_t len = (fileLength/4096 + 1) * 4096;

    auto mmappedFile =
        (const char *)mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);

    if (mmappedFile == MAP_FAILED) {
        // TODO: handle this case correctyl
        // close FD, provide useful information how to avoid this error
        perror(filename.c_str());
        throw std::runtime_error("Can't mmap file " + filename);
    }

    return std::unique_ptr<StringBuffer>(new StringBuffer(mmappedFile, fileLength));

}

}