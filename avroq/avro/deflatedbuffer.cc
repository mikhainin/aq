#include <zlib.h>
#include <assert.h>
#include <string.h>

#include <algorithm>
#include <stdexcept>

#include "deflatedbuffer.h"
namespace avro {

DeflatedBuffer::DeflatedBuffer() {
    buf.resize(5 * 1024 * 1024);
}

void DeflatedBuffer::assignData(const char *compressedData, size_t compressedDataLength) {
    length = 0;
    pointer = 0;
    documentStartPointer = 0;

    decompress(compressedData, compressedDataLength);
}

void DeflatedBuffer::decompress(const char *compressedData, size_t compressedDataLength) {

    int ret;
    z_stream strm;

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    // strm.zalloc = malloc;
    // strm.zfree = free;
    ret = inflateInit2(&strm, -15);
    assert(ret == Z_OK);

    size_t avail = buf.size();
    size_t offset = 0;

    /* decompress until deflate stream ends or end of file */
    do {
        strm.avail_in = avail;

        if (strm.avail_in == 0)
            break;
        strm.next_in = (unsigned char *)(compressedData + offset);
        /* run inflate() on input until output buffer not full */
        do {

            strm.avail_out = (uInt)buf.size();
            strm.next_out = buf.data();
            ret = inflate(&strm, Z_NO_FLUSH);
            assert(ret != Z_STREAM_ERROR);  /* state not clobbered */
            switch (ret) {
            case Z_NEED_DICT:
                // ret = Z_DATA_ERROR;     /* and fall through */
            case Z_DATA_ERROR:
            case Z_MEM_ERROR:
                (void)inflateEnd(&strm);
                throw std::runtime_error("Z_MEM_ERROR/Z_DATA_ERROR/Z_NEED_DICT");
            }

            // have = CHUNK - strm.avail_out;
            /*
            if (fwrite(out, 1, have, dest) != have || ferror(dest)) {
                (void)inflateEnd(&strm);
                throw std::runtime_error("Z_ERRNO");
            }
            */
        } while (strm.avail_out == 0);

        avail -= strm.avail_in;
        offset += strm.avail_in;

        /* done when inflate() says it's done */
    } while (ret != Z_STREAM_END);

    length = strm.total_out;
}

char DeflatedBuffer::getChar() {
    char result = buf[pointer];
    pointer++;
    return result;
}

void DeflatedBuffer::read(void *to, size_t len) {
    memcpy(to, buf.data() + pointer, len);
    pointer += len;
}

size_t DeflatedBuffer::size() {
    return length;
}

void DeflatedBuffer::startDocument() {
    documentStartPointer = pointer;
}

void DeflatedBuffer::resetToDocument() {
    pointer = documentStartPointer;
}

std::string DeflatedBuffer::getStdString(size_t len) {
    std::string result;
    result.resize(len);

    read((void*)result.data(), len);

    return result;
    
}

}
