#ifndef __avroq__deflatedbuffer__
#define __avroq__deflatedbuffer__

#include <vector>
#include <cstdint>

#include "stringbuffer.h"

namespace avro {

class DeflatedBuffer {
public:
    DeflatedBuffer();

    void assignData(const char *compressedData, size_t length);

    char getChar();
    StringBuffer getStringBuffer(size_t length);

    inline
    void skipChar() {
        pointer++;
    }

    inline
    void skip(size_t n) {
        pointer += n;
    }

    inline
    bool eof() {
        return length < pointer;
    }

    void read(void *to, size_t len);

    size_t size();

    void startDocument();

    void resetToDocument();
    
    StringBuffer getString(size_t len);

private:
    std::vector<uint8_t> buf;
    size_t length = 0;
    size_t pointer = 0;

    size_t documentStartPointer = 0;

    void decompress(const char *compressedData, size_t length);
};

}

#endif /* defined(__avroq__deflatedbuffer__) */
