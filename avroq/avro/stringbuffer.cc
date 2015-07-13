#include <string.h>

#include "stringbuffer.h"
namespace avro {

StringBuffer::StringBuffer(const char *c, size_t length) :
    c(c),
    length(length),
    pointer(0)
{
}

const char *StringBuffer::getAndSkip(size_t len) {
    const char *result = c + pointer;
    pointer += len;
    return result;
}

size_t StringBuffer::size() const {
    return length;
}

char StringBuffer::getChar() {
    char result = *(c + pointer);
    pointer++;
    return result;
}

std::string StringBuffer::getStdString(size_t len) {
    std::string result;
    result.resize(len);

    read((void*)result.data(), len);

    return result;
}

void StringBuffer::read(void *to, size_t len) {
    memcpy(to, c + pointer, len);
    pointer += len;

}

const char *StringBuffer::data() const {
    return c;
}

}
