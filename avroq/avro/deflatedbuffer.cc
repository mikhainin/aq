#include <zlib.h>
#include <assert.h>
#include <string.h>

#include <algorithm>
#include <stdexcept>

#include "deflatedbuffer.h"
namespace avro {

DeflatedBuffer::DeflatedBuffer() {
}


void DeflatedBuffer::assignData(const StringBuffer &b) {
    c = b.data();
    length = b.size();
}

char DeflatedBuffer::getChar() {
    char result = c[pointer];
    pointer++;
    return result;
}

void DeflatedBuffer::read(void *to, size_t len) {
    memcpy(to, c + pointer, len);
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

StringBuffer DeflatedBuffer::getString(size_t len) {
    StringBuffer result((const char*)c + pointer, len);
    pointer += len;
    return result;
}

}
