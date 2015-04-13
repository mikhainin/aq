
#ifndef __avroq__stringbuffer__
#define __avroq__stringbuffer__

#include <cstddef>
#include <string>

namespace avro {

class StringBuffer {
public:
    explicit StringBuffer(const char *c, size_t length);

    const char *getAndSkip(size_t len);
    char size() const;
    char getChar();
    std::string getStdString(size_t len);
    void read(void *to, size_t len);

    inline
    bool eof() {
        return length <= pointer;
    }

private:
    const char *c = nullptr;
    size_t length = 0;
    size_t pointer = 0;
};

}

#endif /* defined(__avroq__stringbuffer__) */
