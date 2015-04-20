
#ifndef __avroq__stringbuffer__
#define __avroq__stringbuffer__

#include <cstddef>
#include <ostream>
#include <string>

namespace avro {

class StringBuffer {
public:
    explicit StringBuffer(const char *c, size_t length);

    const char *getAndSkip(size_t len);
    size_t size() const;
    char getChar();
    std::string getStdString(size_t len);
    void read(void *to, size_t len);

    inline
    bool eof() {
        return length <= pointer;
    }

    inline
    std::ostream& write(std::ostream& os) const
    {
        os.write(c, length);
        return os;
    }

private:
    const char *c = nullptr;
    size_t length = 0;
    size_t pointer = 0;
};

inline
std::ostream& operator<<(std::ostream& os, const StringBuffer& s)
{
    return s.write(os);
}

}

#endif /* defined(__avroq__stringbuffer__) */
