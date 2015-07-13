
#ifndef __avroq__stringbuffer__
#define __avroq__stringbuffer__

#include <cstddef>
#include <cstring>
#include <ostream>
#include <string>
#include <algorithm>

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

    size_t bytesLeft() const {
        return length - pointer;
    }

    inline
    std::ostream& write(std::ostream& os) const
    {
        os.write(c, length);
        return os;
    }

    inline
    bool operator == (const std::string &s) const {
        if (s.size() != bytesLeft()) {
            return false;
        }
        return std::strncmp(s.data(), c + pointer, std::min<size_t>(s.size(), bytesLeft())) == 0;
    }

    inline
    bool operator != (const std::string &s) const {
        return !(*this == s);
    }

    inline
    bool contains (const std::string &s) const {
        const char *last = c + bytesLeft();
        auto p = std::search(
            c + pointer, last,
            s.data(), s.data() + s.size()
        );
        return p != last;
    }

    inline
    bool starts_with (const std::string &s) const {
        if (s.size() > bytesLeft()) {
            return false;
        }

        return std::strncmp(s.data(), c + pointer, s.size()) == 0;
    }

    inline
    bool ends_with (const std::string &s) const {
        if (s.size() > bytesLeft()) {
            return false;
        }
        size_t lastNthByte = bytesLeft() - s.size();
        return std::strncmp(s.data(), c + pointer + lastNthByte, s.size()) == 0;
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
