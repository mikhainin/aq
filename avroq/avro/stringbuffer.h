
#ifndef __avroq__stringbuffer__
#define __avroq__stringbuffer__

#include <cstddef>

namespace avro {

class StringBuffer {
public:
    explicit StringBuffer();

private:
    const char *const c = nullptr;
    size_t length = 0;
};

}

#endif /* defined(__avroq__stringbuffer__) */
