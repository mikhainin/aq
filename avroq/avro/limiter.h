#ifndef __avroq__limiter__
#define __avroq__limiter__

#include <atomic>

namespace avro {

class Limiter {
public:
    explicit Limiter(int limit);
    void documentFinished();
    bool finished() const;
private:
    std::atomic_int limit;
};

}
#endif /* defined(__avroq__limiter__) */
