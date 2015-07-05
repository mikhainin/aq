#include <limits>

#include "finished.h"
#include "limiter.h"

namespace avro {

Limiter::Limiter(int limit) : limit(limit) {
    if (limit == -1) {
        this->limit = std::numeric_limits<int>::max();
    }
}

void Limiter::documentFinished() {
    --limit;
    if (limit <= 0) {
        throw Finished();
    }
}


}