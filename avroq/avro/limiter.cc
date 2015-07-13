#include <limits>
#include <thread>
#include <iostream>

#include "finished.h"
#include "limiter.h"

namespace avro {

Limiter::Limiter(int limit) : limit(limit) {
    if (limit == -1) {
        this->limit = std::numeric_limits<int>::max();
    }
}

void Limiter::documentFinished() {
    if (limit.fetch_sub(1) <= 0) {
        throw Finished();
    }
}

bool Limiter::finished() const {
    return limit <= 0;
}


}