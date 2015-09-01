#include <cstring>

#include "header.h"

namespace avro {

bool header::operator != (const header &b) const {
    return metadata != b.metadata;
}

bool header::operator == (const header &b) const {
    return metadata == b.metadata;
}


void header::setSync(const sync_t &sync) {
    memcpy(this->sync, sync, sizeof sync);
}

}
