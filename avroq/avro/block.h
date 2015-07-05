#pragma once

#include "deflatedbuffer.h"

namespace avro {

struct Block {
    DeflatedBuffer buffer;
    int64_t objectCount = 0;
};

}