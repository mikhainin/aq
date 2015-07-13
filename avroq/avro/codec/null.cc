#include <zlib.h>
#include <assert.h>

#include <functional>
#include <stdexcept>

#include "null.h"

namespace avro {
namespace codec {


Null::~Null() {
}

StringBuffer Null::decode(
            const StringBuffer &encodedData,
            std::vector<uint8_t> &storage) {

    return encodedData;

}


}
}