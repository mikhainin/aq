#include <stdexcept>

#include <avro/header.h>

#include "deflate.h"
#include "null.h"

namespace avro {
namespace codec {


std::shared_ptr<Codec> createForHeader(const ::avro::header &header) {
    auto &codecName = header.metadata.at("avro.codec");

    if (codecName == "deflate") {
        return std::make_shared<Deflate>();
    } else if (codecName == "null") {
        return std::make_shared<Null>();
    }
    throw std::runtime_error("Unsupported codec: " + codecName);
}


}
}