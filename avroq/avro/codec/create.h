#include <memory>

#include "codec.h"


namespace avro {

struct header;

namespace codec {


std::shared_ptr<Codec> createForHeader(const ::avro::header &header);


}
}