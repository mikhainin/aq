#ifndef __avro_codec_codec_h_
#define __avro_codec_codec_h_

#include <string>
#include <vector>

#include <avro/stringbuffer.h>

namespace avro {
namespace codec {

class Codec {
public:

    virtual ~Codec();

    virtual StringBuffer decode(
                const StringBuffer &encodedData,
                std::vector<uint8_t> &storage) = 0;

};

}
}

#endif