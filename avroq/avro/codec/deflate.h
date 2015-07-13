#ifndef __avro_codec_deflate_h_
#define __avro_codec_deflate_h_

#include "codec.h"


namespace avro {
namespace codec {

class Deflate : public Codec {
public:

    virtual ~Deflate();

    virtual StringBuffer decode(
                const StringBuffer &encodedData,
                std::vector<uint8_t> &storage);
};

}
}

#endif