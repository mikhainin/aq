#ifndef __avro_codec_null_h_
#define __avro_codec_null_h_

#include "codec.h"


namespace avro {
namespace codec {

class Null : public Codec {
public:

    virtual ~Null();

    virtual StringBuffer decode(
                const StringBuffer &encodedData,
                std::vector<uint8_t> &storage);
};

}
}

#endif