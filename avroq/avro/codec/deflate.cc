#include <zlib.h>
#include <assert.h>

#include <iostream>
#include <stdexcept>

#include <util/onscopeexit.h>
#include "deflate.h"    

namespace avro {
namespace codec {


namespace {
    constexpr unsigned long long operator"" _MiB ( unsigned long long b )
    {
        return b * 1024 * 1024;
    }
}

Deflate::~Deflate() {
}

StringBuffer Deflate::decode(
            const StringBuffer &encodedData,
            std::vector<uint8_t> &storage) {

    int ret;
    z_stream strm;

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.next_in = Z_NULL;
    strm.total_out = 0;
    ret = inflateInit2(&strm, -15);

    assert(ret == Z_OK);

    util::on_scope_exit cleanup([&]{inflateEnd(&strm);}); // std::bind(inflateEnd, &strm));

    /* decompress until deflate stream ends or end of file */
    strm.avail_in = encodedData.size();
    strm.next_in  = (unsigned char *)(encodedData.data());

    do {

        if (strm.total_out == storage.size()) {
            storage.resize(storage.size() + 4_MiB);
        }

        strm.avail_out = (uInt)storage.size() - strm.total_out;
        strm.next_out = storage.data() + strm.total_out;

        ret = inflate(&strm, Z_NO_FLUSH);
        assert(ret != Z_STREAM_ERROR);  /* state not clobbered */

        if (ret == Z_STREAM_END) {
            break;
        } else if (ret != Z_OK) {
            throw std::runtime_error(
                    std::string("Inflate error (Z_MEM_ERROR/Z_DATA_ERROR/Z_NEED_DICT): ") +
                    (strm.msg ? strm.msg : "")
                );
        }

    } while (ret != Z_STREAM_END);

    return StringBuffer((const char *)storage.data(), strm.total_out);

}


}
}
