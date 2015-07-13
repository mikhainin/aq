#include <zlib.h>
#include <assert.h>

#include <functional>
#include <stdexcept>

#include "deflate.h"    

namespace avro {
namespace codec {


namespace {
    struct scope_exit {
        scope_exit(std::function<void()> f) : f(f ) {}
        ~scope_exit() { f(); }
    private:
        std::function<void()> f;
    };

    constexpr unsigned long long operator"" _KiB ( unsigned long long b )
    {
        return b * 1024;
    }

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
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    ret = inflateInit2(&strm, -15);

    assert(ret == Z_OK);

    size_t avail = storage.size();
    size_t offset = 0;


    scope_exit cleanup([&]{inflateEnd(&strm);}); // std::bind(inflateEnd, &strm));
    /* decompress until deflate stream ends or end of file */
    do {
        strm.avail_in = avail;

        if (strm.avail_in == 0) {
            break;
        }
        strm.next_in = (unsigned char *)(encodedData.data() + offset);
        do {

            strm.avail_out = (uInt)storage.size() - offset;
            strm.next_out = storage.data() + offset;
            ret = inflate(&strm, Z_NO_FLUSH);
            assert(ret != Z_STREAM_ERROR);  /* state not clobbered */
            switch (ret) {
            case Z_NEED_DICT:
                // ret = Z_DATA_ERROR;     /* and fall through */
            case Z_DATA_ERROR:
            case Z_MEM_ERROR:
                throw std::runtime_error("Z_MEM_ERROR/Z_DATA_ERROR/Z_NEED_DICT");
            }

        } while (strm.avail_out == 0);

        avail -= strm.avail_in;
        offset += strm.avail_in;

        if (avail < (512_KiB)) {
            storage.resize(storage.size() + 4_MiB);
        }

        /* done when inflate() says it's done */
    } while (ret != Z_STREAM_END);

    return StringBuffer((const char *)storage.data(), strm.total_out);

}


}
}