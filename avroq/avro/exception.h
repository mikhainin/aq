#ifndef __avroq__avro_exception__
#define __avroq__avro_exception__

#include <string>

namespace avro {

class PathNotFound {
public:
    PathNotFound(const std::string &path);
    const std::string &getPath() const;

private:
    std::string path;
};

}

#endif