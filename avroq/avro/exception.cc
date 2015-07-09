
#include "exception.h"

namespace avro {

PathNotFound::PathNotFound(const std::string &path)
    : path(path) {
}

const std::string &PathNotFound::getPath() const {
    return path;
}

}