#ifndef __avroq__avro_header__
#define __avroq__avro_header__

#include <string>
#include <map>
#include <memory>

namespace avro {

namespace node {
    class Node;
}

struct header {
    std::map<std::string, std::string> metadata;
    std::unique_ptr<node::Node> schema;
    int nodesNumber = 0;
    char sync[16] = {0}; // TODO sync length to a constant
};

}
#endif