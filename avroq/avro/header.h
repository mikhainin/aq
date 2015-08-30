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
    using sync_t = char[16];
    std::map<std::string, std::string> metadata;
    std::unique_ptr<node::Node> schema;
    int nodesNumber = 0;
    sync_t sync = {0}; // TODO sync length to a constant

    bool operator != (const header &b) const;
    bool operator == (const header &b) const;

    void setSync(const sync_t &sync); // TODO: it should be done automatically
};

}
#endif