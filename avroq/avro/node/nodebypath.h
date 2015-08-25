#ifndef __avroq__node_nodebypath__
#define __avroq__node_nodebypath__

#include <string>

namespace avro {
namespace node {

class Node;

const Node* nodeByPath(const std::string &path, const Node* rootNode);

// use tags here
const Node* nodeByPathIgnoreArray(const std::string &path, const Node* rootNode);

}
}

#endif