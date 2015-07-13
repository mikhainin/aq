
#include "node.h"

namespace avro {
namespace node {


Node::Node(int number, const std::string& typeName, const std::string &itemName)
    : number(number),
      typeName(typeName),
      itemName(itemName) {
}

Node::~Node() {
}

const std::string &Node::getTypeName() const {
    return typeName;
}

const std::string &Node::getItemName() const {
    return itemName;
}

int Node::getNumber() const {
    return number;
}

}
}