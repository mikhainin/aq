
#include "schemanode.h"

namespace avro {



SchemaNode::SchemaNode(const std::string& typeName, const std::string &itemName)
    : typeName(typeName),
      itemName(itemName) {
}

SchemaNode::~SchemaNode() {
}

const std::string &SchemaNode::getTypeName() const {
    return typeName;
}

const std::string &SchemaNode::getItemName() const {
    return itemName;
}

}
