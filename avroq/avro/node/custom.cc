
#include "custom.h"

namespace avro {
namespace node {

Custom::Custom(const std::string &typeName)
    : SchemaNode(typeName, "custom_type_name") { // TODO: check it it's correct
}

void Custom::setDefinition(std::unique_ptr<SchemaNode> && definition) {
    this->definition = std::move(definition);
}

const std::unique_ptr<SchemaNode> &Custom::getDefinition() const {
    return definition;
}


}
}