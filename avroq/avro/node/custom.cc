
#include "custom.h"

namespace avro {
namespace node {

Custom::Custom(int number, const std::string &typeName)
    : Node(number, typeName, "custom_type_name") { // TODO: check it it's correct
}

void Custom::setDefinition(std::unique_ptr<Node> && definition) {
    this->definition = std::move(definition);
}

const std::unique_ptr<Node> &Custom::getDefinition() const {
    return definition;
}


}
}