
#include "array.h"


namespace avro {
namespace node {


Array::Array(int number, const std::string &name) : Node(number, "array", name) {
}

void Array::setItemsType(std::unique_ptr<Node> definition) {
    itemType = std::move(definition);
}

const std::unique_ptr<Node> &Array::getItemsType() const {
    return itemType;
}

}
}