#include "map.h"

namespace avro {
namespace node {


Map::Map(int number, const std::string &name) : Node(number, "map", name) {
}

void Map::setItemsType(std::unique_ptr<Node> definition) {
    itemType = std::move(definition);
}

const std::unique_ptr<Node> &Map::getItemsType() const {
    return itemType;
}

}
}