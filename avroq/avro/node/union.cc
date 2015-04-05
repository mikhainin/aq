
#include "union.h"


namespace avro {
namespace node {


Union::Union(int number, const std::string &name) : Node(number, "union", name) {
}

void Union::addChild(std::unique_ptr<Node> definition) {
    children.emplace_back(std::move(definition));
}

const std::vector<std::unique_ptr<Node> > &Union::getChildren() const {
    return children;    
}

}
}