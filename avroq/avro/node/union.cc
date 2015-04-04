
#include "union.h"


namespace avro {
namespace node {


Union::Union(const std::string &name) : SchemaNode("union", name) {
}

void Union::addChild(std::unique_ptr<SchemaNode> definition) {
    children.emplace_back(std::move(definition));
}

const std::vector<std::unique_ptr<SchemaNode> > &Union::getChildren() const {
    return children;    
}

}
}