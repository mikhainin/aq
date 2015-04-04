
#include "record.h"


namespace avro {
namespace node {


Record::Record(const std::string &name)
: SchemaNode("record", name) {
}

void Record::addChild(std::unique_ptr<SchemaNode> &&newChild) {
    children.push_back(std::move(newChild));
}

const std::vector<std::unique_ptr<SchemaNode> > &Record::getChildren() const {
    return children;
}

}
}
