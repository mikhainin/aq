
#include "record.h"


namespace avro {
namespace node {


Record::Record(int number, const std::string &name)
: Node(number, "record", name) {
}

void Record::addChild(std::unique_ptr<Node> &&newChild) {
    children.push_back(std::move(newChild));
}

const std::vector<std::unique_ptr<Node> > &Record::getChildren() const {
    return children;
}

}
}
