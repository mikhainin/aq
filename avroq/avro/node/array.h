
#ifndef __avroq__array__
#define __avroq__array__

#include "node.h"

namespace avro {
namespace node {

class Array : public Node {
public:
    explicit Array(int number, const std::string &name);
    void setItemsType(std::unique_ptr<Node> definition);
    const std::unique_ptr<Node> &getItemsType() const;

private:
    std::unique_ptr<Node> itemType;
};

}
}
#endif /* defined(__avroq__array__) */
