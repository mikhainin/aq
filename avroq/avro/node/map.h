#ifndef __avroq__map__
#define __avroq__map__


#include "node.h"

namespace avro {
namespace node {

class Map : public Node {
public:
    explicit Map(int number, const std::string &name);
    void setItemsType(std::unique_ptr<Node> definition);
    const std::unique_ptr<Node> &getItemsType() const;

private:
    std::unique_ptr<Node> itemType;
};

}
}

#endif /* defined(__avroq__map__) */
