
#ifndef __avroq__union__
#define __avroq__union__

#include <vector>
#include "node.h"

namespace avro {
namespace node {

class Union : public Node {
public:
    explicit Union(int number, const std::string &name);
    void addChild(std::unique_ptr<Node> definition);
    const std::vector<std::unique_ptr<Node> > &getChildren() const;

private:
    std::vector<std::unique_ptr<Node>> children;
};

}
}

#endif /* defined(__avroq__union__) */
