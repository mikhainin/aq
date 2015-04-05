#ifndef __avroq__record__
#define __avroq__record__

#include <vector>
#include <memory>

#include "node.h"

namespace avro {
namespace node {


class Record : public Node {
public:
    explicit Record(int number, const std::string &name);

    void addChild(std::unique_ptr<Node> &&newChild);

    const std::vector<std::unique_ptr<Node> > &getChildren() const;
private:
    std::vector<std::unique_ptr<Node> > children;
};
    


}
}
#endif /* defined(__avroq__record__) */
