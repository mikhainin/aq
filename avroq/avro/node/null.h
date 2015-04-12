#ifndef __avroq__null__
#define __avroq__null__

#include "node.h"

namespace avro {
namespace node {

class Null : public Node {
public:
    explicit Null(int number, const std::string &name);
};

}
}
#endif /* defined(__avroq__null__) */
