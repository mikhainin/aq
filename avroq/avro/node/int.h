#ifndef __avroq__int__
#define __avroq__int__

#include "node.h"

namespace avro {
namespace node {

class Int : public Node {
public:
    explicit Int(int number, const std::string &name);
};

}
}
#endif /* defined(__avroq__int__) */
