#ifndef __avroq__double__
#define __avroq__double__

#include "node.h"

namespace avro {
namespace node {

class Double : public Node {
public:
    explicit Double(int number, const std::string &name);
};

}
}
#endif /* defined(__avroq__double__) */
