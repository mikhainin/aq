
#ifndef __avroq__string__
#define __avroq__string__

#include "node.h"

namespace avro {
namespace node {

class String : public Node {
public:
    explicit String(int number, const std::string &name);
};
}
}

#endif /* defined(__avroq__string__) */
