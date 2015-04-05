#ifndef __avroq__boolean__
#define __avroq__boolean__

#include "node.h"

namespace avro {
namespace node {

class Boolean : public Node {
public:
    explicit Boolean(int number, const std::string &name);
};

}
}
#endif /* defined(__avroq__boolean__) */
