#ifndef __avroq__float__
#define __avroq__float__

#include "node.h"

namespace avro {
namespace node {

class Float : public Node {
public:
    explicit Float(int number, const std::string &name);
};

}
}
#endif /* defined(__avroq__float__) */
