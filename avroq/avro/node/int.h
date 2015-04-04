#ifndef __avroq__int__
#define __avroq__int__

#include "schemanode.h"

namespace avro {
namespace node {

class Int : public SchemaNode {
public:
    explicit Int(const std::string &name);
};

}
}
#endif /* defined(__avroq__int__) */
