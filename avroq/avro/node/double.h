#ifndef __avroq__double__
#define __avroq__double__

#include "schemanode.h"

namespace avro {
namespace node {

class Double : public SchemaNode {
public:
    explicit Double(const std::string &name);
};

}
}
#endif /* defined(__avroq__double__) */
