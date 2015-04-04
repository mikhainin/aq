
#ifndef __avroq__string__
#define __avroq__string__

#include "schemanode.h"

namespace avro {
namespace node {

class String : public SchemaNode {
public:
    explicit String(const std::string &name);
};
}
}

#endif /* defined(__avroq__string__) */
