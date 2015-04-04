#ifndef __avroq__boolean__
#define __avroq__boolean__

#include "schemanode.h"

namespace avro {
namespace node {

class Boolean : public SchemaNode {
public:
    explicit Boolean(const std::string &name);
};

}
}
#endif /* defined(__avroq__boolean__) */
