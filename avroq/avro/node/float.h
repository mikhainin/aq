#ifndef __avroq__float__
#define __avroq__float__

#include "schemanode.h"

namespace avro {
namespace node {

class Float : public SchemaNode {
public:
    explicit Float(const std::string &name);
};

}
}
#endif /* defined(__avroq__float__) */
