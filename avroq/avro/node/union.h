
#ifndef __avroq__union__
#define __avroq__union__

#include <vector>
#include "schemanode.h"

namespace avro {
namespace node {

class Union : public SchemaNode {
public:
    explicit Union(const std::string &name);
    void addChild(std::unique_ptr<SchemaNode> definition);
    const std::vector<std::unique_ptr<SchemaNode> > &getChildren() const;

private:
    std::vector<std::unique_ptr<SchemaNode>> children;
};

}
}

#endif /* defined(__avroq__union__) */
