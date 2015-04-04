#ifndef __avroq__record__
#define __avroq__record__

#include <vector>
#include <memory>

#include "schemanode.h"

namespace avro {
namespace node {


class Record : public SchemaNode {
public:
    explicit Record(const std::string &name);

    void addChild(std::unique_ptr<SchemaNode> &&newChild);

    const std::vector<std::unique_ptr<SchemaNode> > &getChildren() const;
private:
    std::vector<std::unique_ptr<SchemaNode> > children;
};
    


}
}
#endif /* defined(__avroq__record__) */
