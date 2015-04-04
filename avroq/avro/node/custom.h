//
//  custom.h
//  avroq
//
//  Created by Mikhail Galanin on 03/04/15.
//  Copyright (c) 2015 Mikhail Galanin. All rights reserved.
//

#ifndef __avroq__custom__
#define __avroq__custom__

#include "schemanode.h"

namespace avro {
namespace node {

class Custom : public SchemaNode {
public:
    explicit Custom(const std::string &typeName);

    void setDefinition(std::unique_ptr<SchemaNode> &&definition);
    const std::unique_ptr<SchemaNode> &getDefinition() const;
private:
    std::unique_ptr<SchemaNode> definition;
};

}
}
#endif /* defined(__avroq__custom__) */
