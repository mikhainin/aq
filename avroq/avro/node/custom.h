//
//  custom.h
//  avroq
//
//  Created by Mikhail Galanin on 03/04/15.
//  Copyright (c) 2015 Mikhail Galanin. All rights reserved.
//

#ifndef __avroq__custom__
#define __avroq__custom__

#include "node.h"

namespace avro {
namespace node {

class Custom : public Node {
public:
    explicit Custom(int number, const std::string &typeName);

    void setDefinition(std::unique_ptr<Node> &&definition);
    const std::unique_ptr<Node> &getDefinition() const;
private:
    std::unique_ptr<Node> definition;
};

}
}
#endif /* defined(__avroq__custom__) */
