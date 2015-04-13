//
//  long.h
//  avroq
//
//  Created by Mikhail Galanin on 13/04/15.
//  Copyright (c) 2015 Mikhail Galanin. All rights reserved.
//

#ifndef __avroq__long__
#define __avroq__long__


#include "node.h"

namespace avro {
namespace node {

class Long : public Node {
public:
    explicit Long(int number, const std::string &name);
};

}
}
#endif /* defined(__avroq__long__) */
