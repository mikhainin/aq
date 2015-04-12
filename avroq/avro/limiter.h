//
//  limiter.h
//  avroq
//
//  Created by Mikhail Galanin on 12/04/15.
//  Copyright (c) 2015 Mikhail Galanin. All rights reserved.
//

#ifndef __avroq__limiter__
#define __avroq__limiter__

namespace avro {

class Limiter {
public:
    explicit Limiter(int limit);
    void documentFinished();
private:
    int limit;
};

}
#endif /* defined(__avroq__limiter__) */
