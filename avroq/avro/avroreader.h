//
//  avroreader.h
//  avroq
//
//  Created by Mikhail Galanin on 12/12/14.
//  Copyright (c) 2014 Mikhail Galanin. All rights reserved.
//

#ifndef __avroq__avroreader__
#define __avroq__avroreader__

#include <string>

class AvroReader {
public:
    void readFile(const std::string & filename);
private:
    int64_t readLong(std::ifstream &input);
    std::string readString(std::ifstream &input);
};

#endif /* defined(__avroq__avroreader__) */
