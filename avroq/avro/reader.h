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
#include <cstdint>
#include <map>

namespace avro {

struct header {
    std::map<std::string, std::string> metadata;
    char sync[16] = {0}; // TODO sync length to a constant
};

class Reader {
public:

    class Eof {};

    Reader(const std::string & filename);
    ~Reader();

    header readHeader();

    void readBlock(const header &header);

    bool eof();
private:

    class Private;
    Private *d = nullptr;

    int64_t readLong();
    std::string readString();
};

}

#endif /* defined(__avroq__avroreader__) */
