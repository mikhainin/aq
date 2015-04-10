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

#include <boost/iostreams/filtering_stream.hpp>

namespace avro {

class Node;
class DeflatedBuffer;

struct header {
    std::map<std::string, std::string> metadata;
    std::unique_ptr<Node> schema;
    int nodesNumber = 0;
    char sync[16] = {0}; // TODO sync length to a constant
};

struct FilterExpression {
    std::string what;
    Node *shemaItem;
    union {
        int i;
        // std::string s;
        float f;
        double d;
        bool b;
    } value;
    std::string strValue;
    //FilterExpression() : value(0) {
        // value.i = 0;
    //}
};

class Reader {
public:

    // class Eof {};

    class PathNotFound {};

    class DumpObject {};

    Reader(const std::string & filename);
    ~Reader();

    header readHeader();

    void readBlock(const header &header, const FilterExpression *filter = nullptr);

    FilterExpression compileCondition(const std::string &what, const std::string &condition, const header &header);

    bool eof();
private:

    class Private;
    Private *d = nullptr;

    // int64_t readLong(std::istream &input);
    std::string readString(DeflatedBuffer &input);
    float readFloat(DeflatedBuffer &input);
    double readDouble(DeflatedBuffer &input);
    bool readBoolean(DeflatedBuffer &input);

    void dumpShema(const std::unique_ptr<Node> &schema, int level = 0) const;
    void decodeBlock(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema, const FilterExpression *filter);

    void dumpDocument(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema, int level);
};

}

#endif /* defined(__avroq__avroreader__) */
