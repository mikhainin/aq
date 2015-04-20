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
class Limiter;

struct header {
    std::map<std::string, std::string> metadata;
    std::unique_ptr<Node> schema;
    int nodesNumber = 0;
    char sync[16] = {0}; // TODO sync length to a constant
};

struct FilterExpression {
    std::string what;
    const Node *shemaItem;
    union {
        long i;
        // std::string s;
        float f;
        double d;
        bool b;
    } value;
    std::string strValue;
};


struct TsvExpression {
	std::map<int, int> what;
	int pos = 0;
};

class Reader {
public:

    // class Eof {};

    class PathNotFound {
    public:
    	PathNotFound(const std::string &path) : path(path) {
    	}
    	const std::string &getPath() const {
    		return path;
    	}
    private:
    	std::string path;
    };

    class DumpObject {};

    Reader(const std::string & filename, Limiter &limit);
    ~Reader();

    header readHeader();

    void readBlock(const header &header, const FilterExpression *filter, const TsvExpression &wd);

    FilterExpression compileCondition(const std::string &what, const std::string &condition, const header &header);
    TsvExpression compileFieldsList(const std::string &filedList, const header &header);

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

    template <class T>
    void writeDocument(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema, T &dumper);

    const Node* schemaNodeByPath(const std::string &path, const header &header);
};

}

#endif /* defined(__avroq__avroreader__) */
