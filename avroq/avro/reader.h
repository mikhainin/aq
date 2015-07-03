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

#include "stringbuffer.h"
namespace filter {
    class Filter;
}

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

    void readBlock(const header &header, const TsvExpression &wd);

    TsvExpression compileFieldsList(const std::string &filedList, const header &header);

    void setFilter(std::shared_ptr<filter::Filter> filter, const header &header);

    bool eof();
private:

    class Private;
    Private *d = nullptr;

    template <typename T>
    T read(DeflatedBuffer &input);

    template <typename T>
    void skip(DeflatedBuffer &input);

    // int64_t readLong(std::istream &input);
    std::string readString(DeflatedBuffer &input);
    StringBuffer readStringBuffer(DeflatedBuffer &input);
    void skipString(DeflatedBuffer &input);
    void skipInt(DeflatedBuffer &input);
    float readFloat(DeflatedBuffer &input);
    double readDouble(DeflatedBuffer &input);
    bool readBoolean(DeflatedBuffer &input);

    void dumpShema(const std::unique_ptr<Node> &schema, int level = 0) const;
    void decodeDocument(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema);

    //void dumpDocument(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema, int level);

    template <class T>
    void dumpDocument(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema, T &dumper);

    const Node* schemaNodeByPath(const std::string &path, const header &header);

    template <class T>
    void skipOrApplyFilter(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema);
};

}

#endif /* defined(__avroq__avroreader__) */
