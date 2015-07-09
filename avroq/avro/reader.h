#ifndef __avroq__avroreader__
#define __avroq__avroreader__

#include <string>
#include <cstdint>
#include <map>

#include "header.h"
#include "stringbuffer.h"
#include "dumper/tsvexpression.h"

namespace filter {
    class Filter;
}

namespace avro {
namespace node {
    class Node;
}

struct Block;
class DeflatedBuffer;
class Limiter;

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

    void nextBlock(const header &header, avro::Block &block);

    void readBlock(const header &header, const dumper::TsvExpression &wd);

    dumper::TsvExpression compileFieldsList(const std::string &filedList, const header &header);

    void setFilter(std::shared_ptr<filter::Filter> filter, const header &header);

    bool eof();
private:

    class Private;
    Private *d = nullptr;

    template <typename T>
    T read(DeflatedBuffer &input);

    template <typename T>
    void skip(DeflatedBuffer &input);

    std::string readString(DeflatedBuffer &input);
    StringBuffer readStringBuffer(DeflatedBuffer &input);
    void skipString(DeflatedBuffer &input);
    void skipInt(DeflatedBuffer &input);

    void dumpShema(const std::unique_ptr<node::Node> &schema, int level = 0) const;
    void decodeDocument(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema);

    //void dumpDocument(DeflatedBuffer &stream, const std::unique_ptr<Node> &schema, int level);

    template <class T>
    void dumpDocument(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema, T &dumper);

    const node::Node* schemaNodeByPath(const std::string &path, const header &header);

    template <class T>
    void skipOrApplyFilter(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema);
};

}

#endif /* defined(__avroq__avroreader__) */
