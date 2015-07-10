#ifndef __avroq__avroreader__
#define __avroq__avroreader__

#include <string>
#include <cstdint>
#include <map>

#include "header.h"
#include "dumper/tsvexpression.h"

namespace avro {
namespace node {
    class Node;
}

struct Block;

class Reader {
public:

    Reader(const std::string & filename);
    ~Reader();

    header readHeader();

    void nextBlock(const header &header, avro::Block &block);

    dumper::TsvExpression compileFieldsList(const std::string &filedList, const header &header);

    bool eof();
private:

    class Private;
    Private *d = nullptr;


    void dumpShema(const std::unique_ptr<node::Node> &schema, int level = 0) const;

    const node::Node* schemaNodeByPath(const std::string &path, const header &header);

    template <class T>
    void skipOrApplyFilter(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema);
};

}

#endif /* defined(__avroq__avroreader__) */
