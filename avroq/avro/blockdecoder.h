#ifndef __avroq__blockdecoder__
#define __avroq__blockdecoder__

#include <memory>
#include <unordered_map>

#include "dumper/tsvexpression.h"

namespace filter {
  class Filter;
}
namespace avro {

struct Block;
struct header;
class DeflatedBuffer;
class Limiter;

namespace node {
    class Node;
}
namespace predicate {
    class Predicate;
}

class BlockDecoder {
public:

    BlockDecoder(const struct header &header, Limiter &limit);

    void decodeAndDumpBlock(Block &block);
    void setFilter(std::unique_ptr<filter::Filter> flt);
    void setTsvFilterExpression(const dumper::TsvExpression &tsvFieldsList);
    void setDumpMethod(std::function<void(const std::string &)> dumpMethod);

private:
    const struct header &header;
    Limiter &limit;
    dumper::TsvExpression tsvFieldsList;
    std::unique_ptr<filter::Filter> filter;
    std::unordered_multimap<const node::Node *, std::shared_ptr<predicate::Predicate>> filterItems;
    std::function<void(const std::string &)> dumpMethod;


    void decodeDocument(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema);

    template <class T>
    void dumpDocument(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema, T &dumper);

    template <typename T>
    void skipOrApplyFilter(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema);
    
    const node::Node* schemaNodeByPath(const std::string &path);
};


}

#endif
