#ifndef __avroq__blockdecoder__
#define __avroq__blockdecoder__

#include <memory>
#include <unordered_map>

#include "dumper/tsvexpression.h"

namespace filter {
    class Filter;
    struct equality_expression;
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
    using parse_func_t = std::function<int(DeflatedBuffer &)>;
public:
    using filter_items_t = std::unordered_multimap<const node::Node *, std::shared_ptr<predicate::Predicate>>;

    BlockDecoder(const struct header &header, Limiter &limit);

    void decodeAndDumpBlock(Block &block);
    void setFilter(std::unique_ptr<filter::Filter> flt);
    void setTsvFilterExpression(const dumper::TsvExpression &tsvFieldsList);
    void setDumpMethod(std::function<void(const std::string &)> dumpMethod);
    void setCountMethod(std::function<void(size_t)> coutMethod);
    void enableCountOnlyMode();

private:
    const struct header &header;
    Limiter &limit;
    dumper::TsvExpression tsvFieldsList;
    std::unique_ptr<filter::Filter> filter;
    filter_items_t filterItems;
    std::function<void(const std::string &)> dumpMethod;
    std::function<void(size_t)> coutMethod;
    bool countOnly = false;

    std::unordered_map<const node::Node *,std::vector<parse_func_t>>
        _pv;

    void decodeDocument(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema);

    void dumpDocument(Block &block);

    template <class T>
    void dumpDocument(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema, T &dumper);

    template <typename T>
    void skipOrApplyFilter(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema);

    template <typename T>
    typename T::result_type convertFilterConstant(const filter::equality_expression* expr, const node::Node *filterNode) const;

    const node::Node* schemaNodeByPath(const std::string &path);

    void compileParser(const std::unique_ptr<node::Node> &schema);


    int compileParser(std::vector<parse_func_t> &parse_items, const std::unique_ptr<node::Node> &schema);

    template <typename SkipType, typename ApplyType, typename... Args>
    void skipOrApplyCompileFilter(std::vector<parse_func_t> &parse_items, const std::unique_ptr<node::Node> &schema, int ret, Args... args);

    template <typename SkipType, typename ApplyType, typename... Args>
    void skipOrApplyCompileFilter_r(std::vector<parse_func_t> &parse_items, const std::unique_ptr<node::Node> &schema, int ret, Args... args);

};


}

#endif
