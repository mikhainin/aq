#ifndef __avroq_avro_predicate_list_
#define __avroq_avro_predicate_list_

#include <memory>
#include <unordered_map>
#include <string>
#include <vector>

namespace filter {
    class Filter;
    struct equality_expression;
    struct record_expression;
}

namespace avro {

namespace node {
    class Node;
}

namespace predicate {

class Predicate;

class List {
public:
    using filter_items_t = std::unordered_multimap<const node::Node *, std::shared_ptr<predicate::Predicate>>;
    List(std::unique_ptr<filter::Filter> filter, const node::Node *rootNode);

    auto getEqualRange(const node::Node *node) {
        return filterItems.equal_range(node);
    }

    bool expressionPassed();
    void resetState();

private:
    std::unique_ptr<filter::Filter> filter;
    filter_items_t filterItems;
    const node::Node *rootNode;

    void assignItems();
    const node::Node* schemaNodeByPath(const std::string &path);

    template <typename T>
    typename T::result_type convertFilterConstant(const filter::equality_expression* expr, const node::Node *filterNode) const;

    const node::Node * schemaPathByIdent(const std::string &path, const node::Node * startNode = nullptr);

    void processPredicate(filter::equality_expression* filterPredicate, const node::Node * filterNode);

    void processRecord(filter::record_expression* filterPredicate, const node::Node * filterNode);


    const node::Node * unwrapCustom(const node::Node * node);
    const node::Node * notNullUnion(const node::Node * node);
};

}
}

#endif