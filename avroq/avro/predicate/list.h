#ifndef __avroq_avro_predicate_list_
#define __avroq_avro_predicate_list_

#include <memory>
#include <unordered_map>
#include <string>
#include <vector>

namespace filter {
    struct equality_expression;
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
    List(std::vector<filter::equality_expression*> exprList, const node::Node *rootNode);

    auto getEqualRange(const node::Node *node) {
        return filterItems.equal_range(node);
    }

private:
    std::vector<filter::equality_expression*> exprList;
    filter_items_t filterItems;
    const node::Node *rootNode;

    void assignItems();
    const node::Node* schemaNodeByPath(const std::string &path);

    template <typename T>
    typename T::result_type convertFilterConstant(const filter::equality_expression* expr, const node::Node *filterNode) const;

    
};

}
}

#endif