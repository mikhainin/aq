#include <boost/algorithm/string.hpp>

#include <avro/exception.h>
#include <avro/node/all_nodes.h>
#include <avro/node/nodebypath.h>
#include <filter/equality_expression.h>
#include <filter/filter.h>
#include <filter/record_expression.h>

#include "predicate.h"
#include "list.h"

namespace avro {
namespace predicate {

    struct TypeName {
        TypeName(const std::string &name)
            : name(name) {
        }

        std::string name;
    };

    template <typename T>
    struct ThowTypeName {
        using result_type = T;

        T operator() (double) const {
            throw TypeName("double");
        }
        T operator() (float) const {
            throw TypeName("float");
        }
        T operator() (int) const {
            throw TypeName("int");
        }
        T operator() (const std::string &) const {
            throw TypeName("string");
        }

        T operator() (bool) const {
            throw TypeName("bool");
        }
        template <typename AgrgType>
        T operator() (const AgrgType&) const {
            throw TypeName(std::string("Unknown type:") + typeid(AgrgType).name());
        }
    };

    template <>
    double ThowTypeName<double>::operator() (double d) const {
        return d;
    }
    template <>
    double ThowTypeName<double>::operator() (float f) const {
        return f;
    }
    template <>
    double ThowTypeName<double>::operator() (int i) const {
        return i;
    }

    using ToDouble = ThowTypeName<double>;


    template <>
    float ThowTypeName<float>::operator() (double d) const {
        return d;
    }
    template <>
    float ThowTypeName<float>::operator() (float f) const {
        return f;
    }
    template <>
    float ThowTypeName<float>::operator() (int i) const {
        return i;
    }

    using ToFloat = ThowTypeName<float>;


    template <>
    int ThowTypeName<int>::operator() (double d) const {
        return d;
    }
    template <>
    int ThowTypeName<int>::operator() (float f) const {
        return f;
    }
    template <>
    int ThowTypeName<int>::operator() (int i) const {
        return i;
    }

    using ToInt = ThowTypeName<int>;


    template <>
    std::string ThowTypeName<std::string>::operator() (const std::string &s) const {
        return s;
    }

    using ToString = ThowTypeName<std::string>;


    template <>
    bool ThowTypeName<bool>::operator() (bool b) const {
        return b;
    }

    using ToBoolean = ThowTypeName<bool>;



List::List(std::unique_ptr<filter::Filter> filter, const node::Node *rootNode)
    : filter(std::move(filter)),
      rootNode(rootNode) {

      assignItems();

}

bool List::expressionPassed() {
    return filter->expressionPassed();
}

void List::resetState() {
    filter->resetState();
}


void List::assignItems() {

    for(auto &predicate : filter->getPredicates()) {
        auto filterNode = schemaPathByIdent(predicate->identifier);
        processPredicate(predicate, filterNode);
        // std::cout << "pred node " << predicate->identifier << std::endl;
    }

    for(auto &record : filter->getRecordExpressions()) {
        auto parentNode = unwrapCustom(schemaPathByIdent(record->identifier));
        parentNode = notNullUnion(parentNode);
        parentNode = unwrapCustom(parentNode);
        // std::cout << "record node " << record->identifier << " " << parentNode->getTypeName() << std::endl;
        // (void)parentNode; // TODO: use me

        if (parentNode->is<node::Array>()) {
            filterItems.insert(
                    std::make_pair(
                        parentNode,
                        std::make_shared<predicate::RecordPredicate>(record)
                    )
                );
            // put predicate on both items: on array and on element
            // std::cout << "IS ARRAY " << record->identifier << std::endl;
            parentNode = parentNode->as<node::Array>().getItemsType().get();
            // parentNode = notNullUnion(parentNode);
            parentNode = unwrapCustom(parentNode);


            filterItems.insert(
                    std::make_pair(
                        parentNode,
                        std::make_shared<predicate::RecordPredicate>(record)
                    )
                );
            for(auto &predicate : filter->getPredicates(record)) {
                auto filterNode = schemaPathByIdent(predicate->identifier, parentNode);
                /// std::cout << "record pred node " << predicate->identifier << std::endl;
                processPredicate(predicate, filterNode);
            }

        } else {
            for(auto &predicate : filter->getPredicates(record)) {
                auto filterNode = schemaPathByIdent(predicate->identifier, parentNode);
                std::cout << "record pred node " << predicate->identifier << std::endl;
                processPredicate(predicate, filterNode);
            }
        }

    }
}

void List::processPredicate(
            filter::equality_expression* filterPredicate,
            const node::Node * filterNode) {

    filterNode = unwrapCustom(filterNode);

    if (filterNode->is<node::Union>()) {
        for( auto &p : filterNode->as<node::Union>().getChildren()) {
            if (filterPredicate->op == filter::equality_expression::IS_NIL ||
                filterPredicate->op == filter::equality_expression::NOT_NIL
            ) {
                if (!filterNode->as<node::Union>().containsNull()) {
                    throw std::runtime_error("Field '" + filterPredicate->identifier + "' can not be null");
                }

                // TODO lookup for index of NULL-node in the union

                break;
            }

            filterNode = unwrapCustom(p.get());

            if (filterNode->isOneOf<
                        node::Array,
                        node::Boolean,
                        node::Double,
                        node::Enum,
                        node::Float,
                        node::Int,
                        node::Long,
                        node::String
                        >()) {
                break;
            }
        }
    } else {
        if (filterPredicate->op == filter::equality_expression::IS_NIL ||
            filterPredicate->op == filter::equality_expression::NOT_NIL
        ) {
            // TODO: take into accout parent nodes path
            throw std::runtime_error("Field '" + filterPredicate->identifier + "' can not be null");
        }
    }

    assert(!filterNode->is<node::Custom>());

    if (filterNode->is<node::Array>()) {
        // put predicate on both items: on array and on element
        filterItems.insert(
                std::make_pair(
                    filterNode,
                    std::make_shared<predicate::Predicate>(filterPredicate)
                )
            );

        filterNode = filterNode->as<node::Array>().getItemsType().get();
        filterNode = unwrapCustom(filterNode);

        if (filterNode->is<node::Union>()) {
            throw std::runtime_error("Only arrays of primitive types are supported. "
             "Can't process field '" + filterPredicate->identifier + "'");
        }
    }

    if (filterNode->isOneOf<node::Union>()) {
        ; // ok, acceptable only for isnil/notnil operations
    } else if (filterNode->is<node::Enum>()) {
        auto const &e = filterNode->as<node::Enum>();
        int i = e.findIndexForValue(boost::get<std::string>(filterPredicate->constant));
        if (i == -1) {
            // TODO: add list of valid values
            throw std::runtime_error("Invalid value for enum field '" + filterPredicate->identifier + "'");
        }
        filterPredicate->constant = i;
    } else if (filterNode->is<node::String>()) {
        filterPredicate->constant = convertFilterConstant<ToString>(filterPredicate, filterNode);
    } else if (filterNode->is<node::Boolean>()) {
        filterPredicate->constant = convertFilterConstant<ToBoolean>(filterPredicate, filterNode);
    } else if (filterNode->is<node::Double>()) {
        filterPredicate->constant = convertFilterConstant<ToDouble>(filterPredicate, filterNode);
    } else if (filterNode->is<node::Float>()) {
        filterPredicate->constant = convertFilterConstant<ToFloat>(filterPredicate, filterNode);
    } else if (filterNode->isOneOf<node::Int, node::Long>()) {
        filterPredicate->constant = convertFilterConstant<ToInt>(filterPredicate, filterNode);
    } else {
        throw std::runtime_error(
            "Sorry, but type '" + filterNode->getTypeName() +
            "' for field '" + filterPredicate->identifier + "' "
            "Is not yet supported in filter expression.");
    }
    filterItems.insert(
            std::make_pair(
                filterNode,
                std::make_shared<predicate::Predicate>(filterPredicate)
            )
        );

}


const node::Node* List::schemaNodeByPath(const std::string &path) {
    auto n = node::nodeByPathIgnoreArray(path, rootNode);
    if (n == nullptr) {
        throw PathNotFound(path);
    }
    return n;
}

const node::Node * List::schemaPathByIdent(
    const std::string &path, const node::Node * startNode) {

    std::vector<std::string> chunks;
    boost::algorithm::split(chunks, path, boost::is_any_of("."));

    auto currentNode = startNode ? startNode : rootNode;

    for(auto p = chunks.begin(); p != chunks.end(); ) {

        auto chunkItem = currentNode;

        if (chunkItem->is<node::Custom>()) {
            chunkItem = chunkItem->as<node::Custom>().getDefinition().get();
        /*} else if (chunkItem->is<node::Array>()) {
            chunkItem = chunkItem->as<node::Array>().getItemsType().get();*/
        } else if (chunkItem->is<node::Union>()) {
            for( auto &n : chunkItem->as<node::Union>().getChildren()) {
                if (!n->is<node::Null>()) {
                    chunkItem = n.get();
                }
            }
        } else if (chunkItem->is<node::Record>()) {
            for( auto &n : chunkItem->as<node::Record>().getChildren()) {
                if (n->getItemName() == *p) {
                    chunkItem = n.get();
                    ++p;
                    break;
                }
            }
        }
        // std::cout << chunkItem->getItemName() << ':' << chunkItem->getTypeName() << std::endl;

        if (chunkItem != currentNode) {
            currentNode = chunkItem;
            continue;
        }
        throw PathNotFound(path);
        // return nullptr;
    }

    if (currentNode == nullptr) {
        throw PathNotFound(path);
    }
/*
    std::vector<const node::Node *> result = {currentNode};

    while( (auto parent = currentNode->parent) != nullptr) {

    }
*/
    return currentNode;


}


template <typename T>
typename T::result_type List::convertFilterConstant(const filter::equality_expression* expr, const node::Node *filterNode) const {
    try {
        return boost::apply_visitor(T(), expr->constant);
    } catch(const TypeName &e) {
        throw std::runtime_error("Invalid type for field '" + expr->identifier + "'"
            + " expected: " + filterNode->getTypeName() + ", got: " +
            e.name);
    }
}


const node::Node * List::unwrapCustom(const node::Node * node) {
    while (node->is<node::Custom>()) {
        node = node->as<node::Custom>().getDefinition().get();
    }
    return node;
}
const node::Node * List::notNullUnion(const node::Node * node) {
    while (node->is<node::Union>()) {
        for( auto &n : node->as<node::Union>().getChildren()) {
            if (!n->is<node::Null>()) {
                node = n.get();
                break;
            }
        }
    }
    return node;
}



}
}
