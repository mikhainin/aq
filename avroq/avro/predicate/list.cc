
#include <avro/exception.h>
#include <avro/node/all_nodes.h>
#include <avro/node/nodebypath.h>
#include <filter/equality_expression.h>
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



List::List(std::vector<filter::equality_expression*> exprList, const node::Node *rootNode)
    : exprList(exprList),
      rootNode(rootNode) {
      assignItems();
}


void List::assignItems() {
    for(auto &filterPredicate : exprList) {
        const node::Node * filterNode = schemaNodeByPath(filterPredicate->identifier);

        if (filterNode->is<node::Custom>()) {
            filterNode = filterNode->as<node::Custom>().getDefinition().get();
        }
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

                filterNode = p.get();

                if (filterNode->is<node::Custom>()) {
                    filterNode = filterNode->as<node::Custom>().getDefinition().get();
                }
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
                throw std::runtime_error("Field '" + filterPredicate->identifier + "' can not be null");
            }
        }

        if (filterNode->is<node::Array>()) {
            // put predicate on both items: on array and on element
            filterItems.insert(
                    std::make_pair(
                        filterNode,
                        std::make_shared<predicate::Predicate>(filterPredicate)
                    )
                );

            filterNode = filterNode->as<node::Array>().getItemsType().get();

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
}

const node::Node* List::schemaNodeByPath(const std::string &path) {
    auto n = node::nodeByPath(path, rootNode);
    if (n == nullptr) {
        throw PathNotFound(path);
    }
    return n;
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



}
}
