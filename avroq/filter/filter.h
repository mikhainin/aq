#ifndef __filter__filter__
#define __filter__filter__

#include <memory>
#include <string>
#include <vector>

#include "detail/ast.hpp"

namespace filter {

namespace detail {
    struct expression_ast;
}
struct equality_expression;

class Filter {
    Filter() = delete;
public:
    explicit Filter(const detail::expression_ast &ast);
    explicit Filter(const Filter& oldFilter);

    void addExpression(equality_expression &expr);
    std::vector<std::string> getUsedPaths() const;
    const std::vector<equality_expression*> &getPredicates();
    bool expressionPassed() const;
    void resetState();
private:
    detail::expression_ast ast;
    std::vector<equality_expression*> predicateList;
};

}

#endif