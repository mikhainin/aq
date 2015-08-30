#ifndef __filter__filter__
#define __filter__filter__

#include <memory>
#include <vector>

#include "detail/ast.hpp"

namespace filter {

namespace detail {
    struct expression_ast;
}
struct equality_expression;
struct record_expression;

class Filter {
    Filter() = delete;
public:
    explicit Filter(const detail::expression_ast &ast);
    explicit Filter(const Filter& oldFilter);

    const std::vector<equality_expression*> &getPredicates();
    std::vector<record_expression*> getRecordExpressions();

    std::vector<equality_expression*> getPredicates(record_expression*r);
    std::vector<record_expression*> getRecordExpressions(equality_expression*e);
    std::vector<record_expression*> getRecordExpressions(record_expression*r);

    bool expressionPassed() const;
    void resetState();
private:
    detail::expression_ast ast;
    std::vector<equality_expression*> predicateList;
};

}

#endif