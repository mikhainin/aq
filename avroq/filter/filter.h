#ifndef __filter__filter__
#define __filter__filter__

#include <memory>
#include <string>
#include <vector>

namespace filter {

namespace detail {
    struct expression_ast;
}
struct equality_expression;

class Filter {
public:
    explicit Filter(std::shared_ptr<detail::expression_ast> ast);

    void addExpression(equality_expression &expr);
    std::vector<std::string> getUsedPaths() const;
    const std::vector<equality_expression*> &getPredicates();
    bool expressionPassed() const;
    void resetState();
private:
    std::shared_ptr<detail::expression_ast> ast;
    std::vector<equality_expression*> predicateList;
};

}

#endif