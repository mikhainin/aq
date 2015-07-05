#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>

#include "equality_expression.h"

#include "filter.h"

namespace qi = boost::spirit::qi;

namespace filter {

struct ExpressionExtractor
{

    typedef void result_type;

    ExpressionExtractor(Filter &filter) : filter(filter){
    }

    void operator()(qi::info::nil) const {}
    void operator()(int n) const { std::cout << n; }
    void operator()(const std::string &s) const { std::cout << s; }
    void operator()(equality_expression &s) const {
        filter.addExpression(s);
    }

    void operator()(const nil &) const {  }

    void operator()(detail::expression_ast& ast) const
    {
        boost::apply_visitor(*this, ast.expr);
    }
    void operator()(detail::binary_op& expr) const
    {
        boost::apply_visitor(*this, expr.left.expr);
        boost::apply_visitor(*this, expr.right.expr);
    }

    Filter &filter;
};


struct AstRunner
{

    typedef bool result_type;

    AstRunner() {
    }

    bool operator()(qi::info::nil) const { return false; }
    bool operator()(int n) const { assert(false); return false; }
    bool operator()(const std::string &s) const { assert(false); return false; }
    bool operator()(const nil &) const { assert(false); return false; }

    bool operator()(const equality_expression &s) const {
        return s.state;
    }

    bool operator()(detail::expression_ast const& ast) const
    {
        return boost::apply_visitor(*this, ast.expr);
    }

    bool operator()(detail::binary_op const& expr) const
    {
        if (expr.op == detail::binary_op::AND) {
            return boost::apply_visitor(*this, expr.left.expr) &&
                boost::apply_visitor(*this, expr.right.expr);
        } else {
            return boost::apply_visitor(*this, expr.left.expr) ||
                boost::apply_visitor(*this, expr.right.expr);
        }
    }

};

struct ExpressionResetter
{

    typedef void result_type;

    ExpressionResetter() {
    }

    void operator()(qi::info::nil) const {}
    void operator()(int ) const { }
    void operator()(const std::string &) const {  }
    void operator()(equality_expression &s) const {
        s.resetState();
    }

    void operator()(const nil &) const {  }

    void operator()(detail::expression_ast& ast) const
    {
        boost::apply_visitor(*this, ast.expr);
    }
    void operator()(detail::binary_op& expr) const
    {
        boost::apply_visitor(*this, expr.left.expr);
        boost::apply_visitor(*this, expr.right.expr);
    }
};

Filter::Filter(const detail::expression_ast &ast) : ast(ast) {
    ExpressionExtractor extractor(*this);
    extractor(this->ast);
}

bool Filter::expressionPassed() const {
    AstRunner runner;
    return runner(ast);
}

void Filter::addExpression(equality_expression&expr) {
    predicateList.push_back(&expr);
}

std::vector<std::string> Filter::getUsedPaths() const {
    std::vector<std::string> result;

    for(const auto *const expr : predicateList) {
        result.emplace_back(expr->identifier);
    }

    return result;
}

const std::vector<equality_expression*> &Filter::getPredicates() {
    return predicateList;
}

void Filter::resetState() {
    ExpressionResetter reset;
    reset(ast);
}

}
