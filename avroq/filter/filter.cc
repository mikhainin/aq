#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>

#include "detail/astrunner.h"
#include "equality_expression.h"
#include "record_expression.h"

#include "filter.h"

namespace qi = boost::spirit::qi;

namespace filter {

struct ExpressionExtractor
{

    typedef void result_type;

    ExpressionExtractor(std::vector<equality_expression*> &list) : list(list){
    }

    void operator()(qi::info::nil_) {}
    void operator()(int n) { std::cout << n; }
    void operator()(const std::string &s) { std::cout << s; }
    void operator()(equality_expression &s) {
        // filter.addExpression(s);
        list.push_back(&s);
    }
    void operator()(record_expression &r) {
        // boost::apply_visitor(*this, r.ast.expr);
        //filter.addExpression(s);
        // TODO: implement me
    }

    void operator()(const nil &) {  }

    void operator()(detail::expression_ast& ast)
    {
        boost::apply_visitor(*this, ast.expr);
    }
    void operator()(detail::binary_op& expr)
    {
        boost::apply_visitor(*this, expr.left.expr);
        boost::apply_visitor(*this, expr.right.expr);
    }
    void operator()(detail::not_op& expr)
    {
        boost::apply_visitor(*this, expr.expr.expr);
    }

    std::vector<equality_expression*> &list;
};

struct RecordExtractor
{

    typedef void result_type;

    RecordExtractor(std::vector<record_expression*> &list)
        : list(list) {
    }

    void operator()(qi::info::nil_) {}
    void operator()(int n) { std::cout << n; }
    void operator()(const std::string &s) { std::cout << s; }
    void operator()(equality_expression &s) {
        // filter.addExpression(s);
    }
    void operator()(record_expression &r) {
        // boost::apply_visitor(*this, r.ast.expr);
        //filter.addExpression(s);
        // TODO: implement me
        list.push_back(&r);
    }

    void operator()(const nil &) {  }

    void operator()(detail::expression_ast& ast)
    {
        boost::apply_visitor(*this, ast.expr);
    }
    void operator()(detail::binary_op& expr)
    {
        boost::apply_visitor(*this, expr.left.expr);
        boost::apply_visitor(*this, expr.right.expr);
    }
    void operator()(detail::not_op& expr)
    {
        boost::apply_visitor(*this, expr.expr.expr);
    }

    std::vector<record_expression*> &list;
};

struct ExpressionResetter
{

    typedef void result_type;

    ExpressionResetter() {
    }

    void operator()(qi::info::nil_) const {}
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
    void operator()(record_expression & r) const {
        boost::apply_visitor(*this, r.ast.expr);
        r.resetState();
    }
    void operator()(detail::binary_op& expr) const
    {
        boost::apply_visitor(*this, expr.left.expr);
        boost::apply_visitor(*this, expr.right.expr);
    }
    void operator()(detail::not_op& expr) const
    {
        boost::apply_visitor(*this, expr.expr.expr);
    }
};

Filter::Filter(const detail::expression_ast &ast) : ast(ast) {
    ExpressionExtractor extractor(predicateList);
    extractor(this->ast);
}

Filter::Filter(const Filter& oldFilter) : ast(oldFilter.ast) {
    ExpressionExtractor extractor(predicateList);
    extractor(this->ast);
}

bool Filter::expressionPassed() const {
    detail::AstRunner runner;
    return runner(ast);
}

std::vector<record_expression*> Filter::getRecordExpressions() {
    std::vector<record_expression*> list;

    RecordExtractor getRecords(list);
    getRecords(ast);

    return list;
}

std::vector<equality_expression*> Filter::getPredicates(record_expression*r) {

    std::vector<equality_expression*> list;

    ExpressionExtractor extractor(list);
    extractor(r->ast);

    return list;

}

std::vector<record_expression*> Filter::getRecordExpressions(equality_expression*e) {

    std::vector<record_expression*> list;

    RecordExtractor getRecords(list);
    getRecords(*e);

    return list;
}

std::vector<record_expression*> Filter::getRecordExpressions(record_expression*r) {
    std::vector<record_expression*> list;

    RecordExtractor getRecords(list);
    getRecords(*r);

    return list;

}


const std::vector<equality_expression*> &Filter::getPredicates() {
    return predicateList;
}

void Filter::resetState() {
    ExpressionResetter reset;
    reset(ast);
}

}
