#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>

#include "detail/astrunner.h"
#include "equality_expression.h"

#include "record_expression.h"

namespace qi = boost::spirit::qi;


namespace filter {


struct ParentSetter
{

    typedef void result_type;

    explicit ParentSetter(record_expression * parent) :
        parent(parent) {
    }

    void operator()(qi::info::nil_) const { }
    void operator()(int n) const {  }
    void operator()(const std::string &s) const {  }
    void operator()(const nil &) const {  }

    void operator()(equality_expression &s) const {
        s.setParent(parent);
    }

    void operator()(detail::expression_ast & ast) const
    {
        return boost::apply_visitor(*this, ast.expr);
    }

    void operator()(record_expression & r) const {
        r.setParent(parent);
    }

    void operator()(detail::binary_op & expr) const
    {
        boost::apply_visitor(*this, expr.left.expr);
        boost::apply_visitor(*this, expr.right.expr);
    }
    void operator()(detail::not_op & expr) const
    {
        boost::apply_visitor(*this, expr.expr.expr);
    }

    record_expression * parent;

};


record_expression::record_expression() : state() {
}


record_expression::record_expression(const detail::array_element &ident):
    state(),
    root_identifier(ident),
    identifier(ident.identifier) {

    is_array_element = true;
    array_index = ident.index;

}

record_expression::record_expression(const std::string &ident):
    state(),
    root_identifier(ident),
    identifier(ident) {
}

record_expression &record_expression::operator &= (const detail::expression_ast &ast) {
    this->ast = ast;

    ParentSetter setParent(this);
    setParent(this->ast);

    return *this;
}

void record_expression::setParent(record_expression * parent) {

    assert(parent && "parent cannot be null");
    this->parent = parent;
    // identifier = parent->identifier + '.' + identifier;

    ParentSetter setParent(this);
    setParent(ast);

}

void record_expression::evaluateState() {
    detail::AstRunner runner;
    bool state = runner(ast);
    // std::cout << "record state=" << state << std::endl;
    setState(state);
}


}
