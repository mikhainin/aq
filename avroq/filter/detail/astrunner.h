#ifndef __avroq_filter_detail_astrunner_hpp
#define __avroq_filter_detail_astrunner_hpp

#include <boost/spirit/include/qi.hpp>

#include <filter/equality_expression.h>
#include <filter/record_expression.h>
#include "ast.hpp"

namespace filter {
namespace detail {

namespace qi = boost::spirit::qi;


struct AstRunner
{

    typedef bool result_type;

    AstRunner() {
    }

    bool operator()(qi::info::nil_) const { return false; }
    bool operator()(int n) const { assert(false); return false; }
    bool operator()(const std::string &s) const { assert(false); return false; }
    bool operator()(const nil &) const { assert(false); return false; }

    bool operator()(const equality_expression &s) const {
        return s.getState();
    }

    bool operator()(detail::expression_ast const& ast) const
    {
        return boost::apply_visitor(*this, ast.expr);
    }

    bool operator()(record_expression const& r) const {
        // return false; // TODO: implement me
        // return boost::apply_visitor(*this, r.ast.expr);
        return r.getState();
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
    bool operator()(detail::not_op const& expr) const
    {
        return !boost::apply_visitor(*this, expr.expr.expr);
    }

};



}
}

#endif
