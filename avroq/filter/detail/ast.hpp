//
//  ast.hpp
//  
//
//  Created by Mikhail Galanin on 26/04/15.
//
//

#ifndef __avroq_filter_detail_ast_hpp
#define __avroq_filter_detail_ast_hpp

#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_function.hpp>

#include <filter/nil.h>

namespace filter {

struct record_expression;
struct equality_expression;

namespace detail {

namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;

///////////////////////////////////////////////////////////////////////////
//  Our AST
///////////////////////////////////////////////////////////////////////////
struct binary_op;
struct not_op;

struct expression_ast
{
    typedef
        boost::variant<
            nil
          , int
          , std::string
          , boost::recursive_wrapper<equality_expression>
          , boost::recursive_wrapper<record_expression>
          , boost::recursive_wrapper<expression_ast>
          , boost::recursive_wrapper<binary_op>
          , boost::recursive_wrapper<not_op>
        >
    type;

    expression_ast();

    template <typename Expr>
    expression_ast(Expr const& expr)
      : expr(expr) {}

    expression_ast& operator &=(expression_ast const& rhs);
    expression_ast& operator |=(expression_ast const& rhs);

    expression_ast& operator != (expression_ast const& expr);

    type expr;
};

struct binary_op
{
    enum OP {
        AND,
        OR
    };
    binary_op(
        OP op
      , expression_ast const& left
      , expression_ast const& right);

    OP op;
    expression_ast left;
    expression_ast right;
};

struct not_op
{
	not_op(expression_ast const& expr);

    expression_ast expr;
};

struct array_element {
    enum {
        ANY  = -1,
        ALL  = -2,
        NONE = -3
    };

    array_element();
    array_element(const std::string &identifier);
    array_element& operator |= (int i);

    std::string identifier;
    int index = 0;
};

std::ostream& operator<<(std::ostream& os, const array_element& s);


}
}
#endif
