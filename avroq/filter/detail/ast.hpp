//
//  ast.hpp
//  
//
//  Created by Mikhail Galanin on 26/04/15.
//
//

#ifndef _ast_hpp
#define _ast_hpp

#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_function.hpp>

#include "../nil.h"
#include "../equality_expression.h"

namespace filter {
namespace detail {

namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;

///////////////////////////////////////////////////////////////////////////
//  Our AST
///////////////////////////////////////////////////////////////////////////
struct binary_op;
//struct equality_expression;

struct expression_ast
{
    typedef
        boost::variant<
            nil
          , int
          , std::string
          , boost::recursive_wrapper<equality_expression>
          , boost::recursive_wrapper<expression_ast>
          , boost::recursive_wrapper<binary_op>
        >
    type;

    expression_ast();

    template <typename Expr>
    expression_ast(Expr const& expr)
      : expr(expr) {}

    expression_ast& operator &=(expression_ast const& rhs);
    expression_ast& operator |=(expression_ast const& rhs);

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

}
}
#endif
