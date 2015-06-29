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


namespace filter {
namespace detail {
    namespace qi = boost::spirit::qi;
    namespace ascii = boost::spirit::ascii;

    ///////////////////////////////////////////////////////////////////////////
    //  Our AST
    ///////////////////////////////////////////////////////////////////////////
    struct binary_op;
    struct nil {};
    struct equality_expression;
    
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

        expression_ast()
          : expr(nil()) {}

        template <typename Expr>
        expression_ast(Expr const& expr)
          : expr(expr) {}

        expression_ast& operator &=(expression_ast const& rhs);
        expression_ast& operator |=(expression_ast const& rhs);

        type expr;
    };

    struct equality_expression
    {
        enum OP {
            EQ,
            NE
        };
        typedef
            boost::variant<
                nil
              , int
              , std::string
            > type;

        equality_expression() {
        }

        equality_expression(const std::string &ident) :
            identifier(ident) {
        }

        equality_expression& operator == (const type &constant) {
            this->constant = constant;
            op = EQ;
            return *this;
        }

        equality_expression& operator != (const type &constant) {
            this->constant = constant;
            op = NE;
            return *this;
        }

        type constant;
        std::string identifier;
        OP op;

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
          , expression_ast const& right)
        : op(op), left(left), right(right) {}

        OP op;
        expression_ast left;
        expression_ast right;
    };

    expression_ast& expression_ast::operator&=(expression_ast const& rhs)
    {
        expr = binary_op(binary_op::AND, expr, rhs);
        return *this;
    }

    expression_ast& expression_ast::operator|=(expression_ast const& rhs)
    {
        expr = binary_op(binary_op::OR, expr, rhs);
        return *this;
    }
}
}
#endif
