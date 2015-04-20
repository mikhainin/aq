
#include "compiler.h"


#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_function.hpp>

#include <iostream>
#include <vector>
#include <string>

namespace client
{
    namespace qi = boost::spirit::qi;
    namespace ascii = boost::spirit::ascii;

    ///////////////////////////////////////////////////////////////////////////
    //  Our AST
    ///////////////////////////////////////////////////////////////////////////
    struct binary_op;
    struct unary_op;
    struct nil {};

    struct expression_ast
    {
        typedef
            boost::variant<
                nil
              , int
              , std::string
              , boost::recursive_wrapper<expression_ast>
              , boost::recursive_wrapper<binary_op>
              , boost::recursive_wrapper<unary_op>
            >
        type;

        expression_ast()
          : expr(nil()) {}

        template <typename Expr>
        expression_ast(Expr const& expr)
          : expr(expr) {}

        expression_ast& operator+=(expression_ast const& rhs);
        expression_ast& operator-=(expression_ast const& rhs);
        expression_ast& operator*=(expression_ast const& rhs);
        expression_ast& operator/=(expression_ast const& rhs);
        expression_ast& operator==(expression_ast const& rhs);
        expression_ast& operator!=(expression_ast const& rhs);
        expression_ast& operator&=(expression_ast const& rhs);
        expression_ast& operator|=(expression_ast const& rhs);

        type expr;
    };

    struct binary_op
    {
        binary_op(
            std::string op
          , expression_ast const& left
          , expression_ast const& right)
        : op(op), left(left), right(right) {}

        std::string op;
        expression_ast left;
        expression_ast right;
    };

    struct unary_op
    {
        unary_op(
            char op
          , expression_ast const& subject)
        : op(op), subject(subject) {}

        char op;
        expression_ast subject;
    };

    expression_ast& expression_ast::operator+=(expression_ast const& rhs)
    {
        expr = binary_op("plus", expr, rhs);
        return *this;
    }

    expression_ast& expression_ast::operator==(expression_ast const& rhs)
    {
        expr = binary_op("==", expr, rhs);
        return *this;
    }

    expression_ast& expression_ast::operator!=(expression_ast const& rhs)
    {
        expr = binary_op("!=", expr, rhs);
        return *this;
    }

    expression_ast& expression_ast::operator-=(expression_ast const& rhs)
    {
        expr = binary_op("-", expr, rhs);
        return *this;
    }

    expression_ast& expression_ast::operator*=(expression_ast const& rhs)
    {
        expr = binary_op("*", expr, rhs);
        return *this;
    }

    expression_ast& expression_ast::operator/=(expression_ast const& rhs)
    {
        expr = binary_op("/", expr, rhs);
        return *this;
    }

    expression_ast& expression_ast::operator&=(expression_ast const& rhs)
    {
        expr = binary_op("and", expr, rhs);
        return *this;
    }

    expression_ast& expression_ast::operator|=(expression_ast const& rhs)
    {
        expr = binary_op("or", expr, rhs);
        return *this;
    }

    // We should be using expression_ast::operator-. There's a bug
    // in phoenix type deduction mechanism that prevents us from
    // doing so. Phoenix will be switching to BOOST_TYPEOF. In the
    // meantime, we will use a phoenix::function below:
    struct negate_expr
    {
        template <typename T>
        struct result { typedef T type; };

        expression_ast operator()(expression_ast const& expr) const
        {
            return expression_ast(unary_op('-', expr));
        }
    };

    boost::phoenix::function<negate_expr> neg;

    ///////////////////////////////////////////////////////////////////////////
    //  Walk the tree
    ///////////////////////////////////////////////////////////////////////////
    struct ast_print
    {
        typedef void result_type;

        // void operator()(qi::info::nil) const {}
        void operator()(int n) const { std::cout << n; }
        void operator()(const std::string &s) const { std::cout << s; }
        void operator()(const nil &) const { std::cout << "/nil/"; }

        void operator()(expression_ast const& ast) const
        {
            boost::apply_visitor(*this, ast.expr);
        }

        void operator()(binary_op const& expr) const
        {
            std::cout << "op:" << expr.op << "(";
            boost::apply_visitor(*this, expr.left.expr);
            std::cout << ", ";
            boost::apply_visitor(*this, expr.right.expr);
            std::cout << ')';
        }

        void operator()(unary_op const& expr) const
        {
            std::cout << "op:" << expr.op << "(";
            boost::apply_visitor(*this, expr.subject.expr);
            std::cout << ')';
        }
    };

/*

creative_id == 123 or (request.uri == "/bad" and r.lua_data =~ nil) or is_local == true

*/
    ///////////////////////////////////////////////////////////////////////////
    //  Our calculator grammar
    ///////////////////////////////////////////////////////////////////////////
    template <typename Iterator>
    struct calculator : qi::grammar<Iterator, expression_ast(), ascii::space_type>
    {
        calculator() : calculator::base_type(condition)
        {
            using qi::_val;
            using qi::_1;
            using qi::int_;
            using qi::string;
            using qi::lexeme;
            using qi::lit;
            using ascii::char_;


            quoted_string =
                  lexeme['"' >> +(char_ - '"') >> '"']
                | lexeme['\'' >> +(char_ - '\'') >> '\'']
                ;

            constant =
                  int_              [_val = _1 ]
                | quoted_string     [_val = _1 ]
                | lit("nil")        [_val = nil()];

            identifier =
                lexeme[+char_("0-9a-zA-Z.")]
                ;


            equality_expr =
                identifier                  [_val = _1]
                    >>  ( ("==" >> constant [_val == _1])
                        | ("~=" >> constant [_val != _1])
                        )
                ;

            braces_expr =
                  '(' >> logical_expression [_val = _1] >> ')'
                | equality_expr             [_val = _1];

            logical_expression =
                    braces_expr                 [_val = _1]
                    >> *( ("and" >> braces_expr [_val &= _1])
                        | ("or"  >> braces_expr [_val |= _1])
                        )
                ;

            condition =
                  logical_expression                [_val = _1]
                ;
            /*
            expression =
                term                            [_val = _1]
                >> *(   ('+' >> term         [_val += _1])
                    |   ('-' >> term            [_val -= _1])
                    )
                ;

            term =
                factor                          [_val = _1]
                >> *(   ('*' >> factor          [_val *= _1])
                    |   ('/' >> factor          [_val /= _1])
                    )
                ;

            factor =
                uint_                           [_val = _1]
                |   '(' >> expression           [_val = _1] >> ')'
                |   ('-' >> factor              [_val = neg(_1)])
                |   ('+' >> factor              [_val = _1])
                ;
                */
        }

        qi::rule<Iterator, expression_ast(), ascii::space_type>
        equality_expr, logical_expression, term, factor, constant, condition, braces_expr;
        qi::rule<Iterator, std::string(), ascii::space_type> quoted_string;
        qi::rule<Iterator, std::string(), ascii::space_type> identifier;
    };
}
