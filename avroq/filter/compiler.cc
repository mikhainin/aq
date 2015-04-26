#include <iostream>
#include <vector>
#include <string>

#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_function.hpp>

#include "filter.cc"

#include "compiler.h"

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
              , boost::recursive_wrapper<unary_op>
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

    struct unary_op
    {
        unary_op(
            char op
          , expression_ast const& subject)
        : op(op), subject(subject) {}

        char op;
        expression_ast subject;
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

    ///////////////////////////////////////////////////////////////////////////
    //  Walk the tree
    ///////////////////////////////////////////////////////////////////////////
    struct ast_print
    {
        typedef void result_type;

        void operator()(qi::info::nil) const {}
        void operator()(int n) const { std::cout << n; }
        void operator()(const std::string &s) const { std::cout << s; }
        void operator()(const equality_expression &s) const {
            std::cout << s.identifier << (s.op == s.EQ ? "==" : "!=");
            this->operator()(s.constant);
        }

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
        }

        qi::rule<Iterator, expression_ast(), ascii::space_type>
        logical_expression, condition, braces_expr;

        qi::rule<Iterator, equality_expression::type(), ascii::space_type> constant;
        qi::rule<Iterator, equality_expression(), ascii::space_type> equality_expr;
        qi::rule<Iterator, std::string(), ascii::space_type> quoted_string;
        qi::rule<Iterator, std::string(), ascii::space_type> identifier;
    };

}

namespace filter {

std::shared_ptr<Filter> Compiler::compile(const std::string &str) {
    using boost::spirit::ascii::space;
    using client::expression_ast;
    typedef std::string::const_iterator iterator_type;
    typedef client::calculator<iterator_type> calculator;

    std::string::const_iterator iter = str.begin();
    std::string::const_iterator end = str.end();
    expression_ast ast;
    // ast_print printer;
    calculator calc; // Our grammar
    bool r = phrase_parse(iter, end, calc, space, ast);

    if (r && iter == end)
    {
        std::cout << "-------------------------\n";
        std::cout << "Parsing succeeded\n";
        // printer(ast);
        std::cout << "\n-------------------------\n";
    }
    else
    {
        std::string rest(iter, end);
        std::cout << "-------------------------\n";
        std::cout << "Parsing failed\n";
        std::cout << "stopped at: \": " << rest << "\"\n";
        std::cout << "-------------------------\n";
    }
    return std::make_shared<Filter>();
}


}
