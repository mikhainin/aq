#include <iostream>
#include <vector>
#include <string>

#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_function.hpp>

#include "detail/ast.hpp"
#include "compiler.h"
#include "equality_expression.h"
#include "filter.h"
#include "string_operator.h"

namespace filter {

namespace client
{
    namespace qi = boost::spirit::qi;
    namespace ascii = boost::spirit::ascii;

    ///////////////////////////////////////////////////////////////////////////
    //  Walk the tree
    ///////////////////////////////////////////////////////////////////////////
    struct ast_print
    {
    	using nil = qi::info::nil_;

        typedef void result_type;

        void operator()(qi::info::nil_) const {}
        void operator()(int n) const { std::cout << n; }
        void operator()(bool b) const { std::cout << (b ? "true" : "false"); }
        void operator()(const std::string &s) const { std::cout << s; }
        void operator()(const equality_expression &s) const {
            std::cout << s.identifier << (s.op == s.EQ ? "==" : "!=");
            this->operator()(s.constant);
        }

        void operator()(const nil &) const { std::cout << "/nil/"; }

        void operator()(detail::expression_ast const& ast) const
        {
            boost::apply_visitor(*this, ast.expr);
        }

        void operator()(detail::binary_op const& expr) const
        {
            std::cout << "op:" << expr.op << "(";
            boost::apply_visitor(*this, expr.left.expr);
            std::cout << ", ";
            boost::apply_visitor(*this, expr.right.expr);
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
    struct calculator : qi::grammar<Iterator, detail::expression_ast(), ascii::space_type>
    {
        calculator() : calculator::base_type(condition)
        {
            using qi::_val;
            using qi::_1;
            using qi::int_;
            using qi::bool_;
            using qi::double_;
            using qi::string;
            using qi::lexeme;
            using qi::lit;
            using ascii::char_;


            quoted_string =
                  lexeme['"' >> *(char_ - '"') >> '"']
                | lexeme['\'' >> *(char_ - '\'') >> '\'']
                ;

            constant =
                  double_           [_val = _1 ]
                | bool_             [_val = _1 ]
                | int_              [_val = _1 ]
                | quoted_string     [_val = _1 ]
                | lit("nil")        [_val = filter::nil()];

            identifier =
                lexeme[+char_("0-9a-zA-Z._")]
                ;


            equality_expr =
                identifier                  [_val =  _1]
                    >>  ( ("==" >> constant [_val == _1])
                        | ("~=" >> constant [_val != _1])
                        | ("<"  >> constant [_val <  _1])
                        | (">"  >> constant [_val >  _1])
                        | ("<=" >> constant [_val <= _1])
                        | (">=" >> constant [_val >= _1])
                        )
                    |

                identifier                       [_val = _1]
                    >> (
                            (
                                lit(":contains(")      [_val |= string_operator(string_operator::CONTAINS)]
                                | lit(":starts_with(") [_val |= string_operator(string_operator::STARTS_WITH)]
                                | lit(":ends_with(")   [_val |= string_operator(string_operator::ENDS_WITH)]
                            )
                         >> quoted_string        [_val |= _1]
                         >> ')'
                       )
                ;

            braces_expr =
                  '(' >> logical_expression   [_val = _1] >> ')'
                | equality_expr               [_val = _1]
                | lit("not") >> equality_expr [_val != _1];

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

        qi::rule<Iterator, detail::expression_ast(), ascii::space_type>
        logical_expression, condition, braces_expr;

        qi::rule<Iterator, equality_expression::type(), ascii::space_type> constant;
        qi::rule<Iterator, equality_expression(), ascii::space_type> equality_expr;
        qi::rule<Iterator, std::string(), ascii::space_type> quoted_string;
        qi::rule<Iterator, std::string(), ascii::space_type> identifier;
    };

}

Compiler::CompileError::CompileError(const std::string &rest)
    : rest(rest) {
}

std::string Compiler::CompileError::what() const {
    return "stopped at: \": " + rest + "\"\n";
}

std::shared_ptr<Filter> Compiler::compile(const std::string &str) {
    using boost::spirit::ascii::space;
    using detail::expression_ast;
    typedef std::string::const_iterator iterator_type;
    typedef client::calculator<iterator_type> calculator;

    std::string::const_iterator iter = str.begin();
    std::string::const_iterator end = str.end();
    detail::expression_ast ast; // = std::make_shared<detail::expression_ast>();
    // ast_print printer;
    calculator calc; // Our grammar
    bool r = phrase_parse(iter, end, calc, space, ast);

    if (r && iter == end)
    {
        // std::cout << "-------------------------\n";
        // std::cout << "Parsing succeeded\n";
        // printer(ast);
        // std::cout << "\n-------------------------\n";
    }
    else
    {
        std::string rest(iter, end);
        throw CompileError(rest);
    }
    return std::make_shared<Filter>(ast);
}


}
