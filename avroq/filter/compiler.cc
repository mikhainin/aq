//#define BOOST_SPIRIT_DEBUG

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
#include "record_expression.h"

#include "filter.h"
#include "string_operator.h"

namespace filter {
    namespace detail {

      struct push_esc
      {
         template <typename Sig>
         struct result { typedef void type; };

         void operator()(std::string& str, u_char c) const
         {
            switch (c)
            {
               case '"': str += '"';          break;
               case '\'': str += '\'';        break;
               case '\\': str += '\\';        break;
               case '/': str += '/';          break;
               case 'b': str += '\b';         break;
               case 'f': str += '\f';         break;
               case 'n': str += '\n';         break;
               case 'r': str += '\r';         break;
               case 't': str += '\t';         break;
            }
         }
      };

      struct push_utf8
      {
         template <typename Sig>
         struct result { typedef void type; };

         void operator()(std::string& utf8, u_char code_point) const
         {
            typedef std::back_insert_iterator<std::string> insert_iter;
            insert_iter out_iter(utf8);
            boost::utf8_output_iterator<insert_iter> utf8_iter(out_iter);
            *utf8_iter++ = code_point;
         }
      };

      struct push_hex
      {
         template <typename Sig>
         struct result { typedef void type; };

         void operator()(std::string& str, u_char c) const
         {
            str += c;
         }
      };
   }

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


      template <typename Iterator>
      struct quoted_string : qi::grammar<Iterator, std::string()>
      {
         quoted_string();
         qi::rule<Iterator, void(std::string&)> escape;
         qi::rule<Iterator, void(std::string&)> char_esc;
         qi::rule<Iterator, std::string()> quoted;
      };

      template< typename Iterator >
      quoted_string<Iterator>::quoted_string()
         : quoted_string::base_type(quoted)
      {
         qi::char_type char_;
         qi::_val_type _val;
         qi::_r1_type _r1;
         qi::_1_type _1;
         qi::lit_type lit;
         qi::repeat_type repeat;
         qi::hex_type hex;
         qi::standard::cntrl_type cntrl;

         using boost::spirit::qi::uint_parser;
         using boost::phoenix::function;

         uint_parser<u_char, 16, 4, 4> hex4;
         uint_parser<u_char, 16, 2, 2> hex2;
         uint_parser<u_char, 8, 3, 3>  oct3;
         function<detail::push_utf8> push_utf8;
         function<detail::push_esc>  push_esc;
         function<detail::push_hex>  push_hex;

         escape =
                ('u' > hex4)            [push_utf8(_r1, _1)]
            |   ('x' > hex2)            [push_hex(_r1, _1)]
            |   oct3                    [push_hex(_r1, _1)]
            |   char_("'\"\\/bfnrt")    [push_esc(_r1, _1)]
            ;

         char_esc =
            '\\' > escape(_r1)
            ;

         // a double quoted string containes 0 or more characters
         // where a character is:
         //     any-Unicode-character-except-"-or-\-or-control-character
         //
         quoted =
                '"'
                > *(  char_esc(_val)
                    | (char_ - '"' - '\\' - cntrl)    [_val += _1]
                   )
                > '"'
            |
                  '\''
                > *(  char_esc(_val)
                    | (char_ - '\'' - '\\' - cntrl)    [_val += _1]
                   )
                > '\''
            ;

         BOOST_SPIRIT_DEBUG_NODE(escape);
         BOOST_SPIRIT_DEBUG_NODE(char_esc);
         BOOST_SPIRIT_DEBUG_NODE(quoted);
      }

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

            constant =
                  double_           [_val = _1 ]
                | bool_             [_val = _1 ]
                | int_              [_val = _1 ]
                | quoted_string_    [_val = _1 ]
                | lit("nil")        [_val = filter::nil()];

            identifier =
                lexeme[+char_("0-9a-zA-Z._")]
                ;


            array_expression =
                identifier             [_val  =  _1]
                    >>  '['
                        >> ( int_        [_val |= _1]
                           | lit("any")  [_val |= detail::array_element::ANY]
                           | lit("all")  [_val |= detail::array_element::ALL]
                           | lit("none") [_val |= detail::array_element::NONE]
                           )
                        >> ']'


                ;

            equality_expr =
                ( array_expression           [_val =  _1]
                | identifier                 [_val =  _1]
                )
                    >>  ( ("==" >> constant        [_val == _1])
                        | ("~=" >> constant        [_val != _1])
                        | ("<"  >> constant        [_val <  _1])
                        | (">"  >> constant        [_val >  _1])
                        | ("<=" >> constant        [_val <= _1])
                        | (">=" >> constant        [_val >= _1])
                        )
                |

                ( array_expression           [_val =  _1]
                | identifier                 [_val =  _1]
                )
                    >> (
                            (
                                lit(":contains(")      [_val |= string_operator(string_operator::CONTAINS)]
                                | lit(":starts_with(") [_val |= string_operator(string_operator::STARTS_WITH)]
                                | lit(":ends_with(")   [_val |= string_operator(string_operator::ENDS_WITH)]
                            )
                         >> quoted_string_        [_val |= _1]
                         >> ')'
                       )
                ;

            record_expr =
                ( array_expression           [_val =  _1]
                | identifier                 [_val =  _1]
                )
                    > '(' > logical_expression [_val &=  _1] > ')';

            braces_expr =
                  '(' >> logical_expression   [_val = _1] >> ')'
                | equality_expr               [_val = _1]
                | record_expr                 [_val = _1]
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

            BOOST_SPIRIT_DEBUG_NODE(array_expression);
            BOOST_SPIRIT_DEBUG_NODE(identifier);
            //BOOST_SPIRIT_DEBUG_NODE(condition);
            //BOOST_SPIRIT_DEBUG_NODE(braces_expr);
        }

        qi::rule<Iterator, detail::expression_ast(), ascii::space_type>
        logical_expression, condition, braces_expr;

        qi::rule<Iterator, equality_expression::type(), ascii::space_type> constant;
        qi::rule<Iterator, equality_expression(), ascii::space_type> equality_expr;
        quoted_string<Iterator> quoted_string_;
        qi::rule<Iterator, std::string(), ascii::space_type> identifier;
        qi::rule<Iterator, detail::array_element(), ascii::space_type> array_expression;
        qi::rule<Iterator, record_expression(), ascii::space_type> record_expr;

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
    detail::expression_ast ast;
    // ast_print printer;
    calculator calc;
    bool r = phrase_parse(iter, end, calc, space, ast);

    if (!r || iter != end) {
        std::string rest(iter, end);
        throw CompileError(rest);
    }
    return std::make_shared<Filter>(ast);
}


}
