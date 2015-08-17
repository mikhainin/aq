#ifndef __avroq__record_expression__
#define __avroq__record_expression__

#include <string>

#include <boost/variant.hpp>

#include "detail/ast.hpp"

namespace filter {

namespace detail {
    struct expression_ast;
}
struct record_expression {
    detail::expression_ast ast;
    boost::variant<std::string, detail::array_element> root_identifier;

    record_expression();

    record_expression(const detail::array_element &ident);
    record_expression(const std::string &ident);


    record_expression &operator &= (const detail::expression_ast &ast);
};

}

#endif