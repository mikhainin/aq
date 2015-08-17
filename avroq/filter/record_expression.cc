
#include "equality_expression.h"

#include "record_expression.h"



namespace filter {


record_expression::record_expression() {
}


record_expression::record_expression(const detail::array_element &ident):
    root_identifier(ident) {
}

record_expression::record_expression(const std::string &ident):
    root_identifier(ident) {
}

record_expression &record_expression::operator &= (const detail::expression_ast &ast) {
    this->ast = ast;
    return *this;
}

}
